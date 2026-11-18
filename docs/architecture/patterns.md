# Паттерны проектирования Business Metrics Analyzer

## 1. Таблица выбранных паттернов

| Паттерн | Где применяется | Обоснование | Альтернативы |
|---------|-----------------|-------------|--------------|
| Repository | Слой доступа к данным: продажи, пользователи, справочники и метрики | Изолирует бизнес-логику от SQLAlchemy и PostgreSQL; позволяет подменять БД на in-memory реализацию в тестах; сосредоточивает оптимизированные запросы (агрегации по периодам) в одном месте | Прямые запросы к ORM из сервисов |
| Unit of Work | Пакетный импорт данных, обновление справочников, любые операции с несколькими репозиториями | Атомарность: импорт либо сохраняется целиком, либо откатывается при ошибке; единая граница транзакции | Ручное управление транзакциями (commit/rollback в сервисах) |
| Service Layer | Сценарии: импорт продаж, расчёт KPI, формирование отчёта, управление пользователями | Инкапсулирует бизнес-сценарии и координирует репозитории, стратегии и адаптеры; REST-контроллеры остаются тонкими | Логика в контроллерах FastAPI |
| Strategy | Расчёт KPI: выручка, средний чек, конверсия, рентабельность | Каждый KPI — отдельный взаимозаменяемый алгоритм; новый KPI добавляется без изменения существующего кода (принцип Open/Closed); поддерживает «Настройку метрик» | Жёстко заданные методы, цепочки if/elif |
| Factory | Создание генераторов отчётов (Excel, PDF) | Сервис отчётов не зависит от конкретных классов; новый формат регистрируется в фабрике без правки сервиса | Условные операторы по типу отчёта |
| DTO | Обмен данными между Web UI и REST API, между слоями | Чёткие контракты API, валидация входных данных (Pydantic), версионирование; в API не «утекают» ORM-модели и внутренние поля (например, хэш пароля) | Передача сырых ORM-моделей |
| Adapter | Интеграция с 1С, чтение CSV/Excel, отправка email через SMTP | Изолирует внешние зависимости за единым интерфейсом (порт); замена источника или почтового сервиса не затрагивает ядро | Прямые вызовы внешних библиотек и API |
| Observer | События «данные импортированы», «KPI пересчитаны», «отчёт готов» | Слабая связность: после импорта автоматически инвалидируется кэш Redis, пересчитываются KPI, отправляется уведомление, пишется аудит | Периодический опрос (polling), прямые вызовы между сервисами |

Расположение паттернов на диаграмме компонентов: [components.png](components.png).

## 2. Фрагменты кода

### 2.1. Repository + Unit of Work

Доменная сущность не зависит от ORM. Интерфейс репозитория принадлежит слою Domain, реализация — слою Infrastructure.

```python
# domain/entities.py
from dataclasses import dataclass
from datetime import date
from decimal import Decimal


@dataclass
class SaleRecord:
    id: int | None
    sale_date: date
    revenue: Decimal
    cost: Decimal
    orders: int
```

```python
# domain/repositories.py
from abc import ABC, abstractmethod
from datetime import date
from typing import Iterable

from domain.entities import SaleRecord


class SaleRepository(ABC):
    @abstractmethod
    def add_many(self, records: Iterable[SaleRecord]) -> None: ...

    @abstractmethod
    def get_by_period(self, start: date, end: date) -> list[SaleRecord]: ...
```

```python
# infrastructure/db/models.py
from datetime import date
from decimal import Decimal

from sqlalchemy import Numeric
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


class SaleModel(Base):
    __tablename__ = "sales"

    id: Mapped[int] = mapped_column(primary_key=True)
    sale_date: Mapped[date] = mapped_column(index=True)
    revenue: Mapped[Decimal] = mapped_column(Numeric(14, 2))
    cost: Mapped[Decimal] = mapped_column(Numeric(14, 2))
    orders: Mapped[int]
```

```python
# infrastructure/db/repositories.py
from datetime import date
from itertools import islice
from typing import Iterable

from sqlalchemy import insert, select
from sqlalchemy.orm import Session

from domain.entities import SaleRecord
from domain.repositories import SaleRepository
from infrastructure.db.models import SaleModel

BATCH_SIZE = 10_000


class SqlSaleRepository(SaleRepository):
    def __init__(self, session: Session):
        self.session = session

    def add_many(self, records: Iterable[SaleRecord]) -> None:
        # пакетная вставка частями — учитываем объём до 1 млн записей
        it = iter(records)
        while batch := list(islice(it, BATCH_SIZE)):
            self.session.execute(
                insert(SaleModel),
                [
                    {"sale_date": r.sale_date, "revenue": r.revenue,
                     "cost": r.cost, "orders": r.orders}
                    for r in batch
                ],
            )

    def get_by_period(self, start: date, end: date) -> list[SaleRecord]:
        rows = self.session.scalars(
            select(SaleModel).where(SaleModel.sale_date.between(start, end))
        ).all()
        return [SaleRecord(m.id, m.sale_date, m.revenue, m.cost, m.orders) for m in rows]
```

```python
# infrastructure/db/unit_of_work.py
from sqlalchemy.orm import Session, sessionmaker

from infrastructure.db.repositories import SqlSaleRepository


class UnitOfWork:
    def __init__(self, session_factory: sessionmaker[Session]):
        self._session_factory = session_factory

    def __enter__(self) -> "UnitOfWork":
        self.session = self._session_factory()
        self.sales = SqlSaleRepository(self.session)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        # по умолчанию откат: коммит происходит только явным вызовом commit()
        if exc_type is not None:
            self.rollback()
        self.session.close()

    def commit(self) -> None:
        self.session.commit()

    def rollback(self) -> None:
        self.session.rollback()
```

Использование (фрагмент Service Layer):

```python
with uow_factory() as uow:
    uow.sales.add_many(records)
    uow.commit()  # всё или ничего
```

### 2.2. Strategy для расчёта KPI

Единый входной объект решает проблему разных наборов аргументов у разных KPI (выручке нужны одни поля, среднему чеку — другие).

```python
# domain/kpi.py
from abc import ABC, abstractmethod
from dataclasses import dataclass
from decimal import Decimal


@dataclass(frozen=True)
class KpiInput:
    revenue: Decimal
    cost: Decimal
    orders: int
    visitors: int = 0


class KpiStrategy(ABC):
    code: str  # уникальный код метрики

    @abstractmethod
    def calculate(self, data: KpiInput) -> Decimal: ...


class RevenueStrategy(KpiStrategy):
    code = "revenue"

    def calculate(self, data: KpiInput) -> Decimal:
        return data.revenue


class AverageCheckStrategy(KpiStrategy):
    code = "average_check"

    def calculate(self, data: KpiInput) -> Decimal:
        return Decimal(0) if data.orders == 0 else data.revenue / data.orders


class ProfitabilityStrategy(KpiStrategy):
    code = "profitability"

    def calculate(self, data: KpiInput) -> Decimal:
        if data.revenue == 0:
            return Decimal(0)
        return (data.revenue - data.cost) / data.revenue * 100


class ConversionStrategy(KpiStrategy):
    code = "conversion"

    def calculate(self, data: KpiInput) -> Decimal:
        return Decimal(0) if data.visitors == 0 else Decimal(data.orders) / data.visitors * 100
```

Использование (фрагмент Service Layer): сервис получает список стратегий и вызывает нужную по коду, поэтому новый KPI добавляется созданием класса без изменения сервиса.

```python
strategies = {s.code: s for s in [
    RevenueStrategy(), AverageCheckStrategy(),
    ProfitabilityStrategy(), ConversionStrategy(),
]}

data = KpiInput(revenue=Decimal("1000"), cost=Decimal("700"), orders=20)
result = {code: s.calculate(data) for code, s in strategies.items()}
```
