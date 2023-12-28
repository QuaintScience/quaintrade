from enum import Enum
from dataclasses import dataclass
import datetime
from typing import Optional


from .util import (default_dataclass_field,
                   current_datetime_field,
                   new_id_field)


class OrderType(Enum):
    LIMIT = "limit"
    MARKET = "market"
    SL_LIMIT = "stoploss_limit"
    SL_MARKET = "stoploss_market"


class TradingProduct(Enum):
    CNC = "cnc"
    MIS = "mis"
    NRML = "nrml"


class TransactionType(Enum):
    BUY = "buy"
    SELL = "sell"


class TradeType(Enum):
    LONG = "long"
    SHORT = "short"


class OrderState(Enum):
    CANCELLED = "cancelled"
    PENDING = "pending"
    COMPLETED = "completed"


class ExecutionType(Enum):
    BACKTESTING = "backtesting"
    LIVE = "live"


@dataclass
class Order:
    scrip_id: str
    exchange_id: str
    scrip: str
    exchange: str
    order_id: str = new_id_field()
    transaction_type: TransactionType = TransactionType.BUY
    timestamp: datetime.datetime = current_datetime_field()
    order_type: OrderType = OrderType.MARKET
    product: TradingProduct = TradingProduct.MIS
    quantity: int = 1    
    validity: str = "DAY"
    state: OrderState = OrderState.PENDING
    trigger_price: float = None
    limit_price: float = None
    filled_quantity: float = 0
    pending_quantity: float = 0
    cancelled_quantity: float = 0
    price: float = 0
    raw_dict: dict = default_dataclass_field({})
    tags: list = default_dataclass_field([])
    parent_order_id: Optional[str] = None
    group_id: Optional[str] = None


@dataclass
class Position:
    scrip_id: str
    scrip: str
    exchange_id: str
    exchange: str
    position_id: str = new_id_field()
    product: TradingProduct = TradingProduct.MIS
    timestamp: datetime.datetime = current_datetime_field()
    quantity: int = 0
    average_price: float = 0.
    last_price: float = 0.
    pnl: float = 0.
    day_change: float = 0.
    raw_dict: dict = default_dataclass_field({})
    stats: dict = default_dataclass_field({})

    def __hash__(self):
        return hash(self.scrip, self.exchange, self.product.value)

    def __eq__(self, other: object):
        if not isinstance(other, Position):
            return False
        if (self.scrip == other.scrip
            and self.exchange == other.exchange
            and self.product == other.product):
            return True
        return False


class CandleType(Enum):

    OHLC = "ohlc"
    HEIKIN_ASHI = "heikin_ashi"


class OHLCStorageType(Enum):

    LIVE = "live"
    PERM = "perm"
