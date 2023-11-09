from enum import Enum
from dataclasses import dataclass
import datetime


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


@dataclass
class Order:
    order_id: str
    scrip_id: str
    exchange_id: str
    transaction_type: TransactionType
    scrip: str
    exchange: str
    raw_dict: dict
    timestamp: datetime.datetime
    order_type: OrderType
    product: TradingProduct
    quantity: int
    purchase_price: float
    validity: str = "DAY"
    state: OrderState = OrderState.PENDING
    trigger_price: float = None
    limit_price: float = None
    filled_quantity: float = 0
    pending_quantity: float = 0
    cancelled_quantity: float = 0


@dataclass
class Position:
    position_id: str
    timestamp: datetime.datetime
    scrip_id: str
    scrip: str
    exchange_id: str
    exchange: str
    product: TradingProduct
    quantity: int
    average_price: float
    last_price: float
    pnl: float
    day_change: float
    raw_dict: dict

    def __hash__(self):
        return hash(self.scrip, self.exchange, self.product)

    def __eq__(self, other: object):
        if not isinstance(other, Position):
            return False
        if (self.scrip == other.scrip
            and self.exchange == other.exchange
            and self.product == other.product):
            return True
        return False


class ExecutionType(Enum):
    BACKTESTING = "backtesting"
    LIVE = "live"