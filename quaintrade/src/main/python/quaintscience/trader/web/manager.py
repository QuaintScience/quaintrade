from typing import Optional
from threading import Lock

from fastapi import FastAPI
from pydantic import BaseModel

from ..core.persistence.sqlite.manager import SqliteManager

app = FastAPI()

FILEPATH = "./quaintrade_manager.sqlite"
manager_storage = SqliteManager(path=FILEPATH)
db_lock = Lock()

@app.get("/data_providers")
def get_data_providers():
    with db_lock:
        return manager_storage.get_data_providers().to_dict(orient='records')

@app.get("/brokers")
def get_data_providers():
    with db_lock:
        return manager_storage.get_brokers().to_dict(orient='records')

@app.get("/strategies")
def get_data_providers():
    with db_lock:
        return manager_storage.get_strategies().to_dict(orient='records')

@app.get("/backtesting_templates")
def get_data_providers():
    with db_lock:
        return manager_storage.get_backtesting_templates().to_dict(orient='records')

@app.get("/backtesting_stats")
def get_data_providers():
    with db_lock:
        return manager_storage.get_backtest_stats().to_dict(orient='records')

@app.get("/live_templates")
def get_data_providers():
    with db_lock:
        return manager_storage.get_live_templates().to_dict(orient='records')

@app.get("/live_stats")
def get_data_providers():
    with db_lock:
        return manager_storage.get_live_trader_stats().to_dict(orient='records')


class DataProvider(BaseModel):
    name: str
    ProviderClass: str
    auth_cache_filepath: str
    StorageClass: str = "quaintscience.trader.core.persistence.sqlite.ohlc.SqliteOHLCStorage"
    TradingBookStorageClass: str = "quaintscience.trader.core.persistence.tradebook.SqliteTradeBookStorage",
    auth_credentials: Optional[dict] = None
    thread_id: str = "default"
    run_name: Optional[str] = None
    custom_kwargs: Optional[dict] = None


class Broker(BaseModel):
    name: str
    ProviderClass: str
    auth_cache_filepath: str
    StorageClass: str = "quaintscience.trader.core.persistence.SqliteOHLCStorage"
    auth_credentials: Optional[dict] = None
    custom_kwargs: Optional[dict] = None


class Strategy(BaseModel):
    name: str
    StrategyClass: str
    custom_kwargs: Optional[dict] = None


class BacktestingTemplate(BaseModel):
    name: str
    data_provider_name: str
    strategy_name: str
    from_date: str
    to_date: str
    interval: str = "3min"
    refresh_orders_immediately_on_gtt_state_change: str = False
    plot_results: str = False
    window_size: int = 5
    live_trading_mode: bool = False
    clear_tradebook_for_scrip_and_exchange: bool = False
    custom_kwargs: Optional[dict] = None


class LiveTemplate(BaseModel):
    name: str
    data_provider_name: str
    broker_name: str
    strategy_name: str
    interval: str
    data_context_size: int
    online_mode: bool
    custom_kwargs: Optional[dict] = None


@app.post("/data_provider")
def add_data_provider(data_provider: DataProvider):

    with db_lock:
        return manager_storage.store_data_provider(**data_provider.model_dump())


@app.post("/broker")
def add_broker(broker: Broker):
    with db_lock:
        return manager_storage.store_broker(**broker.model_dump())


@app.post("/strategy")
def add_strategy(strategy: Strategy):
    with db_lock:
        return manager_storage.store_strategy(**strategy.model_dump())


@app.post("/backtesting_template")
def add_backtesting_template(backtesting_template: BacktestingTemplate):
    with db_lock:
        return manager_storage.store_backtesting_template(**backtesting_template.model_dump())


@app.post("/live_template")
def add_live_template(live_template: LiveTemplate):
    with db_lock:
        return manager_storage.store_strategy(**live_template.model_dump())
