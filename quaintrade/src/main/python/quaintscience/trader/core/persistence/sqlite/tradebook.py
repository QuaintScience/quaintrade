from typing import Union, Optional
import datetime

import pandas as pd

from .common import SqliteStorage
from ..tradebook import TradeBookStorageMixin
from ...ds import Order, Position, TransactionType, TradingProduct



class SqliteTradeBookStorage(SqliteStorage, TradeBookStorageMixin):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def create_tables_impl(self, table_name: str, conflict_resolution_type: str = "REPLACE"):
        self.connection.execute(f"""CREATE TABLE IF NOT EXISTS {table_name}__orders (date VARCHAR(255) NOT NULL,
                                                                             strategy VARCHAR(255) NOT NULL,
                                                                             run_name VARCHAR(255) NOT NULL,
                                                                             run_id VARCHAR(255) NOT NULL,
                                                                             scrip VARCHAR(255) NOT NULL,
                                                                             exchange VARCHAR(255) NOT NULL,
                                                                             order_id VARCHAR(255) NOT NULL,
                                                                             transaction_type VARCHAR(255) NOT NULL,
                                                                             tags VARCHAR(255) NOT NULL,
                                                                             product VARCHAR(255) NOT NULL,
                                                                             order_type VARCHAR(255) NOT NULL,
                                                                             quantity VARCHAR(255) NOT NULL,
                                                                             price REAL NOT NULL,
                                                                             limit_price REAL,
                                                                             trigger_price REAL,
                                                                             parent_order_id VARCHAR(255),
                                                                             group_id VARCHAR(255),
                                                                             event VARCHAR(255),
                                                                             PRIMARY KEY (order_id) ON CONFLICT {conflict_resolution_type});""")
        self.connection.execute(f"""CREATE TABLE IF NOT EXISTS {table_name}__positions (date VARCHAR(255) NOT NULL,
                                                                             scrip VARCHAR(255) NOT NULL,
                                                                             exchange VARCHAR(255) NOT NULL,
                                                                             strategy VARCHAR(255) NOT NULL,
                                                                             run_name VARCHAR(255) NOT NULL,
                                                                             run_id VARCHAR(255) NOT NULL,
                                                                             product VARCHAR(255) NOT NULL,
                                                                             pnl REAL NOT NULL,
                                                                             charges REAL NOT NULL,
                                                                             PRIMARY KEY (date) ON CONFLICT {conflict_resolution_type});""")
        self.connection.execute(f"""CREATE TABLE IF NOT EXISTS {table_name}__events (date VARCHAR(255) NOT NULL,
                                                                             scrip VARCHAR(255) NOT NULL,
                                                                             exchange VARCHAR(255) NOT NULL,
                                                                             strategy VARCHAR(255) NOT NULL,
                                                                             run_id VARCHAR(255) NOT NULL,
                                                                             run_name VARCHAR(255) NOT NULL,
                                                                             quantity REAL,
                                                                             price REAL,
                                                                             transaction_type VARCHAR(255),
                                                                             event_type VARCHAR(255) NOT NULL,
                                                                             PRIMARY KEY (date) ON CONFLICT {conflict_resolution_type});""")

    def store_event(self,
                    strategy: str,
                    run_name: str,
                    run_id: str,
                    scrip: str,
                    exchange: str,
                    event_type: str,
                    transaction_type: Optional[TransactionType] = None,
                    price: Optional[float] = 0.,
                    quantity: Optional[int] = 0,
                    date: Optional[Union[str, datetime.datetime]] = None,
                    conflict_resolution_type: str = "REPLACE"):

        key = self.init_cache_for(strategy, run_name,
                                  conflict_resolution_type=conflict_resolution_type)

        transaction_type = transaction_type.value if isinstance(transaction_type,
                                                                TransactionType) else None
        if date is None:
            data = datetime.datetime.now()
        
        self.cache[key]["events"].append({"scrip": scrip,
                                          "exchange": exchange,
                                          "transaction_type": transaction_type,
                                          "event_type": event_type,
                                          "quantity": quantity,
                                          "price": price,
                                          "strategy": strategy,
                                          "run_name": run_name,
                                          "run_id": run_id,
                                          "date": date})
<<<<<<< HEAD
        if len(self.cache[key]["events"]) > 1000:
            self.commit()
=======
       
>>>>>>> 1e314e13b6fa1d64fdc5ea31562aa7266bece468

    def store_order_execution(self,
                              strategy: str,
                              run_name: str,
                              run_id: str,
                              order: Order,
                              event: str,
                              date: Optional[Union[str, datetime.datetime]] = None,
                              conflict_resolution_type: str = "REPLACE"):

        key = self.init_cache_for(strategy, run_name,
                                  conflict_resolution_type=conflict_resolution_type)

        if date is None:
            date = datetime.datetime.now()

        self.cache[key]["orders"].append({"scrip": order.scrip,
                                          "exchange": order.exchange,
                                          "order_id": order.order_id,
                                          "transaction_type": order.transaction_type.value,
                                          "tags": ", ".join(order.tags),
                                          "product": order.product.value,
                                          "quantity": order.quantity,
                                          "price": order.price,
                                          "limit_price": order.limit_price,
                                          "trigger_price": order.trigger_price,
                                          "parent_order_id": order.parent_order_id,
                                          "group_id": order.group_id,
                                          "strategy": strategy,
                                          "order_type": order.order_type.value,
                                          "event": event,
                                          "run_id": run_id,
                                          "run_name": run_name,
                                          "date": date})
<<<<<<< HEAD
        if len(self.cache[key]["orders"]) > 1000:
            self.commit()
=======
>>>>>>> 1e314e13b6fa1d64fdc5ea31562aa7266bece468

    def store_position_state(self,
                             strategy: str,
                             run_name: str,
                             run_id: str,
                             position: Position,
                             date: Optional[Union[str, datetime.datetime]] = None,
                             conflict_resolution_type: str = "REPLACE"):
        
        key = self.init_cache_for(strategy, run_name,
                                  conflict_resolution_type=conflict_resolution_type)

        if date is None:
            date = datetime.datetime.now()

        self.cache[key]["positions"].append({"scrip": position.scrip,
                                             "exchange": position.exchange,
                                             "strategy": strategy,
                                             "run_name": run_name,
                                             "run_id": run_id,
                                             "product": position.product.value,
                                             "pnl": position.pnl,
                                             "charges": position.charges,
                                             "date": date})
<<<<<<< HEAD
        if len(self.cache[key]["positions"]) > 1000:
            self.commit()
=======
>>>>>>> 1e314e13b6fa1d64fdc5ea31562aa7266bece468

    def get_orders_for_run(self,
                           strategy: str,
                           run_name: str,
                           run_id: Optional[str] = None,
                           from_date: Optional[Union[str, datetime.datetime]] = None,
                           to_date: Optional[Union[str, datetime.datetime]] = None,
                           conflict_resolution_type: str = "REPLACE") -> pd.DataFrame:
        cols = ["scrip", "exchange" "order_id",
                "transaction_type", "tags", "product",
                "quantity", "price", "limit_price", "trigger_price",
                "parent_order_id", "group_id", "strategy", "run_name"]
        col_filters = None
        if run_id is not None:
            col_filters = {"run_id": run_id}
        data = self.get_timestamped_data(strategy, run_name,
                                         table_name_suffixes=["orders"],
                                         from_date=from_date,
                                         to_date=to_date,
                                         cols=cols,
                                         index_col="order_id",
                                         col_filters=col_filters,
                                         conflict_resolution_type=conflict_resolution_type)
        data["transaction_type"] = data["transaction_type"].apply(lambda x: TransactionType(x) if x is not None else None)
        data["product"] = data["product"].apply(lambda x: TradingProduct(x) if x is not None else x)
        return data

    def get_positions_for_run(self, strategy: str,
                              run_name: str,
                              run_id: Optional[str] = None,
                              from_date: Optional[Union[str, datetime.datetime]] = None,
                              to_date: Optional[Union[str, datetime.datetime]] = None,
                              conflict_resolution_type: str = "REPLACE") -> pd.DataFrame:
        cols = ["scrip", "exchange", "strategy", "run_name",
                "product", "pnl", "charges"]
        col_filters = None
        if run_id is not None:
            col_filters = {"run_id": run_id}
        data = self.get_timestamped_data(strategy, run_name,
                                         table_name_suffixes=["positions"],
                                         from_date=from_date,
                                         to_date=to_date,
                                         cols=cols,
                                         index_col="date",
                                         col_filters=col_filters,
                                         conflict_resolution_type=conflict_resolution_type)
        data["product"] = data["product"].apply(lambda x: TradingProduct(x) if x is not None else None)
        return data

    def get_events(self, strategy: str,
                   run_name: str,
                   run_id: Optional[str] = None,
                   scrip: Optional[str] = None,
                   exchange: Optional[str] = None,
                   transaction_type: Optional[TransactionType] = None,
                   event_type: Optional[str] = None,
                   from_date: Optional[Union[str, datetime.datetime]] = None,
                   to_date: Optional[Union[str, datetime.datetime]] = None) -> pd.DataFrame:

        cols = ["scrip", "exchange", "transaction_type", "event_type",
                "quantity", "price", "strategy", "run_name"]
        col_filters = {}
        if scrip is not None:
            col_filters["scrip"] = scrip
        if exchange is not None:
            col_filters["exchange"] = exchange
        if transaction_type is not None:
            col_filters["transaction_type"] = transaction_type.value
        if event_type is not None:
            col_filters["event_type"] = event_type
        if run_id is not None:
            col_filters["run_id"] = run_id

        data = self.get_timestamped_data(strategy, run_name,
                                         table_name_suffixes=["events"],
                                         from_date=from_date,
                                         to_date=to_date,
                                         cols=cols,
                                         index_col="date",
                                         col_filters=col_filters,
                                         conflict_resolution_type="IGNORE")
        data["transaction_type"] = data["transaction_type"].apply(lambda x: TransactionType(x) if x is not None else None)
        return data

    def clear_run(self, strategy: str,
                  run_name: str,
                  scrip: str,
                  exchange: str):
        table_name = self.create_tables(strategy, run_name,
                                        conflict_resolution_type="REPLACE")
        for table in self.table_names:
            sql = f"DELETE FROM {table_name}__{table} WHERE scrip='{scrip}' AND exchange='{exchange}';"
            self.logger.debug(f"Executing {sql}")
            self.connection.execute(sql)
