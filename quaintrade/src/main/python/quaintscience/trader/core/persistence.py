from abc import abstractmethod, ABC
from typing import Union, Optional
import sqlite3
import datetime

import pandas as pd

from .logging import LoggerMixin
from .util import get_datetime, sanitize
from .ds import Order, Position, TransactionType, TradingProduct



class Storage(ABC, LoggerMixin):
    
    def __init__(self,
                 path: str,
                 *args, **kwargs):
        self.path = path
        super().__init__(*args, **kwargs)
        self.connect()

    @abstractmethod
    def connect(self):
        pass


class SqliteStorage(Storage):

    def __init__(self, *args, **kwargs):
        self.cache = {}
        super().__init__(*args, **kwargs)

    def init_cache_for(self, *args,
                       conflict_resolution_type: str = "REPLACE"):
        key = self.get_table_name(*args)
        self.create_tables(*args,
                           conflict_resolution_type=conflict_resolution_type)
        if key not in self.cache:
            self.cache[key] = {}
            for k in self.table_names:
                self.cache[key][k] = []
        return key

    def commit(self):
        for key, all_data in self.cache.items():
            for table_suffix, data in all_data.items():
                if len(data) > 0:
                    self.logger.info(f"Writing cache for {key} / {table_suffix} with {len(data)} to {self.path}")
                    df = pd.DataFrame(data)
                    df.to_sql(f"{key}__{table_suffix}",
                            con=self.connection,
                            if_exists="append",
                            index=False)
        self.cache = {}

    def connect(self):
        self.logger.info(f"Connecting to {self.path}")
        self.connection = sqlite3.connect(self.path)

    def get_table_name(self, *args):
        return "__".join([sanitize(str(arg)) for arg in args])

    def create_tables(self, *args,
                      conflict_resolution_type: str = "IGNORE"):
        table_name = self.get_table_name(*args)
        self.create_tables_impl(table_name,
                                conflict_resolution_type)
        return table_name

    def __date_parse(self, from_date, to_date):
        if from_date is None:
            from_date = datetime.datetime.now() - datetime.timedelta(days=100000)
        if to_date is None:
            to_date = datetime.datetime.now()
        from_date = get_datetime(from_date).strftime("%Y-%m-%d %H:%M:%S")
        to_date = get_datetime(to_date).strftime("%Y-%m-%d %H:%M:%S")
        return from_date, to_date

    @abstractmethod
    def create_tables_impl(self, table_name, conflict_resolution_type: str = "IGNORE"):
        pass

    def get_timestamped_data(self, 
                             *args,
                             table_name_suffixes: Optional[list] = None,
                             from_date: Optional[Union[str, datetime.datetime]] = None,
                             to_date: Optional[Union[str, datetime.datetime]] = None,
                             data_name: str = "data",
                             cols: list = None,
                             index_col: str = "date",
                             col_filters: Optional[dict] = None,
                             conflict_resolution_type: str = "IGNORE"):
        self.create_tables(*args,
                           conflict_resolution_type=conflict_resolution_type)
        if table_name_suffixes is None:
            table_name_suffixes = []
        if cols is None or len(cols) == 0:
            raise ValueError("Cols not specified to fetch data")

        table_name = self.create_tables(*args)

        from_date, to_date = self.__date_parse(from_date, to_date)
        if len(table_name_suffixes) > 0:
            table_name = f"{table_name}__{'__'.join(table_name_suffixes)}"
        self.logger.debug(f"Reading {data_name} from {from_date} to {to_date} from {table_name}...")

        if "date" not in cols:
            cols.append("date")
        filters = ""
        if col_filters is None:
            col_filters = {}
        if len(col_filters) > 0:
            filters = []
            for k, v in col_filters.items():
                if isinstance(v, float) or isinstance(v, int):
                    filters.append(f"{k}={v}")
                else:
                    filters.append(f"{k}='{v}'")
            filters = " AND ".join(filters)
            filters = f"AND {filters}"
        sql = (f"SELECT {', '.join(cols)} FROM "
               f"{table_name} WHERE "
               f"(datetime(date) BETWEEN '{from_date}' AND '{to_date}')"
               f"{filters};")
        self.logger.debug(f"Executing {sql}")
        data = self.connection.execute(sql).fetchall()
        data = pd.DataFrame(data, columns=cols)
        if index_col is not None:
            data.index = data[index_col]
            data.index.name = index_col
            if index_col == "date":
                data.index = pd.to_datetime(data.index)
            data.drop([index_col], axis=1, inplace=True)
            data = data[~data.index.duplicated(keep='last')]

        return data


class OHLCStorageMixin():

    def __init__(self,
                 *args, **kwargs):
        pass
    
    @abstractmethod
    def put(self, scrip: str, exchange: str, df: pd.DataFrame):
        pass

    @abstractmethod
    def get(self, scrip: str, exchange: str,
            fromdate: Union[str, datetime.datetime],
            todate: Union[str, datetime.datetime]) -> pd.DataFrame:
        pass


class TradeBookStorageMixin():

    table_names = ["events", "orders", "positions"]

    def __init__(self,
                 *args,
                 **kwargs):
        pass

    @abstractmethod
    def store_order_execution(self,
                              strategy: str,
                              run_name: str,
                              date: Union[str, datetime.datetime],
                              order: Order,
                              event: str):
        pass

    @abstractmethod
    def store_position_state(self,
                             strategy: str,
                             run_name: str,

                             date: Union[str, datetime.datetime],
                             position: Position):
        pass

    @abstractmethod
    def store_event(self, strategy: str, run_name: str,
                    scrip: str,
                    exchange: str,
                    event_type: str,
                    transaction_type: Optional[TransactionType] = None,
                    price: Optional[float] = 0.,
                    quantity: Optional[int] = 0,
                    date: Optional[Union[str, datetime.datetime]] = None):
        pass

    @abstractmethod
    def get_events(self, strategy: str,
                   run_name: str,
                   scrip: Optional[str] = None,
                   exchange: Optional[str] = None,
                   transaction_type: Optional[TransactionType] = None,
                   event_type: Optional[str] = None,
                   from_date: Optional[Union[str, datetime.datetime]] = None,
                   to_date: Optional[Union[str, datetime.datetime]] = None):
        pass

    @abstractmethod
    def get_orders_for_run(self,
                           strategy: str,
                           run_name: str,
                           from_date: Optional[Union[str, datetime.datetime]] = None,
                           to_date: Optional[Union[str, datetime.datetime]] = None) -> pd.DataFrame:
        pass

    @abstractmethod
    def get_positions_for_run(self, strategy: str,
                              run_name: str,
                              from_date: Optional[Union[str, datetime.datetime]] = None,
                              to_date: Optional[Union[str, datetime.datetime]] = None) -> pd.DataFrame:
        pass

    def get_position_statement_monthwise(self, strategy: str, run_name: str) -> dict[str, dict[str, float]]:
        positions = self.get_positions_for_run(strategy=strategy, run_name=run_name)
        result = {}
        for group_name, group in positions.groupby(pd.Grouper(freq="M")):
            result[group_name] = {"pnl": group.iloc[-1]["pnl"] - group.iloc[0]["pnl"],
                                  "charges": group.iloc[-1]["charges"] - group.iloc[0]["charges"]}
        return result

    def get_position_statement(self,
                               strategy: str,
                               run_name: str,
                               from_date: Optional[Union[str, datetime.datetime]] = None,
                               to_date: Optional[Union[str, datetime.datetime]] = None) -> dict[str, float]:
        positions = self.get_positions_for_run(strategy=strategy, run_name=run_name,
                                               from_date=from_date, to_date=to_date)
        return {"pnl": positions.iloc[-1]["pnl"], "charges": positions.iloc[-1]["charges"]}

    @abstractmethod
    def clear_run(self, strategy: str, run_name: str):
        pass


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


class SqliteOHLCStorage(SqliteStorage, OHLCStorageMixin):

    def __init__(self,
                 *args,
                 **kwargs):
        super().__init__(*args, **kwargs)
    

    def create_tables_impl(self, table_name, conflict_resolution_type: str = "REPLACE"):
        self.connection.execute(f"""CREATE TABLE IF NOT EXISTS {table_name} (date VARCHAR(255) NOT NULL,
                                                                             open REAL NOT NULL,
                                                                             high REAL NOT NULL,
                                                                             low REAL NOT NULL,
                                                                             close REAL NOT NULL,
                                                                             volume INTEGER NOT NULL,
                                                                             oi INTEGER NOT NULL,
                                                                             PRIMARY KEY (date) ON CONFLICT {conflict_resolution_type});""")

    def put(self,
            scrip: str,
            exchange: str,
            df: pd.DataFrame,
            conflict_resolution_type: str = "IGNORE"):
        table_name = self.create_tables(scrip, exchange,
                                        conflict_resolution_type=conflict_resolution_type)
        df.to_sql(table_name, con=self.connection, if_exists="append")

    def get(self, scrip: str, exchange: str,
            from_date: Union[str, datetime.datetime],
            to_date: Union[str, datetime.datetime],
            conflict_resolution_type: str) -> pd.DataFrame:
        cols = ["date", "open", "high", "low",
                "close", "volume", "oi"]
        return self.get_timestamped_data(scrip, exchange,
                                         table_name_suffixes=[],
                                         from_date=from_date,
                                         to_date=to_date,
                                         cols=cols,
                                         index_col="date",
                                         conflict_resolution_type=conflict_resolution_type)
