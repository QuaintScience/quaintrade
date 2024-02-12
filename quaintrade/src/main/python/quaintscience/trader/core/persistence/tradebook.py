from abc import abstractmethod
from typing import Union, Optional
import datetime

import pandas as pd

from ..ds import Order, Position, TransactionType


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
<<<<<<< HEAD
                              run_id: str,
=======
>>>>>>> 1e314e13b6fa1d64fdc5ea31562aa7266bece468
                              date: Union[str, datetime.datetime],
                              order: Order,
                              event: str):
        pass

    @abstractmethod
    def store_position_state(self,
                             strategy: str,
                             run_name: str,
<<<<<<< HEAD
                             run_id: str,
=======

>>>>>>> 1e314e13b6fa1d64fdc5ea31562aa7266bece468
                             date: Union[str, datetime.datetime],
                             position: Position):
        pass

    @abstractmethod
<<<<<<< HEAD
    def store_event(self, strategy: str, run_name: str, run_id: str,
=======
    def store_event(self, strategy: str, run_name: str,
>>>>>>> 1e314e13b6fa1d64fdc5ea31562aa7266bece468
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
    def clear_run(self, strategy: str,
                  run_name: str,
                  scrip: str,
                  exchange: str):
        pass
