from abc import abstractmethod
from typing import Union, Optional
import datetime

import pandas as pd

from ...ds import Order, Position, TransactionType
from ..tradebook import TradeBookStorageMixin

class DummyTradeBookStorage(TradeBookStorageMixin):

    def __init__(self,
                 *args,
                 **kwargs):
        super().__init__(*args, **kwargs)

    def store_order_execution(self,
                              strategy: str,
                              run_name: str,
                              run_id: str,
                              date: Union[str, datetime.datetime],
                              order: Order,
                              event: str):
        pass

    def store_position_state(self,
                             strategy: str,
                             run_name: str,
                             run_id: str,
                             date: Union[str, datetime.datetime],
                             position: Position):
        pass

    def store_event(self, strategy: str,
                    run_name: str,
                    run_id: str,
                    scrip: str,
                    exchange: str,
                    event_type: str,
                    transaction_type: Optional[TransactionType] = None,
                    price: Optional[float] = 0.,
                    quantity: Optional[int] = 0,
                    date: Optional[Union[str, datetime.datetime]] = None):
        pass

    def get_events(self, strategy: str,
                   run_name: str,
                   run_id: str,
                   scrip: Optional[str] = None,
                   exchange: Optional[str] = None,
                   transaction_type: Optional[TransactionType] = None,
                   event_type: Optional[str] = None,
                   from_date: Optional[Union[str, datetime.datetime]] = None,
                   to_date: Optional[Union[str, datetime.datetime]] = None):
        pass

    def get_orders_for_run(self,
                           strategy: str,
                           run_name: str,
                           run_id: str,
                           from_date: Optional[Union[str, datetime.datetime]] = None,
                           to_date: Optional[Union[str, datetime.datetime]] = None) -> pd.DataFrame:
        pass

    def get_positions_for_run(self, strategy: str,
                              run_name: str,
                              run_id: str,
                              from_date: Optional[Union[str, datetime.datetime]] = None,
                              to_date: Optional[Union[str, datetime.datetime]] = None) -> pd.DataFrame:
        pass

    def get_position_statement_monthwise(self, strategy: str, run_name: str, run_id: str) -> dict[str, dict[str, float]]:
        positions = self.get_positions_for_run(strategy=strategy, run_name=run_name,
                                               run_id=run_id)
        result = {}
        for group_name, group in positions.groupby(pd.Grouper(freq="M")):
            result[group_name] = {"pnl": group.iloc[-1]["pnl"] - group.iloc[0]["pnl"],
                                  "charges": group.iloc[-1]["charges"] - group.iloc[0]["charges"]}
        return result

    def get_position_statement(self,
                               strategy: str,
                               run_name: str,
                               run_id: str,
                               from_date: Optional[Union[str, datetime.datetime]] = None,
                               to_date: Optional[Union[str, datetime.datetime]] = None) -> dict[str, float]:
        positions = self.get_positions_for_run(strategy=strategy, run_name=run_name,
                                               run_id=run_id,
                                               from_date=from_date, to_date=to_date)
        return {"pnl": positions.iloc[-1]["pnl"], "charges": positions.iloc[-1]["charges"]}

    def clear_run(self, strategy: str,
                  run_name: str,
                  scrip: str,
                  exchange: str):
        pass

    def commit(self):
        pass