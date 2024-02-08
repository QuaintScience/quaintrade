from __future__ import annotations
from abc import ABC, abstractmethod
from typing import Optional
from enum import Enum

import pandas as pd
from dotmap import DotMap

from .logging import LoggerMixin
from .roles import Broker
from .strategy import Strategy
from .ds import Order


class TradingStateMachine(LoggerMixin, ABC):

    def __init__(self,
                 scrip: str,
                 exchange: str,
                 *args,
                 **kwargs):
        self.state = DotMap()
        self.persistent_state = DotMap()
        self.reset()
        super().__init__(*args, **kwargs)
        self.scrip = scrip
        self.exchange = exchange
        self.init()

    def init(self):
        pass

    def is_green_candle(self, row: pd.Series):
            return row["close"] > row["open"]

    def is_red_candle(self, row: pd.Series):
        return not self.is_green_candle(row)

    def reset(self):
        self.state = DotMap()
        self.state.id = "start"
    
    def as_dict(self):
        return self.state.toDict()

    @abstractmethod
    def run(self,
            strategy: Strategy,
            window: pd.DataFrame,
            context: dict[str, pd.DataFrame],
            orders: dict[str, Order],
            **kwargs) -> Optional[Action]:
        pass

    def print(self):
        for k, v in self.as_dict().items():
            if isinstance(v, pd.Series):
                v = f"O {v['open']} H {v['high']} L {v['low']} C {v['close']}"
            self.logger.info(f"{k}    : {v}")


class Action(Enum):
        TakePosition = "TakePosition"
        AverageOutTrade = "AverageOutTrade"
        CancelPosition = "CancelPosition"
        CreateEntryWithInitialSL = "CreateEntryWithInitialSL"
        UpdateStoploss = "UpdateStoploss"
        UpdateTarget = "UpdateStoploss"
        UpdatePosition = "UpdatePosition"
