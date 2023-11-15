from typing import Optional

import pandas as pd

from ..core.ds import TradeType
from ..core.strategy import (StrategyExecutor,
                             CandleBasedPriceEntryMixin,
                             RelativeStopLossAndTargetMixin)
from ..core.indicator import IndicatorPipeline, DonchainIndicator, BreakoutIndicator



class DonchainBreakoutStrategy(RelativeStopLossAndTargetMixin,
                               CandleBasedPriceEntryMixin,
                               StrategyExecutor):

    def __init__(self, *args, **kwargs):
        indicators = indicators=[(DonchainIndicator(), None, None),
                                 (BreakoutIndicator(upper_breakout_column="donchainUpper",
                                                   lower_breakout_column="donchainLower",
                                                   data_interval="10min"), None, None)]
        kwargs["indicator_pipeline"] = IndicatorPipeline(indicators=indicators)
        kwargs["entry_price_column"] = "close"
        kwargs["sl_target_price_column"] = "close"
        kwargs["relative_stoploss_value"] = 10
        kwargs["relative_target_value"] = 20
        print(args, kwargs)
        super().__init__(*args, **kwargs)

    def strategy(self, window: pd.DataFrame) -> Optional[TradeType]:
        if window.iloc[-1]["donchainUpper_breakout"] > 0:
            return TradeType.LONG
        if window.iloc[-1]["donchainLower_breakout"] > 0:
            return TradeType.SHORT
