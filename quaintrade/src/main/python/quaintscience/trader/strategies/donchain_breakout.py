from typing import Optional

import pandas as pd

from ..core.ds import TradeType
from ..core.strategy import (StrategyExecutor,
                             CandleBasedPriceEntryMixin,
                             RelativeStopLossAndTargetMixin)
from ..core.indicator import (IndicatorPipeline,
                              DonchainIndicator,
                              BreakoutIndicator,
                              ADXIndicator,
                              RSIIndicator)



class DonchainBreakoutStrategy(RelativeStopLossAndTargetMixin,
                               CandleBasedPriceEntryMixin,
                               StrategyExecutor):

    def __init__(self, *args, **kwargs):
        indicators = indicators=[(DonchainIndicator(), None, None),
                                 (ADXIndicator(), None, None),
                                 (RSIIndicator(), None, None),
                                 (BreakoutIndicator(upper_breakout_column="donchainUpper",
                                                   lower_breakout_column="donchainLower",
                                                   data_interval="10min"), None, None)]
        kwargs["indicator_pipeline"] = IndicatorPipeline(indicators=indicators)
        kwargs["long_entry_price_column"] = "high"
        kwargs["short_entry_price_column"] = "low"
        kwargs["sl_price_column_long"] = "low"
        kwargs["sl_price_column_short"] = "high"
        kwargs["target_price_column_long"] = "high"
        kwargs["target_price_column_short"] = "low"
        kwargs["relative_stoploss_value"] = 0
        kwargs["relative_target_value"] = 40
        kwargs["indicator_fields"] = ["donchainUpper",
                                      "donchainLower",
                                      "donchainMiddle",
                                      {"field": "donchainUpper_breakout",
                                       "panel": 1},
                                       {"field": "donchainLower_breakout",
                                       "panel": 1},
                                       {"field": "RSI",
                                       "panel": 2},]
        print(args, kwargs)
        super().__init__(*args, **kwargs)

    def strategy(self, window: pd.DataFrame) -> Optional[TradeType]:
        if window.iloc[-1]["donchainUpper_breakout"]:
            return TradeType.LONG
        if window.iloc[-1]["donchainLower_breakout"]:
            return TradeType.SHORT
