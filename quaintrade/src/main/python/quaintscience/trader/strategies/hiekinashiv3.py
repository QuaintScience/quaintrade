from typing import Optional

import pandas as pd
import numpy as np

from ..core.ds import (TradeType, PositionType, TradingProduct)

from ..core.strategy import Strategy
from ..core.indicator import (IndicatorPipeline,
                              HeikinAshiIndicator,
                              BBANDSIndicator,
                              ATRIndicator,
                              RSIIndicator,
                              ChoppinessIndicator,
                              ADXIndicator,
                              MajorityRuleIndicator,
                              SupertrendIndicator)
from ..core.roles import Broker


class HiekinAshiStrategyV3(Strategy):

    def __init__(self,
                 *args,
                 product: TradingProduct = TradingProduct.MIS,
                 st_period: int = 7,
                 st_multiplier: float = 2.5,
                 ma_period: int = 10,
                 donchain_period: int = 15,
                 bb_period: int = 15,
                 atr_period: int = 14,
                 rsi_period: int = 14,
                 choppiness_period: int = 14,
                 adx_period: int = 14,
                 majority_rule_period: int = 14,
                 **kwargs):
        self.st_period = st_period
        self.st_multiplier = st_multiplier
        self.ma_period = ma_period
        self.bb_period = bb_period
        self.donchain_period = donchain_period
        self.atr_period = atr_period
        self.rsi_period = rsi_period
        self.adx_period = adx_period
        self.majority_rule_period = majority_rule_period
        self.product = product
        self.choppiness_period = choppiness_period
        self.short_context1 = "15min"
        self.short_context2 = "30min"
        self.long_context1 = "45min"
        self.long_context2 = "60min"
        self.long_context3 = "75min"

        self.rsi_upper_threshold = 60
        self.rsi_lower_threshold = 40
        self.rsi_col = f"RSI_{self.rsi_period}"
        self.atr_col = f"ATR_{self.atr_period}"
        self.st_col = f"supertrend_{self.st_period}_{self.st_multiplier:.1f}"
        self.adx_col = f"ADX_{self.adx_period}"
        self.majority_col = f"majority_rule_{self.majority_rule_period}"
        self.choppiness_col = f"choppiness_{self.choppiness_period}"
        indicators = indicators=[(HeikinAshiIndicator(), None, None),
                                 (ADXIndicator(period=self.adx_period), None, None),
                                 (SupertrendIndicator(period=self.st_period,
                                                      multiplier=self.st_multiplier), None, None),
                                 (RSIIndicator(period=self.rsi_period), None, None),
                                 (BBANDSIndicator(period=self.bb_period), None, None),
                                 (ATRIndicator(period=self.atr_period), None, None),
                                 (MajorityRuleIndicator(period=self.majority_rule_period), None, None)
                                 ]
        
        kwargs["indicator_pipeline"] = IndicatorPipeline(indicators=indicators)
        kwargs["plottables"] = {"indicator_fields": [{"field": "ha_long_trend", "panel": 2},
                                                     {"field": "ha_trending_green", "panel": 2, "context": self.long_context3},
                                                     {"field": "ha_short_trend", "panel": 3},
                                                     {"field": "ha_trending_red", "panel": 3, "context": self.long_context3},
                                                     #{"field": "ha_non_trending", "panel": 4},
                                                     f"BBandUpper_{self.bb_period}",
                                                     f"BBandLower_{self.bb_period}",
                                                     {"field": self.st_col, "panel": 0, "context": self.short_context1},
                                                     {"field": self.st_col, "panel": 0, "context": self.short_context2},
                                                     #{"field": f"ATR_{self.atr_period}", "panel": 5},
                                                     
                                                     {"field": self.majority_col, "panel": 4},
                                                     {"field": self.majority_col, "panel": 4, "context": self.long_context3},
                                                     #f"supertrend_{self.st_period}_{self.st_multiplier:.1f}",
                                                     #f"SMA_{self.ma_period}",
                                                     #f"donchainUpper_{self.donchain_period}",
                                                     #f"donchainLower_{self.donchain_period}",
                                                     ]}
        non_trading_timeslots = []
        non_trading_timeslots.extend(Strategy.NON_TRADING_FIRST_HOUR)
        non_trading_timeslots.extend(Strategy.NON_TRADING_AFTERNOON)
        kwargs["non_trading_timeslots"] = non_trading_timeslots
        kwargs["context_required"] = list(set([self.short_context1,
                                               self.short_context2,
                                               self.long_context1,
                                               self.long_context2,
                                               self.long_context3]))

        self.entry_threshold = 0.1
        """
        self.sl_factor = 2
        self.target_factor = 2.5
        """

        self.sl_factor = 2
        self.target_factor = 10

        #self.sl_factor = 10
        #self.target_factor = 10

        """
        self.sl_factor = 3
        self.target_factor = 3
        """

        print(args, kwargs)
        super().__init__(*args, **kwargs)

    def get_entry(self, window: pd.DataFrame, trade_type: TradeType):
        if trade_type == TradeType.LONG:
            return window.iloc[-1]["high"] + self.entry_threshold * window.iloc[-1][self.atr_col]
        else:
            return window.iloc[-1]["low"] - self.entry_threshold * window.iloc[-1][self.atr_col]

    def get_target(self, window: pd.DataFrame, context: dict[str, pd.DataFrame], trade_type: TradeType):
        """
        if trade_type == TradeType.LONG:
            return window.iloc[-1]["high"] + self.risk_ratio * abs(self.get_entry(window, trade_type) - self.get_stoploss(window, trade_type))
        else:
            return window.iloc[-1]["low"] - self.risk_ratio * abs(self.get_stoploss(window, trade_type) - self.get_entry(window, trade_type))
        """

        if trade_type == TradeType.LONG:
            return window.iloc[-1]["high"] + self.target_factor * window.iloc[-1][self.atr_col]
        else:
            return window.iloc[-1]["low"] - self.target_factor * window.iloc[-1][self.atr_col]

    def get_stoploss(self, window: pd.DataFrame, context: dict[str, pd.DataFrame], trade_type: TradeType):
        
        if trade_type == TradeType.LONG:
            return window.iloc[-1]["low"] - self.sl_factor * window.iloc[-1][self.atr_col]
        else:
            return window.iloc[-1]["high"] + self.sl_factor * window.iloc[-1][self.atr_col]

    def apply_impl(self,
                   broker: Broker,
                   scrip: str,
                   exchange: str,
                   window: pd.DataFrame,
                   context: dict[str, pd.DataFrame]) -> Optional[TradeType]:

        colvals = []
        for col in window.columns:
            if col not in ["open", "high", "low", "close"]:
                colvals.append(f"{col}={window.iloc[-1][col]}")
        self.logger.info(f"{self.__class__.__name__} [{window.iloc[-1].name}]:"
                         f" O={window.iloc[-1]['open']}"
                         f" H={window.iloc[-1]['high']}"
                         f" L={window.iloc[-1]['low']}"
                         f" C={window.iloc[-1]['close']}"
                         f" {' '.join(colvals)}")
        self.logger.info(f"{self.long_context3} "
                         f'green {context[self.long_context3].iloc[-1]["ha_trending_green"]}'
                         f' red {context[self.long_context3].iloc[-1]["ha_trending_red"]}')

        current_run = self.get_current_run(broker,
                                           scrip,
                                           exchange)

        self.logger.info(f"Current Run: {current_run}")
        if self.can_trade(window, context):
            make_entry = False
            if (window.iloc[-1]["ha_long_trend"] == 1.0
                # and window.iloc[-2]["ha_long_trend"] == 0.0
                #and context[self.long_context].iloc[-1]["ha_trending_green"] == 1.0
                and context[self.long_context3].iloc[-1]["ha_trending_green"] == 1.0
                and context[self.long_context3].iloc[-1]["ha_non_trending"] != 1.0
                
                and context[self.long_context2].iloc[-1]["ha_trending_green"] == 1.0
                and context[self.long_context2].iloc[-1]["ha_non_trending"] != 1.0

                and context[self.long_context1].iloc[-1]["ha_trending_green"] == 1.0
                and context[self.long_context1].iloc[-1]["ha_non_trending"] != 1.0
                
                and context[self.long_context3].iloc[:-3][self.adx_col].max() > 30
                and context[self.long_context2].iloc[:-3][self.adx_col].max() > 20
                and context[self.long_context1].iloc[:-3][self.adx_col].max() > 20

                #and context[self.short_context1].iloc[-1][self.majority_col] > 0.6

                and current_run != TradeType.LONG):
                current_run = TradeType.LONG
                make_entry = True
                self.logger.debug("Entering long trade!")

            if (window.iloc[-1]["ha_short_trend"] == 1.0
                 # and window.iloc[-2]["ha_short_trend"] == 0.0
                #and context[self.long_context].iloc[-1]["ha_trending_red"] == 1.0
                and context[self.long_context3].iloc[-1]["ha_trending_red"] == 1.0
                and context[self.long_context3].iloc[-1]["ha_non_trending"] != 1.0
                
                and context[self.long_context2].iloc[-1]["ha_trending_red"] == 1.0
                and context[self.long_context2].iloc[-1]["ha_non_trending"] != 1.0

                and context[self.long_context1].iloc[-1]["ha_trending_red"] == 1.0
                and context[self.long_context1].iloc[-1]["ha_non_trending"] != 1.0
                
                and context[self.long_context3].iloc[:-3][self.adx_col].max() > 30
                and context[self.long_context2].iloc[:-3][self.adx_col].max() > 20
                and context[self.long_context1].iloc[:-3][self.adx_col].max() > 20
                
                #and context[self.short_context1].iloc[-1][self.majority_col] < 0.4

                and current_run != TradeType.SHORT):
                current_run = TradeType.SHORT
                make_entry = True
                self.logger.debug("Entering short trade!")

            #if context[self.long_context].iloc[:-4][self.st_col].nunique() == 1:
            #    make_entry = False

            if make_entry and not (np.isnan(self.get_entry(window, current_run))
                                   or np.isnan(self.get_stoploss(window, context, current_run))
                                   or np.isnan(self.get_target(window, context, current_run))):
                qty = max(self.max_budget // window.iloc[-1]["close"], self.min_quantity)

                self.logger.debug(f"Taking position!")
                quantity = self.cancel_active_orders(broker=broker,
                                                     scrip=scrip,
                                                     exchange=exchange,
                                                     product=self.product)
                self.perform_squareoff(broker=broker,
                                       scrip=scrip,
                                       exchange=exchange,
                                       quantity=quantity)

                entry_order = self.take_position(scrip=scrip,
                                                 exchange=exchange,
                                                 broker=broker,
                                                 position_type=PositionType.ENTRY,
                                                 trade_type=current_run,
                                                 price=self.get_entry(window, current_run),
                                                 quantity=qty,
                                                 product=self.product)
                if entry_order is None:
                    print(f"Placing order failed. skipping gtts; this happens if price movement is too fast.")
                else:
                    self.take_position(scrip=scrip,
                                    exchange=exchange,
                                    broker=broker,
                                    position_type=PositionType.STOPLOSS,
                                    trade_type=current_run,
                                    price=self.get_stoploss(window, context, current_run),
                                    quantity=qty,
                                    product=self.product,
                                    parent_order=entry_order)
                    self.take_position(scrip=scrip,
                                    exchange=exchange,
                                    broker=broker,
                                    position_type=PositionType.TARGET,
                                    trade_type=current_run,
                                    price=self.get_target(window, context, current_run),
                                    quantity=qty,
                                    product=self.product,
                                    parent_order=entry_order)
