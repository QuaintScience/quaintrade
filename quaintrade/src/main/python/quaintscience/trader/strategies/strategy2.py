from __future__ import annotations
from typing import Optional
import datetime
from enum import Enum
import pandas as pd
import numpy as np
from ..core.ds import (Order,
                       TradeType,
                       OrderState,
                       TransactionType,
                       PositionType,
                       TradingProduct)

from ..core.strategy import Strategy
from ..core.indicator import (IndicatorPipeline,
                              HeikinAshiIndicator,
                              WMAIndicator,
                              PivotIndicator,
                              ATRIndicator,
                              ADXIndicator,
                              RSIIndicator,
                              GapUpDownIndicator,
                              DonchianIndicator)
from ..core.ml.lorentzian import LorentzianClassificationIndicator
from ..core.roles import Broker
from ..core.statemachine import TradingStateMachine, Action
from ..core.util import (new_id,
                         get_key_from_scrip_and_exchange,
                         is_monotonically_increasing,
                         is_monotonically_decreasing,
                         is_local_maxima,
                         is_local_minima,
                         span,
                         sameday,
                         get_pivot_value)





class HeikinAshiPullBackStateMachine(TradingStateMachine):


    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def __get_closest_htf_candle(self,
                                 window: pd.DataFrame,
                                 context: dict[str, pd.DataFrame],
                                 contexts_to_use: list[str]):
        candle = window.iloc[-1]
        stoploss_candle = None
        for context_name in contexts_to_use:
            cdf = context[context_name]
            if (sameday(cdf.iloc[-1].name, candle.name)):
                stoploss_candle = cdf.iloc[-1]
                self.logger.info(f"Using {context_name} for closest htf candle")
                break
        if stoploss_candle is None:
            self.logger.info("Could not find htf candles; using current candle, instead...")
            stoploss_candle = candle
        return stoploss_candle

    def init(self):
        self.persistent_state.rsi_long_breakout_used = False
        self.persistent_state.rsi_short_breakout_used = False

    def run(self,
        strategy: Strategy,
        window: pd.DataFrame,
        context: dict[str, pd.DataFrame],
        orders: dict[str, Order],
        current_run: TradeType,
        **kwargs) -> Optional[Action]:

        if (len(window) < 3
            or any(len(context[c]) < 3 for c in context.keys())):
            self.logger.info("Not enough context...")
            return
        
        candle = window.iloc[-1]

        rsi_ctf = window[strategy.rsi_col]

        rsi_highs = 0
        rsi_lows = 0
        rsi_monotonically_increasing = 0
        rsi_monotonically_decreasing = 0
        for context_name, cdf in context.items():
            rsi = cdf.iloc[-1][strategy.rsi_col]
            if is_monotonically_increasing(cdf.iloc[-strategy.monotonically_increasing_context:][strategy.rsi_col]):
                rsi_monotonically_increasing += 1
            if is_monotonically_decreasing(cdf.iloc[-strategy.monotonically_decreasing_context:][strategy.rsi_col]):
                rsi_monotonically_decreasing += 1
            if rsi >= strategy.rsi_upper_threshold:
                rsi_highs += 1
            if rsi <= strategy.rsi_lower_threshold:
                rsi_lows += 1

        adx_monotonically_increasing = 0
        adx_monotonically_decreasing = 0
        adx_local_maximas = 0
        adx_local_minimas = 0
        for context_name, cdf in context.items():
            adx = cdf[strategy.adx_col]
            if is_monotonically_increasing(adx.iloc[-strategy.monotonically_increasing_context:]):
                adx_monotonically_increasing += 1
            if is_monotonically_decreasing(adx.iloc[-strategy.monotonically_decreasing_context:]):
                adx_monotonically_decreasing += 1
            if is_local_maxima(adx, 1):
                adx_local_maximas += 1
            if is_local_minima(adx, 1):
                adx_local_minimas += 1

        self.logger.info(f"rsi_highs {rsi_highs} "
                         f"rsi_lows {rsi_lows} "
                         f", rsi_monotonically_increasing {rsi_monotonically_increasing}"
                         f", rsi_monotonically_decreasing {rsi_monotonically_decreasing}"
                         f", adx_monotonically_increasing {adx_monotonically_increasing}"
                         f", adx_monotonically_decreasing {adx_monotonically_decreasing}"
                         f", adx_local_maximas {adx_local_maximas}"
                         f", adx_local_minimas {adx_local_minimas}")

        if (rsi_lows >= len(context) * 0.5):
            self.logger.info("Resetting short rsi breakout flag as RSIs have risen.")
            self.persistent_state.rsi_long_breakout_used = False

        if (rsi_highs >= len(context) * 0.5):
            self.logger.info("Resetting short rsi breakout flag as RSIs have fallen.")
            self.persistent_state.rsi_short_breakout_used = False

        if self.persistent_state.dont_trade_today:
            if sameday(self.persistent_state.today, candle.name):
                self.logger.info("Won't trade today!")
                return

        if not sameday(self.persistent_state.today, candle.name):
            self.persistent_state.today = candle.name
            self.persistent_state.donchian_up_breaks = 0
            self.persistent_state.donchian_down_breaks = 0
            self.persistent_state.dont_trade_today = False

        self.logger.info(f"Current state {self.state.id} "
                         f" Shortbreakoutused: {self.persistent_state.rsi_short_breakout_used}; "
                         f" Longbreakoutused: {self.persistent_state.rsi_long_breakout_used}")

        """
        if ((is_monotonically_decreasing(rsi_ctf[:-5])
            and current_run == TradeType.LONG)
            or (is_monotonically_increasing(rsi_ctf[:-5])
                and current_run == TradeType.SHORT)):
            self.logger.info(f"RSI continuously falling in trading time frame for 5 periods.")
            self.persistent_state.rsi_long_breakout_used = False
            self.persistent_state.rsi_short_breakout_used = False
            self.reset()
            return Action.CancelPosition

        if ((adx_monotonically_decreasing >= 0.9 * len(context)
            and current_run == TradeType.LONG)
            or (adx_monotonically_increasing >= 0.9 * len(context)
                and current_run == TradeType.SHORT)):
            self.logger.info(f"ADX monotonically inc/dec in two of the contexts for run {current_run}.")
            self.persistent_state.rsi_long_breakout_used = False
            self.persistent_state.rsi_short_breakout_used = False
            self.reset()
            return Action.CancelPosition
        
        if ((adx_local_maximas >= 2
            and current_run == TradeType.LONG)
            or (adx_local_minimas >= 2
                and current_run == TradeType.SHORT)):
            self.logger.info(f"ADX maxima/minimas found for run {current_run}.")
            self.persistent_state.rsi_long_breakout_used = False
            self.persistent_state.rsi_short_breakout_used = False
            self.reset()
            return Action.CancelPosition
        """
        """
        if (orders["target"] is not None
            and orders["stoploss"] is not None):
            action = None
            th = context[strategy.long_contexts[0]].iloc[-1][strategy.short_wma_col]
            if ((current_run == TradeType.LONG
                 and candle["close"] < th - (4 * candle[strategy.atr_col]))
                or (current_run == TradeType.SHORT
                    and candle["close"] > th + (4 * candle[strategy.atr_col]))):
                self.logger.info(f"Price has crossed short context wma "
                                 f"{candle['close']} < {th}. So cancelling order.")
                self.persistent_state.rsi_long_breakout_used = False
                self.persistent_state.rsi_short_breakout_used = False
                self.reset()
                return Action.CancelPosition

        if (orders["target"] is not None
            and orders["stoploss"] is not None):
            action = None
            th = context[strategy.long_contexts[-1]].iloc[-1][strategy.short_wma_col]
            if ((current_run == TradeType.LONG
                 and candle["close"] < th) #- (1. * candle[strategy.atr_col]))
                or (current_run == TradeType.SHORT
                    and candle["close"] > th) #+ (1. * candle[strategy.atr_col]))):
                ):
                self.logger.info(f"Price has crossed long context wma "
                                 f"{candle['close']} < {th}. So cancelling order.")
                self.persistent_state.rsi_long_breakout_used = False
                self.persistent_state.rsi_short_breakout_used = False
                self.reset()
                return Action.CancelPosition
        """
        if self.state.id is None:
            self.logger.info("state id is None; so resettting..")
            self.persistent_state.rsi_short_breakout_used = False
            self.persistent_state.rsi_long_breakout_used = False
            self.reset()

        if ((orders["entry"] is None
             or orders["entry"].state == OrderState.COMPLETED)
            and orders["stoploss"] is None
            and orders["target"] is None
            and self.state.id == "exit_swing"):
            self.logger.info(f"Since state is {self.state.id} "
                             "and no orders exist, resetting state")
            self.persistent_state.rsi_long_breakout_used = False
            self.persistent_state.rsi_short_breakout_used = False
            self.reset()

        if (orders["stoploss"] is not None
            and orders["target"] is not None):
            th = 4.0 * candle[strategy.atr_col]
            direction = None
            if current_run == TradeType.LONG:
                direction = "down"
            elif current_run == TradeType.SHORT:
                direction = "up"
            if direction is not None:
                fall = span(window, size=3, direction=direction)
                if fall > th:
                    self.logger.info(f"Sudden fall in price detected threshold: {th}; fall={fall}")
                    self.reset()
                    self.persistent_state.rsi_long_breakout_used = False
                    self.persistent_state.rsi_short_breakout_used = False
                    return Action.CancelPosition

        if (orders["entry"] is not None
            and orders["entry"].state == OrderState.PENDING
            and window.iloc[-1].name - orders["entry"].timestamp >= datetime.timedelta(minutes=5)):
            self.logger.info("Stale order found. exiting position.")
            self.persistent_state.rsi_long_breakout_used = False
            self.persistent_state.rsi_short_breakout_used = False
            self.reset()
            return Action.CancelPosition


        if (orders["stoploss"] is not None):
            if current_run == TradeType.LONG:
                self.state.stoploss_candle = candle
                return Action.UpdateStoploss

        if candle["gapup"] == 1.0 and candle["open"] > candle["close"]:
            self.persistent_state.dont_trade_today = True
            self.logger.info("Gapup with red candle; don't trade today!")


        if candle["gapdown"] == 1.0 and candle["close"] > candle["open"]:
            self.persistent_state.dont_trade_today = True
            self.logger.info("Gapdown with green candle; don't trade today!")

        
        if candle["low"] < window.iloc[-2][strategy.dc_lower_col]:
            self.persistent_state.donchian_down_breaks += 1
            self.persistent_state.donchian_up_breaks = 0
        elif candle["high"] > window.iloc[-2][strategy.dc_upper_col]:
            self.persistent_state.donchian_up_breaks += 1
            self.persistent_state.donchian_down_breaks = 0

        long_context_wma = context[strategy.long_contexts[-1]].iloc[-2:][strategy.short_wma_col]
        short_context_wma = context[strategy.long_contexts[0]].iloc[-2:][strategy.short_wma_col]

        if candle["high"] < short_context_wma[-1]:
            self.persistent_state.donchain_up_breaks = 0
        if candle["low"] > short_context_wma[-1]:
            self.persistent_state.donchain_down_breaks = 0

        if (orders["target"] is not None
            and ((current_run == TradeType.LONG
            and self.persistent_state.donchian_down_breaks >= 5)
            or (current_run == TradeType.SHORT
            and self.persistent_state.donchian_up_breaks >=5))):
            self.logger.info("opposite donchian movement detected. Quitting trade")
            self.reset()
            self.persistent_state.rsi_long_breakout_used = False
            self.persistent_state.rsi_short_breakout_used = False
            return Action.CancelPosition

        """
        lorentzian_long_score = sum([context[ctx].iloc[-1]["isBullish"] for ctx in strategy.long_contexts])
        lorentzian_short_score = sum([context[ctx].iloc[-1]["isBearish"] for ctx in strategy.long_contexts])
        if (current_run == TradeType.LONG
            and lorentzian_long_score < lorentzian_short_score):
            self.logger.info("Long trade with bearish sign. exiting.")
            self.reset()
            self.persistent_state.rsi_long_breakout_used = False
            self.persistent_state.rsi_short_breakout_used = False
            return Action.CancelPosition
        elif (current_run == TradeType.SHORT
              and lorentzian_long_score > lorentzian_short_score):
            self.logger.info("Short trade with bullish sign. exiting.")
            self.reset()
            self.persistent_state.rsi_long_breakout_used = False
            self.persistent_state.rsi_short_breakout_used = False
            return Action.CancelPosition
        """

        match self.state.id:

            case "start":
                if strategy.can_trade(window, context):
                    
                    if (orders["entry"] is not None
                        and orders["entry"].state == OrderState.COMPLETED
                        and sameday(orders["entry"].timestamp,
                                    window.iloc[-1].name)):
                        self.logger.info("Already traded for the day!")
                        return

                    print("can trade. checking for conditions")
                    entry_done = False
                    """
                    self.logger.info(f"is ctf local minima: {is_local_minima(rsi_ctf, context_size=1)} "
                                     f"is ctf local maxima: {is_local_maxima(rsi_ctf, context_size=1)} "
                                     f"WMA stat inc {is_monotonically_increasing(short_context_wma)}"
                                     f"{is_monotonically_increasing(long_context_wma)}"
                                     f"WMA stat dec {is_monotonically_decreasing(short_context_wma)}"
                                     f"{is_monotonically_decreasing(long_context_wma)}")
                    self.logger.info(f"Consecutive donchian breaks: "
                                     f"UP: {self.persistent_state.donchian_up_breaks}; "
                                     f"DOWN: {self.persistent_state.donchian_down_breaks}")
                    
                    long_score = sum([rsi_highs, #>= 2,
                                      rsi_monotonically_increasing, #>= 2,
                                      adx_monotonically_increasing, #>= 1,
                                      is_local_minima(rsi_ctf, context_size=1),
                                      candle["low"] > long_context_wma.iloc[-1],
                                      is_monotonically_increasing(short_context_wma),
                                      is_monotonically_increasing(long_context_wma),
                                      self.persistent_state.donchian_up_breaks,
                                      long_context_wma[-1] - long_context_wma[0] >= candle[strategy.atr_col] * 1])
                    short_score = sum([rsi_lows, #>= 2,
                                       rsi_monotonically_decreasing, #>= 2,
                                       adx_monotonically_increasing, #>= 1,
                                       is_local_maxima(rsi_ctf,
                                                       context_size=1),
                                       candle["high"] < long_context_wma.iloc[-1],
                                       self.persistent_state.donchian_down_breaks,
                                       is_monotonically_decreasing(short_context_wma),
                                       is_monotonically_decreasing(long_context_wma),
                                       long_context_wma[0] - long_context_wma[-1] >= candle[strategy.atr_col] * 1])
                    """

                    if (
                        #not self.persistent_state.rsi_long_breakout_used
                         #and (long_score - short_score) >= 18
                         not window.iloc[-2]["isBullish"] and window.iloc[-1]["isBullish"]
                         #and lorentzian_long_score > 3
                        ):
                        
                        #self.logger.info(f"Long trend found {long_score} {short_score}.")
                        self.logger.info("Long found")
                        self.state.potential_trade = TradeType.LONG
                        #self.persistent_state.rsi_long_breakout_used = True
                        #self.persistent_state.rsi_short_breakout_used = False
                        entry_done = True
                    elif (
                          #(not self.persistent_state.rsi_short_breakout_used
                          #and (short_score - long_score) >= 18
                          #and candle["isBearish"])
                        not window.iloc[-2]["isBearish"] and window.iloc[-1]["isBearish"]
                        #and lorentzian_short_score > 3
                        ):
                        #self.logger.info(f"Short trend found {long_score} {short_score}")
                        self.logger.info("Short trade found.")
                        self.state.potential_trade = TradeType.SHORT
                        #self.persistent_state.rsi_short_breakout_used = True
                        #self.persistent_state.rsi_long_breakout_used = False
                        entry_done = True
                    if entry_done:
                        self.state.entry_candle = candle
                        self.state.stoploss_candle = self.__get_closest_htf_candle(window,
                                                                                context,
                                                                                contexts_to_use=strategy.long_contexts)
                        self.state.id = "exit_swing"
                        return Action.TakePosition


class Strategy2(Strategy):

    def __init__(self,
                *args,
                atr_period: int = 14,
                short_wma_period: int = 13,
                medium_wma_period: int = 44,
                long_contexts: list[str] = ["7min",
                                            "10min",
                                            "15min",
                                            "30min",
                                            "45min"],
                rsi_period: int = 14,
                dc_period: int = 30, #15,
                short_dc_period: int = 1,
                pivot_period: int = 14,
                adx_period: int = 15,
                monotonically_increasing_context: int = 3,
                monotonically_decreasing_context: int = 3,
                product: TradingProduct = TradingProduct.MIS,
                **kwargs):
        self.atr_period = atr_period
        self.long_contexts = long_contexts
        self.short_wma_period = short_wma_period
        self.medium_wma_period = medium_wma_period
        self.product = product
        self.rsi_period = rsi_period
        self.monotonically_increasing_context = monotonically_increasing_context
        self.monotonically_decreasing_context = monotonically_decreasing_context
        self.atr_col = f"ATR_{self.atr_period}"
        self.short_wma_col = f"WMA_{self.short_wma_period}"
        self.medium_wma_col = f"WMA_{self.medium_wma_period}"
        self.dc_period = dc_period
        self.short_dc_period = short_dc_period
        self.pivot_period = pivot_period
        self.dc_upper_col = f"donchianUpper_{self.dc_period}"
        self.dc_lower_col = f"donchianLower_{self.dc_period}"
        self.dc_sl_upper_col = f"donchianUpper_{self.short_dc_period}"
        self.dc_sl_lower_col = f"donchianLower_{self.short_dc_period}"
        self.rsi_col = f"RSI_{self.rsi_period}"
        self.pivot_high_col = f"pivot_high_{self.pivot_period}"
        self.pivot_low_col = f"pivot_low_{self.pivot_period}"
        self.adx_period = adx_period
        self.adx_col = f"ADX_{self.adx_period}"
        indicators =IndicatorPipeline([(ATRIndicator(period=self.atr_period), None, None),
                                       (DonchianIndicator(period=self.dc_period), None, None),
                                       (WMAIndicator(period=self.medium_wma_period), None, None),
                                       (RSIIndicator(period=self.rsi_period), None, None),
                                       (PivotIndicator(left_period=self.pivot_period,
                                                       right_period=self.pivot_period), None, None),
                                       (DonchianIndicator(period=self.short_dc_period), None, None),
                                       (GapUpDownIndicator(), None, None),
                                       (LorentzianClassificationIndicator(neighbors_count = 8,
                                                                          user_ema_filter=False,
                                                                          use_sma_filter=False,
                                                                          use_adx_filter=False,
                                                                          use_kernel_smoothing=True,
                                                                          use_dynamic_exists=True,
                                                                          use_volatility_filter=True,
                                                                          lookback_window=32,
                                                                          regime_threshold=1), None, None)])
        indicators_long_context = IndicatorPipeline([(HeikinAshiIndicator(), None, None),
                                                     (ATRIndicator(period=self.atr_period), None, None),
                                                     (WMAIndicator(period=self.short_wma_period), None, None),
                                                     (RSIIndicator(period=self.rsi_period), None, None),
                                                     (ADXIndicator(period=self.adx_period), None, None),
                                                     (DonchianIndicator(period=self.dc_period), None, None),
                                                     (DonchianIndicator(period=self.short_dc_period), None, None),
                                                     (LorentzianClassificationIndicator(neighbors_count = 16,
                                                                                        user_ema_filter=False,
                                                                                        use_sma_filter=False,
                                                                                        use_adx_filter=False,
                                                                                        use_kernel_smoothing=True,
                                                                                        use_dynamic_exists=True,
                                                                                        use_volatility_filter=True,
                                                                                        regime_threshold=-0.1), None, None)])
        context_indicators = {}
        for context in self.long_contexts:
            context_indicators[context] = indicators_long_context

        kwargs["indicator_pipeline"] = {"window": indicators,
                                        "context": context_indicators}
        self.rsi_upper_threshold = 60
        self.rsi_lower_threshold = 40
        self.rsi_reversal_threshold = 10
        kwargs["plottables"] = {"indicator_fields": [{"field": self.dc_upper_col, "color": "magenta", "panel": 0},
                                                     {"field": self.dc_lower_col, "color": "magenta", "panel": 0},
                                                     {"field": self.dc_sl_upper_col, "color": "black", "panel": 0},
                                                     {"field": self.dc_sl_lower_col, "color": "black", "panel": 0},
                                                     {"field": "gapup", "color": "green", "panel": 4},
                                                     {"field": "gapdown", "color": "red", "panel": 4},
                                                     {"field": self.short_wma_col, "color": "black",
                                                      "context": self.long_contexts[-1],
                                                      "panel": 0},
                                                     {"field": self.short_wma_col, "color": "black",
                                                      "context": self.long_contexts[0],
                                                      "panel": 0},
                                                     {"field": self.rsi_col, "color": "black", "panel": 2,
                                                      "fill_region": [self.rsi_lower_threshold,
                                                                      self.rsi_upper_threshold]
                                                      },
                                                      {"field": "yhat1", "color": "red"},
                                                      {"field": "yhat2", "color": "green"},
                                                     ]}
        for context in self.long_contexts:
            kwargs["plottables"]["indicator_fields"].append({"field": self.rsi_col,
                                                             "panel": 2,
                                                             "context": context})
            kwargs["plottables"]["indicator_fields"].append({"field": self.adx_col,
                                                             "panel": 3,
                                                             "context": context})
        kwargs["plot_context_candles"] = []
        non_trading_timeslots = []
        #non_trading_timeslots.extend(Strategy.NON_TRADING_FIRST_HOUR)
        non_trading_timeslots.extend([{"from": {"hour": 9,
                                       "minute": 15},
                                       "to": {"hour": 9,
                                       "minute": 25}}])
        non_trading_timeslots.extend([{"from": {"hour": 14,
                                       "minute": 45},
                                       "to": {"hour": 15,
                                       "minute": 59}}])
        kwargs["non_trading_timeslots"] = non_trading_timeslots
        kwargs["context_required"] = self.long_contexts
    
        kwargs["intraday_squareoff"] = True
        kwargs["squareoff_hour"] = 15
        kwargs["squareoff_minute"] = 00


        self.entry_atr_factor = 0.02
        self.sl_atr_factor = 0.5
        self.risk_reward_ratio = 10.0
        # self.max_risk = 2.
        super().__init__(*args, **kwargs)
        self.reset_state_machine()


    def get_entry(self,
                  candle: pd.Series,
                  trade_type: TradeType):

        trigger_price, limit_price = None, None
        if trade_type == TradeType.LONG:
            trigger_price = candle["high"] + candle[self.atr_col] * self.entry_atr_factor
            limit_price = candle["high"] + candle[self.atr_col] * 2 * self.entry_atr_factor
        else:
            trigger_price = candle["low"] - candle[self.atr_col] * 2
            limit_price = candle["low"] - candle[self.atr_col] * 2 * self.entry_atr_factor
        return trigger_price, limit_price

    def get_target(self,
                   candle: pd.Series,
                   entry: float,
                   stoploss: float,
                   trade_type: TradeType):
        
        if trade_type == TradeType.LONG:
            return candle["high"] + self.risk_reward_ratio * (entry - stoploss)
        else:
            return candle["low"] - self.risk_reward_ratio * (stoploss - entry)

    def get_stoploss(self,
                     candle: pd.Series,
                     trade_type: TradeType):
        trigger_price, limit_price = None, None
        if trade_type == TradeType.LONG:
            trigger_price = candle[self.dc_lower_col] - candle[self.atr_col] * self.sl_atr_factor
            limit_price = candle[self.dc_lower_col] - candle[self.atr_col] * self.sl_atr_factor * 2
            
        else:
            trigger_price = candle[self.dc_upper_col] + candle[self.atr_col] * self.sl_atr_factor
            limit_price = candle[self.dc_upper_col] + candle[self.atr_col] * self.sl_atr_factor * 2
        return trigger_price, limit_price

    def perform_action(self,
                        broker: Broker,
                        sm: HeikinAshiPullBackStateMachine,
                        orders: dict[str, Order],
                        current_run: TradeType,
                        action: Action):
        self.logger.info(f"Performing action {action}")

        if action == Action.TakePosition:
            qty = max(self.max_budget // sm.state.entry_candle["close"],
                      self.min_quantity)
            self.max_stoploss = 100
            print(self.max_stoploss)
            (entry_trigger_price,
             entry_limit_price) = self.get_entry(sm.state.entry_candle,
                                                 sm.state.potential_trade)
            entry_order = self.take_position(scrip=sm.scrip,
                                             exchange=sm.exchange,
                                             broker=broker,
                                             position_type=PositionType.ENTRY,
                                             trade_type=sm.state.potential_trade,
                                             trigger_price=entry_trigger_price,
                                             limit_price=entry_limit_price,
                                             quantity=qty,
                                             product=self.product)

            if entry_order is None:
                print("Placing order failed. "
                        "skipping gtts; this happens if price"
                        " movement is too fast.")
            
            else:
                (stoploss_trigger_price,
                 stoploss_limit_price) = self.get_stoploss(sm.state.stoploss_candle,
                                                           sm.state.potential_trade)
                self.take_position(scrip=sm.scrip,
                                   exchange=sm.exchange,
                                   broker=broker,
                                   position_type=PositionType.STOPLOSS,
                                   trade_type=sm.state.potential_trade,
                                   trigger_price=stoploss_trigger_price,
                                   limit_price=stoploss_limit_price,
                                   quantity=qty,
                                   product=self.product,
                                   parent_order=entry_order)
                self.take_position(scrip=sm.scrip,
                                   exchange=sm.exchange,
                                   broker=broker,
                                   position_type=PositionType.TARGET,
                                   trade_type=sm.state.potential_trade,
                                   limit_price=self.get_target(candle=sm.state.entry_candle,
                                                               entry=entry_trigger_price,
                                                               stoploss=stoploss_trigger_price,
                                                               trade_type=sm.state.potential_trade),
                                   quantity=qty,
                                   product=self.product,
                                   parent_order=entry_order)
        elif action == Action.CancelPosition:
            quantity = self.cancel_active_orders(broker,
                                                 scrip=sm.scrip,
                                                 exchange=sm.exchange,
                                                 product=self.product)
            if quantity != 0:
                self.logger.warn(f"Looks like entry has fructified; performing squareoff")
                self.perform_squareoff(broker=broker,
                                       scrip=sm.scrip,
                                       exchange=sm.exchange,
                                       product=self.product,
                                       quantity=quantity)
        elif action == Action.UpdateStoploss:
            slorder = orders["stoploss"]
            new_sl, new_trigger = self.get_stoploss(sm.state.stoploss_candle,
                                                    current_run)
            update_sl = False
            if ((current_run == TradeType.LONG
                and slorder.limit_price < new_sl)
                or (current_run == TradeType.SHORT
                and slorder.limit_price > new_sl)):
                slorder.limit_price = new_sl
                slorder.trigger_price = new_trigger
                broker.update_order(slorder)

    def reset_state_machine(self,
                            scrip: str = None,
                            exchange: str = None) -> None:
        if scrip is None and exchange is None:
            self.logger.warn(f"Resetting all states as scrip and exchange were none.")
            self.state_machines = {}
            return
        k = get_key_from_scrip_and_exchange(scrip, exchange)
        self.state_machines[k] = HeikinAshiPullBackStateMachine(scrip=scrip,
                                                        exchange=exchange)

    def get_state_machine(self, scrip: str,
                        exchange: str) -> HeikinAshiPullBackStateMachine:
        k = get_key_from_scrip_and_exchange(scrip, exchange)
        if k not in self.state_machines:
            self.reset_state_machine(scrip=scrip,
                                    exchange=exchange)
        return self.state_machines[k]

    def apply_impl(self,
                broker: Broker,
                scrip: str,
                exchange: str,
                window: pd.DataFrame,
                context: dict[str, pd.DataFrame]) -> Optional[TradeType]:

        sm = self.get_state_machine(scrip, exchange)

        current_target_order = self.get_current_position_order(broker,
                                                            scrip=scrip,
                                                            exchange=exchange,
                                                            product=self.product,
                                                            position_name="target",
                                                            refresh_order_cache=False,
                                                            states=[OrderState.PENDING])

        current_stoploss_order = self.get_current_position_order(broker,
                                                                scrip=scrip,
                                                                exchange=exchange,
                                                                product=self.product,
                                                                position_name="stoploss",
                                                                refresh_order_cache=False,
                                                                states=[OrderState.PENDING])
        current_entry_order = self.get_current_position_order(broker,
                                                            scrip=scrip,
                                                            exchange=exchange,
                                                            product=self.product,
                                                            position_name="entry",
                                                            refresh_order_cache=False,
                                                            states=[OrderState.PENDING])
        if (current_stoploss_order is not None 
            and current_target_order is not None):
            current_entry_order = self.get_current_position_order(broker,
                                                                  scrip=scrip,
                                                                  exchange=exchange,
                                                                  product=self.product,
                                                                  position_name="entry",
                                                                  refresh_order_cache=False,
                                                                  states=[OrderState.COMPLETED])
        elif (current_stoploss_order is None
              and current_target_order is None
              and current_entry_order is None):
              current_entry_order = self.get_current_position_order(broker,
                                                                  scrip=scrip,
                                                                  exchange=exchange,
                                                                  product=self.product,
                                                                  position_name="entry",
                                                                  refresh_order_cache=False,
                                                                  states=[OrderState.COMPLETED])

        orders = {"entry": current_entry_order,
                    "target": current_target_order,
                    "stoploss": current_stoploss_order}
        current_run = self.get_current_run(broker=broker,
                                            scrip=scrip,
                                            exchange=exchange,
                                            refresh_cache=True)
        if current_entry_order is not None:
            self.logger.info(f"Entry ({current_entry_order.state}): {current_entry_order.transaction_type} / "
                            f"{current_entry_order.quantity} @ TRG "
                            f"{current_entry_order.trigger_price} "
                            f"LMT {current_entry_order.limit_price}")
        else:
            self.logger.info("Entry: None")
        if current_stoploss_order is not None:
            self.logger.info(f"SL: {current_stoploss_order.transaction_type} / "
                            f"{current_stoploss_order.quantity} @ TRG "
                            f"{current_stoploss_order.trigger_price} "
                            f"LMT {current_stoploss_order.limit_price}")
            self.logger.info(f"TGT: {current_target_order.transaction_type} / "
                            f"{current_target_order.quantity} @ TRG "
                            f"{current_target_order.trigger_price} "
                            f"LMT {current_target_order.limit_price}")
        else:
            self.logger.info("SL: None")
            self.logger.info("TGT: None")
        
        self.logger.info("====Before running state machine====")
        sm.print()

        action = sm.run(strategy=self,
                        window=window,
                        context=context,
                        orders=orders,
                        current_run=current_run)

        self.perform_action(broker=broker,
                            sm=sm,
                            action=action,
                            orders=orders,
                            current_run=current_run)

        self.logger.info("====After running state machine====")
        sm.print()

