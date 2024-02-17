from __future__ import annotations
from typing import Optional
import datetime
from enum import Enum
import pandas as pd
import numpy as np
from ..core.ds import (Order,
                       TradeType,
                       OrderState,
                       PositionType,
                       TradingProduct)

from ..core.strategy import Strategy
from ..core.indicator import (IndicatorPipeline,
                              HeikinAshiIndicator,
                              MAIndicator,
                              ATRIndicator,
                              GapUpDownIndicator,
                              BreakoutDetector,
                              DonchianIndicator)

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

        


class Strategy4(Strategy):

    def __init__(self,
                *args,
                atr_period: int = 14,
                ma_period: int = 20,
                ma_type: str = "WMA",
                long_context: str = "10min",
                dc_period: int = 15,
                product: TradingProduct = TradingProduct.MIS,
                **kwargs):
        self.atr_period = atr_period
        self.long_context = long_context
        self.ma_period = ma_period
        self.ma_type = ma_type
        self.product = product
        self.dc_period = dc_period
        
        self.dc_upper_col = f"donchianUpper_{self.dc_period}"
        self.dc_lower_col = f"donchianLower_{self.dc_period}"

        self.dc_indicator = DonchianIndicator(period=self.dc_period)
        dc_col_names = self.dc_indicator.get_default_column_names()

        self.dc_upper_col = dc_col_names["donchianUpper"]
        self.dc_lower_col = dc_col_names["donchianLower"]
        
        self.atr_indicator = ATRIndicator(period=self.atr_period)
        self.atr_col = self.atr_indicator.get_default_column_names()["ATR"]

        self.ma_indicator = MAIndicator(period=self.ma_period,
                                        ma_type=self.ma_type)
        self.ma_col = self.ma_indicator.get_default_column_names()["MA"]

        self.upper_breakout_indicator = BreakoutDetector(direction="up",
                                                         threshold_signal=self.dc_upper_col,
                                                         signal="high")

        self.lower_breakout_indicator = BreakoutDetector(direction="down",
                                                         threshold_signal=self.dc_lower_col,
                                                         signal="low")

        self.upper_breakout_col = self.upper_breakout_indicator.get_default_column_names()["breakout"]
        self.lower_breakout_col = self.lower_breakout_indicator.get_default_column_names()["breakout"]

        indicators =IndicatorPipeline([(self.atr_indicator, None, None),
                                       (self.dc_indicator, None, None),
                                       (self.ma_indicator, None, None),
                                       (GapUpDownIndicator(), None, None)
                                       ])
        indicators_long_context = IndicatorPipeline([(HeikinAshiIndicator(replace_ohlc=True), None, None),
                                                     (self.ma_indicator, None, None),
                                                     (self.dc_indicator, None, None),
                                                     (self.upper_breakout_indicator, None, None),
                                                     (self.lower_breakout_indicator, None, None),
                                                     ])
        context_indicators = {}
        for context in [self.long_context]:
            context_indicators[context] = indicators_long_context

        kwargs["indicator_pipeline"] = {"window": indicators,
                                        "context": context_indicators}
        self.rsi_upper_threshold = 60
        self.rsi_lower_threshold = 40

        kwargs["plottables"] = {"indicator_fields": [{"field": self.dc_upper_col,
                                                      "color": "magenta",
                                                      "panel": 0},
                                                     {"field": self.dc_lower_col,
                                                      "color": "magenta",
                                                      "panel": 0},
                                                    {"field": self.dc_upper_col,
                                                      "color": "blue",
                                                      "context": self.long_context,
                                                      "panel": 0},
                                                     {"field": self.dc_lower_col,
                                                      "color": "blue",
                                                      "context": self.long_context,
                                                      "panel": 0},
                                                     {"field": "gapup", "color": "green", "panel": 2},
                                                     {"field": "gapdown", "color": "red", "panel": 2},
                                                     {"field": self.upper_breakout_col, "color": "green", "panel": 3,
                                                     "context": self.long_context},
                                                     {"field": self.lower_breakout_col, "color": "red", "panel": 3,
                                                      "context": self.long_context},
                                                     {"field": self.ma_col, "color": "black",
                                                      "context": self.long_context,
                                                      "panel": 0},
                                                     ]}

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
        kwargs["context_required"] = [self.long_context]
    
        kwargs["intraday_squareoff"] = True
        kwargs["squareoff_hour"] = 15
        kwargs["squareoff_minute"] = 5


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
            trigger_price = candle[self.dc_upper_col] + candle[self.atr_col] * self.entry_atr_factor
            limit_price = candle[self.dc_upper_col] + candle[self.atr_col] * 2 * self.entry_atr_factor
        else:
            trigger_price = candle[self.dc_lower_col] - candle[self.atr_col] * 2
            limit_price = candle[self.dc_lower_col] - candle[self.atr_col] * 2 * self.entry_atr_factor
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
            trigger_price = candle["low"] - candle[self.atr_col] * self.sl_atr_factor
            limit_price = candle["low"] - candle[self.atr_col] * self.sl_atr_factor * 2
            
        else:
            trigger_price = candle["high"] + candle[self.atr_col] * self.sl_atr_factor
            limit_price = candle["high"] + candle[self.atr_col] * self.sl_atr_factor * 2
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

