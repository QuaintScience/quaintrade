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
                              WMAIndicator,
                              ATRIndicator,
                              PauseBarIndicator,
                              SupportIndicator,
                              SupertrendIndicator,
                              RSIIndicator)
from ..core.roles import Broker
from ..core.statemachine import TradingStateMachine
from ..core.util import new_id, get_key_from_scrip_and_exchange



class RSIPullBackStateMachine(TradingStateMachine):

    class Action(Enum):
        TakePosition = "TakePosition"
        AverageOutTrade = "AverageOutTrade"
        CancelPosition = "CancelPosition"
        CreateEntryWithFixetSL = "CreateEntryWithFixetSL"
        UpdateStoploss = "UpdateStoploss"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def is_green_candle(self, row):
            return row["close"] > row["open"]

    def is_red_candle(self, row):
        return not self.is_green_candle(row)

    def run(self,
            strategy: Strategy,
            candle: pd.Series,
            long_context: pd.Series,
            orders: dict[str, Order],
            context: dict[str, any]) -> Optional[RSIPullBackStateMachine.Action]:
        
        if self.state.id == 5:
            # All work done!
            self.reset()

        # Remove stale orders
        if orders["entry"] is not None:
            tdiff = candle.name - orders["entry"].timestamp
            if tdiff > datetime.timedelta(minutes=6):
                self.logger.info(f"Recommending cancellation of position as it's been stale."
                                 f"tdiff={tdiff}")
                return RSIPullBackStateMachine.Action.CancelPosition

        # Clear non-fructified orders if candles start forming below WMA
        # and also clear state
        if self.state.expected_run == TradeType.LONG:
            
            if orders["entry"] is not None:
                if min(candle["open"], candle["close"]) < long_context[strategy.medium_wma_col] * 0.98:
                    self.logger.info(f"Cancelling position as it is non-fructified and "
                                    f"candles are forming below WMA")
                    return RSIPullBackStateMachine.Action.CancelPosition
            elif (self.state.id > 0
                  and min(candle["open"], candle["close"]) < candle[strategy.medium_wma_col]):
                self.logger.info(f"Resetting state as candle formed below candle wma")
                self.reset()
                return

        # Case where a new long-context is formed.
        if context["new_context_bar"]:
            """
            if orders["stoploss"] is not None:
                # Trail stoploss
                self.state.stoploss_candle = candle
                self.state.stoploss_long_context = long_context
                return RSIPullBackStateMachine.Action.UpdateStoploss
            """
            if context["rsi"] < context["rsi_upper_threshold"]:
                self.persistent_state.entry_cnt = 0
            if context["is_pause_bar"]:
                self.logger.info("A new LC PAUSE bar has formed. "
                            "Starting checks for entry conditions...")
                print("State", self.state.id, self.state.expected_run)
                if (self.state.id == 0
                    and orders["entry"] is None
                    and orders["stoploss"] is None
                    and orders["target"] is None):
                    self.logger.info("Case-A: Clean slate.")
                    if  abs(candle[strategy.medium_wma_col] - long_context[strategy.medium_wma_col]) > 2 * candle[strategy.atr_col]:
                        if context["rsi"] > context["rsi_upper_threshold"]:
                            self.logger.info(f"LC RSI {context['rsi']}> {context['rsi_upper_threshold']}."
                                            f"Searching for long entries and clearing short positions")
                            if self.persistent_state.entry_cnt < 1:
                                self.state.start_candle = candle
                                self.state.expected_run = TradeType.LONG
                                self.state.id = 1
                            else:
                                self.logger.info(f"Already tried {self.persistent_state.entry_cnt}"
                                                f" after rsi breakout. so skipping...")
                            return
                        """elif context["rsi"] < context["rsi_lower_threshold"]:
                            self.logger.info(f"LC RSI {context['rsi']} < {context['rsi_lower_threshold']}."
                                            f"Searching for short entries and clearing long positions")
                            if self.persistent_state.entry_cnt < 1:
                                self.state.start_candle = candle
                                self.state.expected_run = TradeType.SHORT
                                self.state.id = 1
                            else:
                                self.logger.info(f"Already tried {self.persistent_state.entry_cnt}"
                                                f" after rsi breakout. so skipping...")
                        """
                    elif (context["rsi"] < context["rsi_upper_threshold"]
                        and self.state.id <= 3
                        and (orders["stoploss"] is None
                            and orders["target"] is None)):
                            self.logger.info(f"RSI {context['rsi']}< {context['rsi_upper_threshold']}. "
                                             f"Looks like we were searching for {self.state.expected_run}."
                                             f"Stopping state machine...")
                            self.reset()
                            return
                else:
                    self.logger.info("A trade is in progress")
                    if (self.state.id <= 3
                        and (orders["stoploss"] is None
                            and orders["target"] is None)):
                            self.logger.info("Resetting state as we are before entry fructification.")
                            self.reset()
                            return
            else:
                self.logger.info("LC Non Pausebar formed")
                if (self.state.id <= 3
                    and (orders["stoploss"] is None
                            and orders["target"] is None)):
                        self.logger.info("Resetting state as we are before entry fructification.")
                        self.reset()
                if (orders["entry"] is not None):
                    return RSIPullBackStateMachine.Action.CancelPosition
                return


        if self.state.expected_run == TradeType.LONG:
            if self.state.id == 1:
                self.logger.info("EntrySearch Stage1: Searching for jump up")
                if self.is_green_candle(candle):
                    self.state.pullback_high = candle
                    return
                elif isinstance(self.state.pullback_high, pd.Series):
                    self.state.id = 2
                    self.state.pulldown_low = candle
                    self.state.entry_candle = candle
                    self.persistent_state.entry_cnt += 1
                    return RSIPullBackStateMachine.Action.CreateEntryWithFixetSL
            elif self.state.id == 2:
                self.logger.info("EntrySearch Stage2: Searching pull down")
                if self.is_red_candle(candle):
                    self.state.pulldown_low = candle
                    return
                else:
                    self.state.id = 3
                    self.state.pullback_high2 = candle
                    self.state.stoploss_candle = self.state.pulldown_low
                    self.state.stoploss_long_context = long_context
                    return RSIPullBackStateMachine.Action.UpdateStoploss
            elif self.state.id == 3:
                self.logger.info("EntrySearch Stage3: Check for pull back failure")
                if self.is_green_candle(candle):
                    self.state.pullback_high2 = candle
                    return
                else:
                    self.logger.debug("Entry Search Stage3: Pull back noticed. "
                                        "Observing...")
                    if (orders["entry"] is not None
                        and candle["low"] < self.state.stoploss_candle["low"]):
                            self.logger.info("Pullback failed.")
                            self.state.pullback_high = self.state.pullback_high2
                            self.state.id = 1
                            return RSIPullBackStateMachine.Action.CancelPosition
                    else:
                        self.state.id = 4
                        self.state.pullback_low = candle
                    return
            elif self.state.id == 4:
                self.logger.info("EntrySearch Stage 4: Check for false traps")
                if (orders["stoploss"] is not None
                    or orders["target"] is not None):
                    if self.is_red_candle(candle):
                        self.state.pullback_low = candle
                        return
                    else:
                        if candle["low"] < self.state.stoploss_long_context["low"] - strategy.sl_atr_factor * candle[strategy.atr_col]:
                            self.logger.info("Trap found!")
                            print(candle)
                            self.state.id = 5
                            self.state.entry_candle = candle
                            self.state.stoploss_long_context = long_context # self.state.pullback_low
                            self.state.stoploss_candle = self.state.pullback_low
                            return RSIPullBackStateMachine.Action.AverageOutTrade
                        self.state.id = 3  # Restart look for pull back failures
                else:
                    # We are out of the trade either because of SL or target
                    self.logger.info("We are out of the trade either because of SL or target. Resetting sm.")
                    self.reset()



class Strategy1(Strategy):

    def __init__(self,
                *args,
                st_period: int = 14,
                st_multiplier: float = 3.0,
                rsi_period: int = 14,
                atr_period: int = 14,
                short_wma_period: int = 9,
                medium_wma_period: int = 20,
                long_context: str = "10min",
                product: TradingProduct = TradingProduct.MIS,
                **kwargs):
        self.rsi_period = rsi_period
        self.atr_period = atr_period
        self.st_period = st_period
        self.st_multiplier = st_multiplier
        self.long_context = long_context
        self.short_wma_period = short_wma_period
        self.medium_wma_period = medium_wma_period
        self.product = product
        self.atr_col = f"ATR_{self.atr_period}"
        self.rsi_col = f"RSI_{self.rsi_period}"
        self.short_wma_col = f"WMA_{self.short_wma_period}"
        self.medium_wma_col = f"WMA_{self.medium_wma_period}"
        self.st_col = f"supertrend_{self.st_period}_{self.st_multiplier:.1f}"
        self.pause_bar_threshold = 0.3
        self.pause_bar_col = f"is_pause_{self.pause_bar_threshold:.2f}_{self.atr_col}"
        self.rsi_breakout_used = False
        indicators =IndicatorPipeline([(ATRIndicator(period=self.atr_period), None, None),
                                       (WMAIndicator(period=self.medium_wma_period), None, None)])
        indicators_long_context = IndicatorPipeline([(RSIIndicator(period=self.rsi_period), None, None),
                                                     (ATRIndicator(period=self.atr_period), None, None),
                                                     (WMAIndicator(period=self.medium_wma_period), None, None),
                                                     (WMAIndicator(period=self.short_wma_period), None, None),
                                                     (PauseBarIndicator(atr_threshold=self.pause_bar_threshold,
                                                                        atr_column_name=self.atr_col), None, None)])
        kwargs["indicator_pipeline"] = {"window": indicators,
                                        "context": {self.long_context: indicators_long_context}}
        self.rsi_upper_threshold = 67
        self.rsi_lower_threshold = 33
        kwargs["plottables"] = {"indicator_fields": [
                                                    #{"field": f"{self.short_wma_col}_up_support", "panel": 2},
                                                    #{"field": self.atr_col, "panel": 4,
                                                    # "context": self.long_context},
                                                    {"field": self.rsi_col, "panel": 3,
                                                    "context": self.long_context,
                                                    "fill_region": [self.rsi_lower_threshold,
                                                                    self.rsi_upper_threshold]},
                                                    {"field": self.pause_bar_col,
                                                    "panel": 5,
                                                    "context": self.long_context},
                                                    self.medium_wma_col,
                                                    {"field": self.medium_wma_col, "panel": 0, "context": self.long_context},
                                                    {"field": self.short_wma_col, "panel": 0, "context": self.long_context},
                                                    ]}
        kwargs["plot_context_candles"] = [self.long_context]
        non_trading_timeslots = []
        non_trading_timeslots.extend(Strategy.NON_TRADING_AFTERNOON)
        kwargs["non_trading_timeslots"] = non_trading_timeslots

        kwargs["context_required"] = [self.long_context]
    
        kwargs["intraday_squareoff"] = True
        kwargs["squareoff_hour"] = 15
        kwargs["squareoff_minute"] = 10

        self.atr_factor = 0.09

        self.sl_atr_factor = 0.5

        self.target_points = 40

        super().__init__(*args, **kwargs)
        self.reset_state_machine()


    def get_entry(self,
                  candle: pd.Series,
                  trade_type: TradeType):
        trigger_price, limit_price = None, None
        if trade_type == TradeType.LONG:
            trigger_price = candle["high"] + candle[self.atr_col] * self.atr_factor
            limit_price = candle["high"] + candle[self.atr_col] * 2 * self.atr_factor
        else:
            trigger_price = candle["low"] - candle[self.atr_col] * self.atr_factor
            limit_price = candle["low"] - candle[self.atr_col] * 2 * self.atr_factor
        return trigger_price, limit_price

    def get_target(self,
                   candle: pd.Series,
                   trade_type: TradeType):
        
        if trade_type == TradeType.LONG:
            return candle["high"] + self.target_points
        else:
            return candle["low"] - self.target_points

    def get_stoploss(self,
                     candle: pd.Series,
                     long_context: pd.Series,
                     trade_type: TradeType):
        trigger_price, limit_price = None, None
        if trade_type == TradeType.LONG:
            #trigger_price = candle["low"] - candle[self.atr_col] * self.sl_atr_factor
            #limit_price = candle["low"] - candle[self.atr_col] * 2 * self.sl_atr_factor
            
            #trigger_price = long_context[self.medium_wma_col] - candle[self.atr_col] * self.sl_atr_factor
            #limit_price = long_context[self.medium_wma_col] - candle[self.atr_col] * 2 * self.sl_atr_factor

            trigger_price = candle["low"] - 20
            limit_price = candle["low"] - 25
        else:
            trigger_price = candle["high"] + candle[self.atr_col] * self.sl_atr_factor
            limit_price = candle["high"] + candle[self.atr_col] * 2 * self.sl_atr_factor
        return trigger_price, limit_price

    def get_fixed_stoploss(self,
                           candle: pd.Series,
                           trade_type: TradeType):
        trigger_price, limit_price = None, None
        if trade_type == TradeType.LONG:
            #trigger_price = candle["low"] - candle[self.atr_col] * 2 * self.sl_atr_factor
            #limit_price = candle["low"] - candle[self.atr_col] * 4 * self.sl_atr_factor
            trigger_price = candle["low"] - 20
            limit_price = candle["low"] - 25
        else:
            trigger_price = candle["high"] + candle[self.atr_col] * 2 * self.sl_atr_factor
            limit_price = candle["high"] + candle[self.atr_col] * 4 * self.sl_atr_factor
        return trigger_price, limit_price

    
    def make_entry(self,
                broker: Broker,
                next_run: TradeType,
                window: pd.DataFrame,
                context: dict[str, pd.DataFrame],
                scrip: str,
                exchange: str,
                quantity: int):
        trigger_price, limit_price = self.get_entry(window, context, next_run)
        entry_order = self.take_position(scrip=scrip,
                                        exchange=exchange,
                                        broker=broker,
                                        position_type=PositionType.ENTRY,
                                        trade_type=next_run,
                                        trigger_price=trigger_price,
                                        limit_price=limit_price,
                                        quantity=quantity,
                                        product=self.product)

        if entry_order is None:
            print("Placing order failed. "
                    "skipping gtts; this happens if price"
                    " movement is too fast.")
        else:
            trigger_price, limit_price = self.get_stoploss(window, context, next_run)
            self.take_position(scrip=scrip,
                            exchange=exchange,
                            broker=broker,
                            position_type=PositionType.STOPLOSS,
                            trade_type=next_run,
                            trigger_price=trigger_price,
                            limit_price=limit_price,
                            quantity=quantity,
                            product=self.product,
                            parent_order=entry_order)
            self.take_position(scrip=scrip,
                            exchange=exchange,
                            broker=broker,
                            position_type=PositionType.TARGET,
                            trade_type=next_run,
                            limit_price=self.get_target(window, context, next_run),
                            quantity=quantity,
                            product=self.product,
                            parent_order=entry_order)
            return True
        return False

    def perform_action(self,
                        broker: Broker,
                        sm: RSIPullBackStateMachine,
                        orders: dict[str, Order],
                        context: dict[str, any],
                        action: RSIPullBackStateMachine.Action):
        self.logger.info(f"Performing action {action}")

        if action == RSIPullBackStateMachine.Action.CreateEntryWithFixetSL:
            qty = max(self.max_budget // sm.state.entry_candle["close"],
                      self.min_quantity)
            trigger_price, limit_price = self.get_entry(sm.state.entry_candle,
                                                        sm.state.expected_run)
            entry_order = self.take_position(scrip=sm.scrip,
                                             exchange=sm.exchange,
                                             broker=broker,
                                             position_type=PositionType.ENTRY,
                                             trade_type=sm.state.expected_run,
                                             trigger_price=trigger_price,
                                             limit_price=limit_price,
                                             quantity=qty,
                                             product=self.product)

            if entry_order is None:
                print("Placing order failed. "
                        "skipping gtts; this happens if price"
                        " movement is too fast.")
            
            else:
                trigger_price, limit_price = self.get_fixed_stoploss(sm.state.entry_candle,
                                                                     sm.state.expected_run)
                self.take_position(scrip=sm.scrip,
                                   exchange=sm.exchange,
                                   broker=broker,
                                   position_type=PositionType.STOPLOSS,
                                   trade_type=sm.state.expected_run,
                                   trigger_price=trigger_price,
                                   limit_price=limit_price,
                                   quantity=qty,
                                   product=self.product,
                                   parent_order=entry_order)
                self.take_position(scrip=sm.scrip,
                                   exchange=sm.exchange,
                                   broker=broker,
                                   position_type=PositionType.TARGET,
                                   trade_type=sm.state.expected_run,
                                   limit_price=self.get_target(sm.state.entry_candle,
                                                               sm.state.expected_run),
                                   quantity=qty,
                                   product=self.product,
                                   parent_order=entry_order)
        elif action == RSIPullBackStateMachine.Action.UpdateStoploss:
            trigger_price, limit_price = self.get_fixed_stoploss(sm.state.stoploss_candle,
                                                                 sm.state.expected_run)
            if orders["stoploss"] is None:
                self.logger.info("Looks like stoploss order is still a GTT. Searching for it...")
                gttorders = broker.get_gtt_orders_for(orders["entry"])
                for order in gttorders:
                    if "stoploss" in order.tags:
                        self.logger.info(f"Found stoploss gtt order {order.order_id}. Updating it.")
                        order.limit_price = limit_price
                        order.trigger_price = trigger_price
                        broker.update_gtt_order(orders["entry"], order)
            else:
                self.logger.info("Looks like stoploss is placed; Updating order..")
                orders["stoploss"].limit_price = limit_price
                orders["stoploss"].trigger_price = trigger_price
                broker.update_order(orders["stoploss"],
                                    refresh_cache=True,
                                    local_update=False)

        elif action == RSIPullBackStateMachine.Action.CancelPosition:
            quantity = self.cancel_active_orders(broker,
                                                 scrip=sm.scrip,
                                                 exchange=sm.exchange,
                                                 product=self.product)
            if quantity > 0:
                self.logger.warn(f"Looks like entry has fructified; performing squareoff")
                self.perform_squareoff(broker=broker,
                                       scrip=sm.scrip,
                                       exchange=sm.exchange,
                                       product=self.product,
                                       quantity=quantity)
        elif action == RSIPullBackStateMachine.Action.AverageOutTrade:
            return
            qty = max(self.max_budget // sm.state.entry_candle["close"],
                        self.min_quantity)
            trigger_price, limit_price = self.get_entry(sm.state.entry_candle,
                                                        sm.state.expected_run)

            entry_order = self.take_position(scrip=sm.scrip,
                                             exchange=sm.exchange,
                                             broker=broker,
                                             position_type=PositionType.ENTRY,
                                             trade_type=sm.state.expected_run,
                                             trigger_price=trigger_price,
                                             limit_price=limit_price,
                                             entry_with_limit=True,
                                             quantity=qty,
                                             product=self.product)
            orders["target"].quantity += qty
            orders["target"].limit_price = entry_order.limit_price + (orders["target"].limit_price - entry_order.limit_price) / 1.2
            orders["target"].trigger_price = entry_order.trigger_price + (orders["target"].trigger_price - entry_order.trigger_price) / 1.2
            broker.update_order(order=orders["target"])
            orders["stoploss"].quantity += qty
            trigger_price, limit_price = self.get_stoploss(candle=sm.state.stoploss_candle,
                                                           long_context=sm.state.stoploss_long_context,
                                                           trade_type=sm.state.expected_run)
            orders["stoploss"].limit_price = limit_price
            orders["target"].trigger_price = trigger_price
            broker.update_order(order=orders["stoploss"])


    def reset_state_machine(self,
                            scrip: str = None,
                            exchange: str = None) -> None:
        if scrip is None and exchange is None:
            self.logger.warn(f"Resetting all states as scrip and exchange were none.")
            self.state_machines = {}
            return
        k = get_key_from_scrip_and_exchange(scrip, exchange)
        self.state_machines[k] = RSIPullBackStateMachine(scrip=scrip,
                                                        exchange=exchange)

    def get_state_machine(self, scrip: str,
                        exchange: str) -> RSIPullBackStateMachine:
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
        if (context[self.long_context].iloc[-1][self.rsi_col] > self.rsi_lower_threshold
            and context[self.long_context].iloc[-1][self.rsi_col] < self.rsi_upper_threshold):
            self.rsi_breakout_used = False

        if self.can_trade(window, context):
            
            
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
            
            orders = {"entry": current_entry_order,
                      "target": current_target_order,
                      "stoploss": current_stoploss_order}


            state_context = {"rsi": context[self.long_context].iloc[-1][self.rsi_col],
                             "new_context_bar": window.iloc[-1].name - context[self.long_context].iloc[-1].name == datetime.timedelta(seconds=pd.Timedelta(self.long_context).total_seconds()),
                             "rsi_upper_threshold": self.rsi_upper_threshold,
                             "is_pause_bar": context[self.long_context].iloc[-1][self.pause_bar_col] == 1.0}
            
            if current_entry_order is not None:
                self.logger.info(f"Entry: {current_entry_order.transaction_type} / "
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
            

            for k, v in state_context.items():
                self.logger.info(f"Context {k}     :    {v}")

            for k, v in sm.as_dict().items():
                if isinstance(v, pd.Series):
                    v = f"O {v['open']} H {v['high']} L {v['low']} C {v['close']}"
                self.logger.info(f"Before Action {k}    : {v}")
            action = sm.run(strategy=self,
                            candle=window.iloc[-1],
                            long_context=context[self.long_context].iloc[-1],
                            orders=orders,
                            context=state_context)    
            self.perform_action(sm=sm,
                                action=action,
                                broker=broker,
                                orders=orders,
                                context=state_context)
            for k, v in sm.as_dict().items():
                if isinstance(v, pd.Series):
                    v = f"O {v['open']} H {v['high']} L {v['low']} C {v['close']}"
                self.logger.info(f"After Action {k}    : {v}")
