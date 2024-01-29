from typing import Optional
import datetime
import pandas as pd
import numpy as np
from ..core.ds import (TradeType, OrderState, PositionType, TradingProduct)

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
from ..core.util import new_id


class Strategy1(Strategy):

    def __init__(self,
                 *args,
                 st_period: int = 14,
                 st_multiplier: float = 3.0,
                 rsi_period: int = 14,
                 atr_period: int = 14,
                 short_wma_period: int = 9,
                 medium_wma_period: int = 15,
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
        self.pause_bar_threshold = 0.4
        self.pause_bar_col = f"is_pause_{self.pause_bar_threshold:.2f}_{self.atr_col}"
        self.rsi_breakout_used = False
        indicators =IndicatorPipeline([(WMAIndicator(period=self.short_wma_period), None, None),
                                       (SupertrendIndicator(period=self.st_period,
                                                      multiplier=self.st_multiplier), None, None)])
        indicators_long_context = IndicatorPipeline([(RSIIndicator(period=self.rsi_period), None, None),
                                                     (ATRIndicator(period=self.atr_period), None, None),
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
                                                     self.short_wma_col,
                                                     self.st_col
                                                     # self.medium_wma_col,
                                                     ]}
        kwargs["plot_context_candles"] = [self.long_context]
        non_trading_timeslots = []
        non_trading_timeslots.extend([{"from": {"hour": 9,
                                                "minute": 0},
                                       "to": {"hour": 9,
                                              "minute": 30}}])
        non_trading_timeslots.extend(Strategy.NON_TRADING_AFTERNOON)
        kwargs["non_trading_timeslots"] = non_trading_timeslots

        kwargs["context_required"] = [self.long_context]
       
        kwargs["intraday_squareoff"] = True


        self.atr_factor = 0.05

        self.sl_atr_factor = 0.5

        self.risk_ratio = 2

        print(args, kwargs)
        super().__init__(*args, **kwargs)

    def get_entry(self, window: pd.DataFrame,
                  context: dict[str, pd.DataFrame], 
                  trade_type: TradeType):
        trigger_price, limit_price = None, None
        if trade_type == TradeType.LONG:
            trigger_price = context[self.long_context].iloc[-1]["high"] + context[self.long_context].iloc[-1][self.atr_col] * self.atr_factor
            limit_price = context[self.long_context].iloc[-1]["high"] + context[self.long_context].iloc[-1][self.atr_col] * 2 * self.atr_factor
        else:
            trigger_price = context[self.long_context].iloc[-1]["low"] - context[self.long_context].iloc[-1][self.atr_col] * self.atr_factor
            limit_price = context[self.long_context].iloc[-1]["low"] - context[self.long_context].iloc[-1][self.atr_col] * 2 * self.atr_factor
        return trigger_price, limit_price

    def get_target(self, window: pd.DataFrame, context: dict[str, pd.DataFrame], trade_type: TradeType):
        
        if trade_type == TradeType.LONG:
            return context[self.long_context].iloc[-1]["high"] + self.risk_ratio * abs(self.get_entry(window, context, trade_type)[1] - self.get_stoploss(window, context, trade_type)[1])
            #return context[self.long_context].iloc[-1]["high"] + self.risk_ratio * abs(context[self.long_context].iloc[-1]["close"] - context[self.long_context].iloc[-1]["open"])
        else:
            return context[self.long_context].iloc[-1]["low"] - self.risk_ratio * abs(self.get_stoploss(window, context, trade_type)[1] - self.get_entry(window, context, trade_type)[1])
            #return context[self.long_context].iloc[-1]["low"] - self.risk_ratio * abs(context[self.long_context].iloc[-1]["close"] - context[self.long_context].iloc[-1]["open"])

    def get_stoploss(self, window: pd.DataFrame,
                     context: dict[str, pd.DataFrame],
                     trade_type: TradeType):
        trigger_price, limit_price = None, None
        if trade_type == TradeType.LONG:
            trigger_price = context[self.long_context].iloc[-1]["low"] - context[self.long_context].iloc[-1][self.atr_col] * self.sl_atr_factor
            limit_price = context[self.long_context].iloc[-1]["low"] - context[self.long_context].iloc[-1][self.atr_col] * 2 * self.sl_atr_factor
        else:
            trigger_price = context[self.long_context].iloc[-1]["high"] + context[self.long_context].iloc[-1][self.atr_col] * self.sl_atr_factor
            limit_price = context[self.long_context].iloc[-1]["high"] + context[self.long_context].iloc[-1][self.atr_col] * 2 * self.sl_atr_factor
        return trigger_price, limit_price

    def is_green_candle(self, row):
        return row["open"] > row["close"]
    
    def make_entry(self, broker: Broker, next_run: TradeType,
                   window: pd.DataFrame, context: dict[str, pd.DataFrame],
                   scrip: str, exchange: str, quantity: int):
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
            
            current_run = self.get_current_run(broker=broker,
                                               scrip=scrip,
                                               exchange=exchange)

            qty = max(self.max_budget // context[self.long_context].iloc[-1]["close"], self.min_quantity)
            group_id = new_id()

            long_context_rsi = context[self.long_context].iloc[-1][self.rsi_col]
            entry_tdiff = None
            
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

            

            if current_entry_order is not None:
                print(current_entry_order)
                entry_tdiff = context[self.long_context].iloc[-1].name - current_entry_order.timestamp

            self.logger.info(f"Current Run: {current_run}; "
                             f"RSI in {self.long_context}: {long_context_rsi}"
                             f"current_entry_order: {current_entry_order}; "
                             f"current_target_order: {current_target_order}; "
                             f"current_stoploss_order: {current_stoploss_order}; "
                             f"Time diff between entry and now : {entry_tdiff}")

            max_breach_timedelta = datetime.timedelta(hours=1)
            
            if (current_entry_order is None
                and current_target_order is None
                and current_stoploss_order is None
                and not self.rsi_breakout_used):
                self.logger.info("Case-A: Clean slate. See if we can place an entry.")
                next_run = None
                if context[self.long_context].iloc[-1][self.pause_bar_col] == 1.0:
                    self.logger.info("Pause bar found.")
                    if long_context_rsi > self.rsi_upper_threshold:
                        self.logger.info("Found long RSI breach.")
                        next_run = TradeType.LONG
                    elif long_context_rsi < self.rsi_lower_threshold:
                        self.logger.info("Found short RSI breach.")
                        next_run = TradeType.SHORT

                if next_run is not None:

                    result = self.make_entry(broker=broker,
                                             next_run=next_run,
                                             window=window,
                                             context=context,
                                             scrip=scrip,
                                             exchange=exchange,
                                             quantity=qty)
                    if result:
                        self.rsi_breakout_used = True
            elif (current_entry_order is None 
                  and current_stoploss_order is not None
                  or current_target_order is not None):
                self.logger.info("Case-B: Order already in progress..")
                """
                cancel_order = False
                if current_run == TradeType.LONG:
                    if (self.is_green_candle(context[self.long_context].iloc[-1])
                        and context[self.long_context].iloc[-1]["open"] < context[self.long_context].iloc[-1][self.medium_wma_col]):
                        self.logger.info("Green candle's open below WMA")
                        cancel_order = True
                    if (not self.is_green_candle(context[self.long_context].iloc[-1])
                        and context[self.long_context].iloc[-1]["close"] < context[self.long_context].iloc[-1][self.medium_wma_col]):
                        self.logger.info("Red candle's close below WMA")
                        cancel_order = True
                else:
                    if (self.is_green_candle(context[self.long_context].iloc[-1])
                        and context[self.long_context].iloc[-1]["close"] > context[self.long_context].iloc[-1][self.medium_wma_col]):
                        self.logger.info("Green candle's open above WMA")
                        cancel_order = True
                    if (not self.is_green_candle(context[self.long_context].iloc[-1])
                        and context[self.long_context].iloc[-1]["close"] > context[self.long_context].iloc[-1][self.medium_wma_col]):
                        self.logger.info("Red candle's close above WMA")
                        cancel_order = True

                if cancel_order:
                    quantity = self.cancel_active_orders(broker=broker,
                                                         scrip=scrip,
                                                         exchange=exchange)
                    self.perform_squareoff(broker=broker,
                                           scrip=scrip,
                                           exchange=exchange,
                                           product=self.product,
                                           quantity=quantity)
                    return
                """
            elif (current_entry_order is not None
                  and current_stoploss_order is None
                  and current_target_order is None):
                self.logger.info("Case-C: Entry exists; it hasn't fructified yet."
                                 "So update/cancel, if required.")
                
                cancel_order = False
                if ((long_context_rsi <= self.rsi_upper_threshold
                     and current_run == TradeType.LONG)
                     or (long_context_rsi <= self.rsi_lower_threshold
                         and current_run == TradeType.SHORT)):
                    cancel_order = True
                    self.logger.info("RSI has weakened. Cancelling entry.")
                
                if current_run == TradeType.LONG:
                    if (self.is_green_candle(context[self.long_context].iloc[-1])
                        and context[self.long_context].iloc[-1]["open"] < context[self.long_context].iloc[-1][self.short_wma_col]):
                        self.logger.info("Green candle's open below WMA")
                        cancel_order = True
                    if (not self.is_green_candle(context[self.long_context].iloc[-1])
                        and context[self.long_context].iloc[-1]["close"] < context[self.long_context].iloc[-1][self.short_wma_col]):
                        self.logger.info("Red candle's close below WMA")
                        cancel_order = True
                else:
                    if (self.is_green_candle(context[self.long_context].iloc[-1])
                        and context[self.long_context].iloc[-1]["close"] > context[self.long_context].iloc[-1][self.short_wma_col]):
                        self.logger.info("Green candle's open above WMA")
                        cancel_order = True
                    if (not self.is_green_candle(context[self.long_context].iloc[-1])
                        and context[self.long_context].iloc[-1]["close"] > context[self.long_context].iloc[-1][self.short_wma_col]):
                        self.logger.info("Red candle's close above WMA")
                        cancel_order = True

                if cancel_order:
                    self.cancel_active_orders(broker=broker,
                                              scrip=scrip,
                                              exchange=exchange)
                    return

                if context[self.long_context].iloc[-1][self.pause_bar_col] == 1.0:
                    new_trigger_price, _ = self.get_entry(window, context, current_run)
                    if ((current_run == TradeType.LONG
                        and new_trigger_price < current_entry_order.trigger_price)
                        or (current_run == TradeType.SHORT
                            and new_trigger_price > current_entry_order.trigger_price)):
                            self.logger.info("Current pause bar makes a lower high/low; so updating entry.")
                            broker.cancel_order(current_entry_order,
                                                refresh_cache=True)
                            self.make_entry(broker=broker,
                                            next_run=current_run,
                                            window=window,
                                            context=context,
                                            scrip=scrip,
                                            exchange=exchange,
                                            quantity=qty)
                else:
                    self.logger.info("Non-pause bar found. Exiting trade...")
                    self.cancel_active_orders(broker=broker,
                                              scrip=scrip,
                                              exchange=exchange)
