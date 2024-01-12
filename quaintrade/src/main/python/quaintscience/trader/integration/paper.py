import datetime
from typing import Optional
from dataclasses import dataclass

import pandas as pd
from tabulate import tabulate

from ..core.ds import (Order,
                       Position,
                       OrderType,
                       OHLCStorageType,
                       OrderState, TradingProduct,
                       TransactionType)
from ..core.roles import Broker, HistoricDataProvider
from ..core.util import (default_dataclass_field,
                         get_key_from_scrip_and_exchange,
                         get_scrip_and_exchange_from_key)



def nse_commission_func(order: Order, brokerage_percentage: float = 0.03, max_commission: float = 20):
    charges = 0.
    if max_commission > 0:
        brokerage = min((brokerage_percentage / 100) * order.price * order.quantity, max_commission)
    else:
        brokerage = (brokerage_percentage / 100) * order.price * order.quantity
    stt = 0.
    if order.product == TradingProduct.MIS:
        if order.transaction_type == TransactionType.SELL:
            stt = (0.025 / 100) * order.price * order.quantity # STT
    else:
        stt = (0.1 / 100) * order.price * order.quantity # STT
    transaction_charges = (0.00325 / 100) * order.price * order.quantity # Transaction charges NSE
    sebi_charges = (order.price * order.quantity / 10000000) * 10
    stamp_charges = 0.
    if order.transaction_type == TransactionType.BUY:
        stamp_charges = (0.015 / 100) * (order.price * order.quantity / 10000000)
    gst = (18 / 100) * (brokerage + sebi_charges + transaction_charges)
    
    brokerage = round(brokerage, 2)
    stt = round(stt, 2)
    transaction_charges = round(transaction_charges, 2)
    sebi_charges = round(sebi_charges, 2)
    stamp_charges = round(stamp_charges, 2)
    gst = round(gst, 2)
    
    total = round(brokerage + stt + transaction_charges + sebi_charges + stamp_charges + gst, 2)
    print(f"Brokerage: {brokerage} "
          f"| STT: {stt} "
          f"| TransactionCharges: {transaction_charges} "
          f"| SEBICharges: {sebi_charges} "
          f"| Stamp: {stamp_charges} "
          f"| GST: {gst} "
          f"| Total : {total}")
    return total


@dataclass(kw_only=True)
class PaperPosition(Position):
    quantity_and_price_history: list[(float, float)] = default_dataclass_field([])

    def __hash__(self):
        return hash(",".join([self.scrip, self.exchange, self.product.value]))

    def __eq__(self, other: object):
        if not isinstance(other, Position):
            return False
        if (self.scrip == other.scrip
            and self.exchange == other.exchange
            and self.product == other.product):
            return True
        return False


class PaperBroker(Broker):

    ProviderName = "paper"

    def __init__(self,
                 *args,
                 data_provider: HistoricDataProvider,
                 instruments: list = None,
                 historic_context_from: datetime.datetime = None,
                 historic_context_to: datetime.datetime = None,
                 interval: str = "10min",
                 refresh_orders_immediately_on_gtt_state_change: bool = False,
                 refresh_data_on_every_time_change: bool = False,
                 commission_func: Optional[callable] = None,
                 **kwargs):

        self.data_provider = data_provider

        if instruments is None:
            instruments = []

        self.instruments = instruments

        if historic_context_from is None:
            historic_context_from = datetime.datetime.now() - datetime.timedelta(days=60)
        if historic_context_to is None:
            historic_context_to = datetime.datetime.now()
        
        self.historic_context_from = historic_context_from
        self.historic_context_to = historic_context_to
        
        self.interval = interval
        self.refresh_orders_immediately_on_gtt_state_change = refresh_orders_immediately_on_gtt_state_change

        self.refresh_data_on_every_time_change = refresh_data_on_every_time_change
        if commission_func is None:
            commission_func = nse_commission_func
        self.commission_func = commission_func
        self.orders = []
        self.positions = {}
        self.gtt_orders = []

        self.data = {}
        self.idx = {}

        self.current_time = None

        self.events = []

        self.pnl_history = []

        self.order_stats = {"completed": 0,
                            "cancelled": 0,
                            "pending": 0}

        super().__init__(*args, **kwargs)

    def init(self):
        del self.data
        self.data = {}
        for instrument in self.instruments:
            self.logger.info(f"Paper Trader: Loading data for {instrument}")
            scrip = instrument["scrip"]
            exchange = instrument["exchange"]
            instrument_data = self.data_provider.get_data_as_df(scrip=scrip,
                                                                exchange=exchange,
                                                                interval=self.interval,
                                                                from_date=self.historic_context_from,
                                                                to_date=self.historic_context_to,
                                                                storage_type=OHLCStorageType.PERM,
                                                                download_missing_data=False)
            self.data[get_key_from_scrip_and_exchange(scrip, exchange)] = instrument_data

    def current_datetime(self):
        return self.current_time

    def set_current_time(self, dt: datetime.datetime,
                         traverse: bool = False):

        if self.refresh_data_on_every_time_change:
            self.init()

        for instrument in self.data.keys():
            to_idx = self.data[instrument].index.get_indexer([dt], method="nearest")[0]
            if self.data[instrument].iloc[to_idx].name < dt:
                to_idx += 1

            if to_idx >= len(self.data[instrument]):
                raise ValueError("Time exceeds last item in data for {instrument}")

            if not traverse:
                self.current_time = dt
                self.idx[instrument] = to_idx

            for idx in range(self.idx.get(instrument, 0) + 1, to_idx + 1):
                scrip, exchange = get_scrip_and_exchange_from_key(instrument)
                self.idx[instrument] = idx
                self.logger.debug(f"INC TIME {self.current_time} >>>> {self.data[instrument].iloc[idx].name} FOR {instrument} [idx={idx}]")
                self.current_time = self.data[instrument].iloc[idx].name
                self.logger.debug(f"{self.current_time} >>>> "
                                  f" O {self.data[instrument].iloc[idx]['open']}"
                                  f" H {self.data[instrument].iloc[idx]['high']}"
                                  f" L {self.data[instrument].iloc[idx]['low']}"
                                  f" C {self.data[instrument].iloc[idx]['close']}")


                self.__process_orders(scrip=scrip,
                                      exchange=exchange)

                self.__update_positions()

                self.__process_orders(scrip=scrip,
                                      exchange=exchange)

                self.__update_positions()

    def get_orders_as_table(self):
        status = [[self.current_time,
                   self.order_stats["pending"],
                   self.order_stats["completed"],
                   self.order_stats["cancelled"]]]
        print(tabulate(status, headers=["Time", "Pending", "Completed", "Cancelled"], tablefmt="double_outline"))
        
        printable_orders = []
        for order in self.orders:
            if order.state == OrderState.PENDING:
                printable_orders.append(["R",
                                         order.order_id[:4],
                                         order.parent_order_id[:4] if order.parent_order_id is not None else "",
                                         order.scrip,
                                         order.exchange,
                                         order.transaction_type,
                                         order.quantity,
                                         order.order_type,
                                         order.limit_price,
                                         ", ".join(order.tags)])
        for from_order, to_order in self.gtt_orders:
            printable_orders.append(["GTT",
                                     to_order.order_id[:4],
                                     from_order.order_id[:4],
                                     to_order.scrip,
                                     to_order.exchange,
                                     to_order.transaction_type,
                                     to_order.quantity,
                                     to_order.order_type,
                                     to_order.limit_price,
                                     ", ".join(to_order.tags)])
        headers = ["Typ", "id", "parent", "scrip", "exchange",
                   "buy/sell", "qty", "order_type", "limit_price", "reason"]
        print(tabulate(printable_orders, headers=headers, tablefmt="double_outline"))
        return printable_orders, headers

    def __update_positions(self):
        for _, position in self.get_positions().items():
            money_spent = position.stats.get("money_spent", 0.)
            cash_flow = position.stats.get("cash_flow", 0.)
            net_quantity = position.stats.get("net_quantity", 0.)

            position.average_price =  (abs(money_spent) / abs(net_quantity)) if abs(net_quantity) > 0 else 0
            key = get_key_from_scrip_and_exchange(position.scrip, position.exchange)
            #print(cash_flow, position.quantity_and_price_history)
            position.pnl = cash_flow + (net_quantity * self.data[key].iloc[self.idx[key]]["close"]) - position.charges

            storage = self.get_tradebook_storage()
            storage.store_position_state(strategy=self.strategy,
                                         run_name=self.run_name,
                                         date=self.current_datetime(),
                                         position=position)

    def __update_position_stats(self, position, price, quantity, charges, transaction_type):
        if transaction_type == TransactionType.SELL:
            quantity = -quantity
        money_spent = position.stats.get("money_spent", 0.)
        cash_flow = position.stats.get("cash_flow", 0.)
        net_quantity = position.stats.get("net_quantity", 0.)
        
        money_spent += quantity * price
        cash_flow += (-quantity * price)
        net_quantity += quantity
        
        position.stats["money_spent"] = money_spent
        position.stats["cash_flow"] = cash_flow
        position.stats["net_quantity"] = net_quantity

        position.average_price =  (abs(money_spent) / abs(net_quantity)) if abs(net_quantity) > 0 else 0
        
        key = get_key_from_scrip_and_exchange(position.scrip, position.exchange)
        #print(cash_flow, position.quantity_and_price_history)
        position.charges += charges
        position.pnl = cash_flow + (net_quantity * self.data[key].iloc[self.idx[key]]["close"]) - position.charges

    def get_positions_as_table(self):
        # self.logger.debug(f"{self.current_time} entered __refresh_positions")
        #self.__update_positions()
        printable_positions = []
        # print(self.positions)
        for _, position in self.get_positions().items():
            key = get_key_from_scrip_and_exchange(position.scrip, position.exchange)
            printable_positions.append([self.current_time,
                                        position.scrip,
                                        position.exchange,
                                        position.stats["net_quantity"],
                                        position.average_price,
                                        self.data[key].iloc[self.idx[key]]["close"],
                                        position.pnl,
                                        position.charges])
            self.logger.info(f"{self.current_time} "
                             f"Position: {position.scrip}/{position.exchange} | {position.stats['net_quantity']} | {position.pnl:.2f}")
        headers = ["time", "scrip", "exchange", "qty", "avgP", "LTP", "PnL", "Comm"]
        print(tabulate(printable_positions, headers=headers, tablefmt="double_outline"))
        return printable_positions, headers

    def __add_position(self,
                       order: Order,
                       last_price: float,
                       price: Optional[float] = None):
        order.state = OrderState.COMPLETED
        self.order_stats["completed"] += 1
        if price is None:
            price = order.limit_price
        order.price = price
        
        self.logger.info(f"Order {order.transaction_type.value} {order.order_id[:4]}/{order.scrip}/"
                         f"{order.exchange}/{order.order_type.value} [tags={order.tags}] @ {order.price} x {order.quantity} executed.")
        """
        self.events.append([self.current_time,
                            {"scrip": order.scrip,
                             "exchange": order.exchange,
                             "transaction_type": order.transaction_type,
                             "quantity": order.quantity,
                             "price": price,
                             "event_type": ",".join(order.tags)}])
        """
        storage = self.get_tradebook_storage()
        storage.store_event(strategy=self.strategy,
                            run_name=self.run_name,
                            scrip=order.scrip,
                            exchange=order.exchange,
                            date=self.current_datetime(),
                            quantity=order.quantity,
                            price=price,
                            event_type=",".join(order.tags),
                            transaction_type=order.transaction_type)

        # self.logger.info(self.events)
        if self.commission_func is not None:
            charges = self.commission_func(order)
        position = PaperPosition(timestamp=self.current_time,
                                 scrip_id=order.scrip_id,
                                 scrip=order.scrip,
                                 exchange_id=order.exchange_id,
                                 exchange=order.exchange,
                                 product=order.product,
                                 average_price=price,
                                 last_price=last_price)

        # Position generates a hash if exchange, scrip, and product are the same
        # So updating position to all the latest values if it already exists..
        # print(self.positions)
        self.positions[position] = self.positions.get(position, position)
        position = self.positions[position]
        # print(position)
        if order.transaction_type == TransactionType.SELL:
            position.quantity -= order.quantity
        else:
            position.quantity += order.quantity
        self.__update_position_stats(position, price, order.quantity, charges, order.transaction_type)

    def __process_orders(self,
                         scrip=None,
                         exchange=None,
                         inside_a_recursion=False):
        # self.logger.debug(f"{self.current_time} entered __process_orders scrip={scrip} exchange={exchange}")
        for order in self.orders:
            key = get_key_from_scrip_and_exchange(order.scrip, order.exchange)
            order_scrip, order_exchange = get_scrip_and_exchange_from_key(key)
            if scrip is not None and exchange is not None:
                if order_scrip != scrip or order_exchange != exchange:
                    continue
            candle = self.data[key].iloc[self.idx[key]]
            self.order_stats["pending"] = 0
            if order.state == OrderState.PENDING:
                change = False
                if order.order_type in [OrderType.SL_LIMIT, OrderType.SL_MARKET]:
                    if order.transaction_type == TransactionType.BUY:
                        if candle["high"] >= order.trigger_price:
                            if order.order_type == OrderType.SL_LIMIT:
                                order.order_type = OrderType.LIMIT
                            else:
                                order.order_type = OrderType.MARKET
                    else:
                        # print(candle["low"], order.trigger_price, order.limit_price)
                        if candle["low"] <= order.trigger_price:
                            if order.order_type == OrderType.SL_LIMIT:
                                self.logger.info(f"{order.order_id[:4]} Became LIMIT FROM SL_LIMIT")
                                change = True
                                order.order_type = OrderType.LIMIT
                            else:
                                order.order_type = OrderType.MARKET

                if order.order_type == OrderType.LIMIT:
                    if order.transaction_type == TransactionType.BUY:
                        if candle["low"] <= order.limit_price:
                            self.__add_position(order,
                                                last_price=candle["close"],
                                                price=min(order.limit_price,
                                                          candle["high"]))
                        self.cancel_invalid_child_orders()
                    elif order.transaction_type == TransactionType.SELL:
                        #if change:
                        #    print(order, candle)
                        if candle["high"] >= order.limit_price:
                             self.__add_position(order,
                                                 last_price=candle["close"],
                                                 price=max(order.limit_price,
                                                           candle["low"]))
                        self.cancel_invalid_child_orders()
                elif order.order_type == OrderType.MARKET:
                    self.__add_position(order, last_price=candle["close"], price=candle["close"])
                    self.cancel_invalid_child_orders()

                if order.state == OrderState.COMPLETED:
                    self.cancel_invalid_child_orders()
                    self.cancel_invalid_group_orders()
                else:
                    self.order_stats["pending"] += 1
        new_gtt_orders = []
        gtt_state_changed = False
        for ii, (entry_order, other_order) in enumerate(self.gtt_orders):
            if scrip is not None and exchange is not None:
                if entry_order.scrip != scrip or entry_order.exchange != exchange:
                    continue
            if entry_order.state == OrderState.COMPLETED:
                self.place_order(other_order)
                # print(other_order)
                gtt_state_changed = True
                continue
            new_gtt_orders.append((entry_order, other_order))
        self.gtt_orders = new_gtt_orders

        if gtt_state_changed and self.refresh_orders_immediately_on_gtt_state_change:
            self.logger.info(f"{self.current_time} gtt_state_changed")
            self.__process_orders(scrip=scrip,
                                  exchange=exchange,
                                  inside_a_recursion=True)

        for position in self.positions:
            key = get_key_from_scrip_and_exchange(position.scrip, position.exchange)
            position.last_price = candle = self.data[key].iloc[self.idx[key]]["close"]
            position.pnl = (position.last_price - position.average_price) * position.quantity
        self.order_callback(self.orders)
        orders = []
        for order in self.orders:
            if order.state == OrderState.COMPLETED:
                continue
            orders.append(order)
        self.orders = orders

    def order_callback(self, orders):
        self.cancel_invalid_child_orders()
        self.cancel_invalid_group_orders()

    # Order streaming / management
    def get_orders(self) -> list[Order]:
        return self.orders

    def update_order(self, order: Order) -> Order:
        found = False
        for ii, other_order in enumerate(self.orders):
            if other_order.order_id == order.order_id:
                self.orders[ii] = order
                found = True
        if not found:
            raise KeyError(f"Order {order} not found.")

    def cancel_pending_orders(self):
        new_orders = []
        for order in self.orders:
            if order.state == OrderState.PENDING:
                continue
            new_orders.append(order)
        self.orders = new_orders

    def place_order(self,
                    order: Order) -> Order:
        self.orders.append(order)
        storage = self.get_tradebook_storage()
        storage.store_order_execution(self.strategy,
                                      self.run_name,
                                      date=self.current_datetime(),
                                      order=order,
                                      event="OrderCreated")
        return order

    def cancel_order(self, order: Order) -> Order:
        for other_order in self.orders:
            if other_order.order_id == order.order_id:
                other_order.state = OrderState.CANCELLED
                self.order_stats["cancelled"] += 1

    def get_positions(self) -> list[Position]:
        return self.positions
