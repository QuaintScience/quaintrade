from functools import partial
from typing import Optional
import copy

import numpy as np

import mplfinance as mpf
import matplotlib
import matplotlib.pyplot as plt
import matplotlib.animation as animation
from matplotlib.widgets import MultiCursor

from .ds import TransactionType

matplotlib.use('TkAgg')


DEFAULT_MPF_STYLE_KWARGS = {"base_mpf_style": 'yahoo', "rc": {'font.size': 6}}


def __animate_live_ohlc_plot(ax, style, get_live_ohlc_func,
                             args=None, kwargs=None):
    ax.clear()
    mpf.plot(get_live_ohlc_func(*args, **kwargs),
             ax=ax,
             type='candle',
             style=style)

def live_ohlc_plot(get_live_ohlc_func: callable,
                   make_mpf_style_kwargs: Optional[dict] = None,
                   args: Optional[tuple] = None,
                   kwargs: Optional[dict] = None,
                   title: str = "Live Data",
                   interval: float = 250.):
    if args is None:
        args = ()
    if kwargs is None:
        kwargs = {}
    if make_mpf_style_kwargs is None:
        make_mpf_style_kwargs = DEFAULT_MPF_STYLE_KWARGS
    style = mpf.make_mpf_style(**make_mpf_style_kwargs)
    fig, axes = mpf.plot(get_live_ohlc_func(*args, **kwargs),
                         returnfig=True,
                         type='candle',
                         title=title,
                         style=style)
    ax = axes[0]
    animate = partial(__animate_live_ohlc_plot,
                      ax=ax,
                      style=style,
                      get_live_ohlc_func=get_live_ohlc_func,
                      args=args, kwargs=kwargs)
    animation.FuncAnimation(fig, animate, interval=interval)
    mpf.show()


def backtesting_results_plot(df,
                             events,
                             hlines_dct=None,
                             indicator_fields=None,
                             title="Backtesting Results",
                             make_mpf_style_kwargs=None,
                             mpf_custom_kwargs=None,
                             custom_addplots=None):
    if hlines_dct is None:
        hlines_dct = {}
    if indicator_fields is None:
        indicator_fields = []
    if make_mpf_style_kwargs is None:
        make_mpf_style_kwargs = DEFAULT_MPF_STYLE_KWARGS
    if mpf_custom_kwargs is None:
        mpf_custom_kwargs = {}
    if custom_addplots is None:
        custom_addplots = []
    style = mpf.make_mpf_style(**make_mpf_style_kwargs)

    df = copy.deepcopy(df)
    df["sell_signals"] = np.nan
    df["buy_signals"] = np.nan
    sell_signal_timestamps = [event[0]
                              for event in events
                              if event[1]["transaction_type"] == TransactionType.SELL]
    sell_price = [event[1]["price"]
                  for event in events
                  if event[1]["transaction_type"] == TransactionType.SELL]
    df.loc[sell_signal_timestamps, "sell_signals"] = sell_price
    buy_signal_timestamps = [event[0]
                              for event in events
                              if event[1]["transaction_type"] == TransactionType.BUY]
    buy_price = [event[1]["price"]
                 for event in events
                 if event[1]["transaction_type"] == TransactionType.BUY]
    df.loc[buy_signal_timestamps, "buy_signals"] = buy_price

    event_plots = []
    if len(events) > 0:
        event_plots.append(mpf.make_addplot(df["sell_signals"], type='scatter', marker=r'$\downarrow$', color='k'))
        event_plots.append(mpf.make_addplot(df["buy_signals"], type='scatter', marker=r'$\uparrow$', color='k'))
    event_plots.extend(custom_addplots)
    num_panels = 1
    for field in indicator_fields:
        if isinstance(field, str):
            event_plots.append(mpf.make_addplot(df[field]))
        else:
            event_plots.append(mpf.make_addplot(df[field.get("field")],
                                                panel=field.get("panel", 1)))
            num_panels = max(num_panels, field.get("panel", 0) + 1)
    kwargs = {"returnfig": True,
              "type": "candle",
              "title": title,
              "style": style,
              "addplot": event_plots,
              "num_panels": num_panels}
    if hlines_dct is not None and len(hlines_dct) > 0:
        kwargs["hlines"] = hlines_dct
    kwargs.update(mpf_custom_kwargs)
    fig, axes = mpf.plot(df,
                         **kwargs)
    mpf.show()

