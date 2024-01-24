from functools import partial
from typing import Optional, Union
import copy
import datetime
import numpy as np
import pandas as pd

import mplfinance as mpf
import matplotlib
import matplotlib.pyplot as plt
import matplotlib.animation as animation
from matplotlib.widgets import MultiCursor

from .ds import TransactionType
from .util import resample_candle_data

matplotlib.use('TkAgg')


DEFAULT_MPF_STYLE_KWARGS = {"base_mpf_style": 'yahoo', "rc": {'font.size': 6}}


def __animate_live_ohlc_plot(*fargs, ax=None, style=None, get_live_ohlc_func=None,
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
    anim = animation.FuncAnimation(fig, animate, interval=interval)
    mpf.show()


def plot_backtesting_results(df: pd.DataFrame,
                             context: dict[str, pd.DataFrame],
                             events: pd.DataFrame,
                             interval: str,
                             indicator_fields: list[Union[dict, str]],
                             title: str = "Backtesting Results",
                             make_mpf_style_kwargs: Optional[dict] = None,
                             mpf_custom_kwargs: Optional[dict] = None,
                             custom_addplots: Optional[list] = None,
                             hlines: Optional[dict] = None):
    if hlines is None:
        hlines = {}
    if indicator_fields is None:
        indicator_fields = []
    if make_mpf_style_kwargs is None:
        make_mpf_style_kwargs = DEFAULT_MPF_STYLE_KWARGS
    if mpf_custom_kwargs is None:
        mpf_custom_kwargs = {}
    if custom_addplots is None:
        custom_addplots = []
    style = mpf.make_mpf_style(**make_mpf_style_kwargs)

    events_exist = False
    df = copy.deepcopy(df)
    if events is not None:
        sell_events_df = copy.deepcopy(events[events["transaction_type"] == TransactionType.SELL])
        sell_events_df["sell_signals"] = sell_events_df["price"]
        sell_events_df = sell_events_df[["sell_signals"]]
        if len(sell_events_df) > 0:
            events_exist = True
            df = df.merge(sell_events_df, how='left', left_index=True, right_index=True)

        buy_events_df = copy.deepcopy(events[events["transaction_type"] == TransactionType.BUY])
        buy_events_df["buy_signals"] = buy_events_df["price"]
        buy_events_df = buy_events_df[["buy_signals"]]
        if len(buy_events_df):
            events_exist = True
            df = df.merge(buy_events_df, on="date", how='left')

    event_plots = []
    
    if events_exist:

        event_plots.append(mpf.make_addplot(df["sell_signals"],
                                            type='scatter',
                                            marker=r'$\downarrow$',
                                            color='k'))
        event_plots.append(mpf.make_addplot(df["buy_signals"],
                                            type='scatter',
                                            marker=r'$\uparrow$',
                                            color='k'))

    event_plots.extend(custom_addplots)

    num_panels = 1
    print("Indicator Fields", indicator_fields)

    for field in indicator_fields:
        if isinstance(field, str):
            event_plots.append(mpf.make_addplot(df[field]))
        else:
            if "context" in field:
                context_data = context[field["context"]][field["field"]].resample(interval, origin=datetime.datetime.fromisoformat('1970-01-01 09:15:00')).ffill()
                df = df.merge(context_data, how='left', left_index=True, right_index=True, suffixes=(None, f"_{field['context']}"))
                print(field)
                print(df)
                event_plots.append(mpf.make_addplot(df.get(f"{field['field']}_{field['context']}"),
                                                    panel=field.get("panel", 1)))
            else:
                print(field)
                print(df)
                event_plots.append(mpf.make_addplot(df[field.get("field")],
                                                    panel=field.get("panel", 1)))
            num_panels = max(num_panels, field.get("panel", 0) + 1)
    
    kwargs = {"returnfig": True,
              "type": "candle",
              "title": title,
              "style": style,
              "num_panels": num_panels}
    if len(event_plots) > 0:
        kwargs["addplot"] = event_plots
    if hlines is not None and len(hlines) > 0:
        kwargs["hlines"] = hlines

    kwargs.update(mpf_custom_kwargs)
    # print(kwargs)
    fig, axes = mpf.plot(df,
                         **kwargs)
    mpf.show()

