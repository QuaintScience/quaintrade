import streamlit as st
import functools
import pandas as pd
from typing import Optional
from quaintscience.trader.integration.nselive import NSELiveHandler
from lightweight_charts.widgets import StreamlitChart
from quaintscience.trader.core.roles import Broker
from quaintscience.trader.core.bot import Bot
from quaintscience.trader.core.ds import TradeType
from quaintscience.trader.core.graphing import live_ohlc_plot
from quaintscience.trader.core.strategy import Strategy
from quaintscience.trader.core.indicator import MAIndicator, IndicatorPipeline
from streamlit_extras.stylable_container import stylable_container

import streamlit_shortcuts


class EMAStrategy(Strategy):

    def __init__(self,
                 *args,
                 ema_period1: int = 9,
                 ema_period2: int = 22,
                 **kwargs):
        self.ema1 = MAIndicator(period=ema_period1, signal="close", ma_type="EMA")
        self.ema2 = MAIndicator(period=ema_period2, signal="close", ma_type="EMA")
        indicators =[
                    (self.ema1, None, None),
                    (self.ema2, None, None)
                    ]
        indicators = IndicatorPipeline(indicators)
        kwargs["indicator_pipeline"] = {"window": indicators,
                                        "context": {}}
        super().__init__(*args, **kwargs)

    def apply_impl(self,
                   broker: Broker,
                   scrip: str,
                   exchange: str,
                   window: pd.DataFrame,
                   context: dict[str, pd.DataFrame]) -> Optional[TradeType]:
        pass


if "qtrade_nse_live" not in st.session_state:
    st.session_state.qtrade_nse_live = NSELiveHandler()
    st.session_state.qtrade_expiries = []
    st.session_state.qtrade_strikes = []

col1, col2 = st.columns([0.25, 0.75])

def do_update_expiry():
    st.session_state.qtrade_expiries = st.session_state.qtrade_nse_live.get_expiries(index)

def do_update_strikes():
    st.session_state.qtrade_strikes = st.session_state.qtrade_nse_live.get_strikes(index, expiry)

@st.cache_resource
def get_ohlc_data():
    if "qtrade_bot" not in st.session_state:
        do_init_bot()
    recent_data = st.session_state.qtrade_bot.get_recent_data(instruments=[{"exchange": "NFO", "scrip" : strikes.split(">")[2].strip()}])
    data = recent_data[list(recent_data.keys())[0]]["data"]
    return data

def do_add_ohlc_data():
    data = get_ohlc_data()
    chart.set(data)
    chart.topbar.textbox('symbol', strikes)
    chart.load()


def do_init_bot():
    if historic_data_provider not in st.session_state.qtrade_provider_objs:
        st.toast("Provider {historic_data_provider} not found.")
        return
    if "historic" not in st.session_state.qtrade_provider_objs[historic_data_provider]:
        st.toast("Provider {historic_data_provider} does not support historic data.")
        return

    historic_data_provider_obj = st.session_state.qtrade_provider_objs[historic_data_provider]["historic"]
    if historic_data_provider_obj is None:
        st.toast("Authentication not done for provider {historic_data_provider}")
        return

    st.session_state.qtrade_bot = Bot(None, EMAStrategy(), historic_data_provider_obj.data_provider, online_mode=online_mode, live_data_context_size= 15)

def do_scalp():
    pass


with st.sidebar:
    tabgraph, tabscalp = st.tabs(["Graph", "Scalp"])
    with tabgraph:
        st.header("Graphing")
        historic_data_provider = st.selectbox("Historic Data Provider", options=["fyers"])
        online_mode = st.checkbox("Online mode")
        width = st.number_input("Width", min_value=100, max_value=10000, value=900)
        height = st.number_input("Height", min_value=100, max_value=10000, value=600)
        init_hdp = st.button("Initialize", on_click=do_init_bot)
        broker = st.selectbox("Broker", options=["neo"])
        index = st.selectbox("Index", options=["NIFTY", "BANKNIFTY"], on_change=do_update_expiry)
        do_update_expiry()
        expiry = st.selectbox("Expiry", options=st.session_state.qtrade_expiries, on_change=do_update_strikes)
        do_update_strikes()
        strikes = st.radio("Strikes", options=st.session_state.qtrade_strikes)
        get_graph = streamlit_shortcuts.button("Load", on_click=do_add_ohlc_data, shortcut="Ctrl+Shift+S")
    with tabscalp:
        with stylable_container(
        "green",
        css_styles="""
        button {
            background-color: #00FF00;
            color: black;
        }""",
        ):
            streamlit_shortcuts.button("Scalp", shortcut="Ctrl+Shift+X", on_click=do_scalp)
        entry = st.number_input("Entry", step=0.5)
        sl = st.number_input("SL", step=0.5)
        target = st.number_input("Target", step=0.5)
        adjust_target_to_rr = st.button("Adjust Target to R R")
        st.markdown("---\n**Settings**")
        price_filter = st.number_input("Filter", min_value=-100., max_value=100., value=1.0, step=0.5)
        rr = st.number_input("R R", min_value=0.1, max_value=100., value=2.0, step=0.1)
        qty = st.number_input("Qty", min_value=1, max_value=100, step=1, value=5)

if "qtraded_chart" not in st.session_state:
    chart = StreamlitChart(width=width, height=height, toolbox=True)
    st.session_state.qtrade_chart = chart