import streamlit as st
import time
import numpy as np
import pandas as pd
import os
import yaml
import functools
import webbrowser
from quaintscience.trader.service.common import DataProviderService, BrokerService, Service
import webbrowser

PROVIDERS = ["neo", "zerodha", "fyers"]


st.set_page_config(page_title="Authentication", page_icon="📈")
if "qtrade_config" not in st.session_state:
    st.session_state.qtrade_config = {"providers": {}}

settings_file = st.text_input("Settings File", "./.auth.env")

def do_load_settings():
    if not os.path.exists(settings_file):
        st.toast("Settings not found!")
    if not "qtrade_config" in st.session_state or refresh_settings_cache or "qtrade_provider_objs" not in st.session_state:
        with open(settings_file, 'r', encoding='utf-8') as fid:
            st.session_state.qtrade_config = yaml.safe_load(fid)
            st.session_state.qtrade_provider_objs = dict.fromkeys(st.session_state.qtrade_config["providers"].keys())
            for k, v in st.session_state.qtrade_provider_objs.items():
                st.session_state.qtrade_provider_objs[k] = {}

def update_login_state():
    if "qtrade_provider_objs" not in st.session_state:
        return pd.DataFrame(columns=["Provider", "Historic Data", "Live Data", "Broker", "Login State"])
    summary = []
    print("HEWREEEE", st.session_state.qtrade_provider_objs)
    for service, obj in st.session_state.qtrade_provider_objs.items():
        data = {"Provider": service, "Historic Data": "No", "Live Data": "No", "Broker": "No", "Login State": "Not logged in."}
        if "historic" in obj:
            data["Historic Data"] = "Yes"
        if "live" in obj:
            data["Live Data"] = "Yes"
        if "broker" in obj:
            data["Broker"] = "Yes"
        if "state" in obj:
            data["Login State"] = obj["state"]
        summary.append(data)
    print(summary)
    st.session_state.qtrade_login_state = summary


def do_login(refresh_cache, provider):
    not_ready = refresh_cache
    if provider not in st.session_state.qtrade_config["providers"]:
        st.toast("Provider not found!")
        return
    components = st.session_state.qtrade_config["providers"][provider]
    config = st.session_state.qtrade_config
    provider_obj = st.session_state.qtrade_provider_objs[provider]
    if "historic_data_provider_class" in components:
        provider_obj["historic"] = DataProviderService(data_path=config["data_path"],
                                                       DataProviderClass=components["historic_data_provider_class"],
                                                       data_provider_login=False,
                                                       data_provider_init=False,
                                                       instruments=[],
                                                       StorageClass=config["storage_class"],
                                                       data_provider_auth_credentials=components["auth_credentials"],
                                                       data_provider_auth_cache_filepath=config["auth_cache_filepath"],
                                                       data_provider_reset_auth_cache=not_ready)
        print(provider_obj)
        st.session_state.qtrade_provider_objs[provider] = provider_obj
        
    if "streaming_provider_class" in components:
        provider_obj["live"] = DataProviderService(data_path=config["data_path"],
                                                   DataProviderClass=components["streaming_provider_class"],
                                                   data_provider_login=False,
                                                   data_provider_init=False,
                                                   instruments=[],
                                                   StorageClass=config["storage_class"],
                                                   data_provider_auth_credentials=components["auth_credentials"],
                                                   data_provider_auth_cache_filepath=config["auth_cache_filepath"],
                                                   data_provider_reset_auth_cache=not_ready)
        st.session_state.qtrade_provider_objs[provider] = provider_obj
    
    if "broker_class" in components:
        provider_obj["broker"] = BrokerService(data_path=config["data_path"],
                                               BrokerClass=components["broker_class"],
                                               broker_audit_records_path=components["broker_audit_records_path"],
                                               broker_login=False,
                                               broker_init=False,
                                               instruments=[],
                                               StorageClass=config["storage_class"],
                                               broker_auth_credentials=components["auth_credentials"],
                                               broker_auth_cache_filepath=config["auth_cache_filepath"],
                                               broker_reset_auth_cache=not_ready)
        st.session_state.qtrade_provider_objs[provider] = provider_obj
    provider_obj["state"] = "Not logged in."
    update_login_state()

def do_login_fyers():
    do_login(refresh_cache_fyers, "fyers")
    generator = st.session_state.qtrade_provider_objs["fyers"]["historic"].data_provider.login()
    url = next(generator) # Login url
    if isinstance(url, str):
        webbrowser.open_new_tab(url)
        next(generator) # Start listening
    st.session_state.qtrade_provider_objs["fyers"]["state"] = "Ready."
    print("Do login fyers", st.session_state.qtrade_provider_objs)
    update_login_state()

def do_login_neo():
    print(st.session_state.qtrade_provider_objs)
    st.session_state.qtrade_provider_objs["neo"]["broker"].broker.auth_inputs["otp"] = otp_neo
    for item in st.session_state.qtrade_provider_objs["neo"]["broker"].broker.finish_login():
        pass
    st.session_state.qtrade_provider_objs["neo"]["state"] = "Ready."

def do_otp_neo():
    do_login(refresh_cache_neo, "neo")
    st.session_state.qtrade_provider_objs["neo"]["broker"].broker.auth_inputs["mobile"] = mobile_neo
    st.session_state.qtrade_provider_objs["neo"]["broker"].broker.auth_inputs["password"] = password_neo
    generator = st.session_state.qtrade_provider_objs["neo"]["broker"].broker.login()
    next(generator) # Mobile
    next(generator) # Password
    next(generator) # OTP
    print(st.session_state.qtrade_provider_objs["neo"]["broker"].broker.auth_inputs)
    st.session_state.qtrade_provider_objs["neo"]["state"] = "OTP Needed"

load_settings = st.button("Load settings", on_click=do_load_settings)
refresh_settings_cache  = st.checkbox("Refresh settings")
do_load_settings()
st.write("# Fyers")

refresh_cache_fyers = st.checkbox("Refresh login cache", False, key="refresh_cache_fyers")

login_fyers = st.button("Login", on_click=do_login_fyers, key="login_fyers")

st.write("# Kotak Neo")

mobile_neo = st.text_input("Mobile (XXXXXXXXXX)", key="mobile_neo")
password_neo = st.text_input("Password", type="password", key="password_neo")
generate_otp = st.button("Generate OTP", key="btn_otp_neo", on_click=do_otp_neo)

otp_neo = st.text_input("OTP", key="otp_neo")
refresh_cache_neo = st.checkbox("Refresh login cache", False, key="refresh_cache_neo")
login_neo = st.button("Login", on_click=do_login_neo, key="login_neo")

update_login_state()

st.dataframe(data=st.session_state.qtrade_login_state)
