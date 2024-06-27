from copy import deepcopy
from ..core.roles import TradingServiceProvider
from .fyers import FyersBaseMixin
from .neo import NeoBaseMixin

def get_instrument_for_provider(instrument: dict, provider: TradingServiceProvider):
    if issubclass(provider, FyersBaseMixin):
        instrument = FyersBaseMixin.denormalize_instrument(instrument)
    if issubclass(provider, NeoBaseMixin):
        instrument = NeoBaseMixin.denormalize_instrument(instrument)
    return instrument


def get_instruments_for_provider(instruments: list[dict], provider: TradingServiceProvider):
    instruments = deepcopy(instruments)
    print(instruments)
    for ii, instrument in enumerate(instruments):
        instruments[ii] = get_instrument_for_provider(instrument, provider)
    return instruments