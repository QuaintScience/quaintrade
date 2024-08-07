import time

from configargparse import ArgParser

from .common import DataProviderService


class RealtimeDataDownloader(DataProviderService):

    default_config_file = ".live.trader.env"

    def __init__(self,
                 *args,
                 clear_live_data_cache: bool = False,
                 **kwargs):
        if "data_provider_custom_kwargs" in kwargs and kwargs["data_provider_custom_kwargs"] is not None:
            kwargs["data_provider_custom_kwargs"]["clear_live_data_cache"] = clear_live_data_cache
        else:
            kwargs["data_provider_custom_kwargs"] = {"clear_live_data_cache": clear_live_data_cache}
        super().__init__(*args, **kwargs)

    def start(self):
        self.logger.info("Starting stream....")
        self.data_provider.start(self.instruments)
        if hasattr(self.data_provider, "ticker_thread") and self.data_provider.ticker_thread is not None:
            self.data_provider.ticker_thread.join()
            while True: time.sleep(1)

    @classmethod
    def enrich_arg_parser(cls, p: ArgParser):
        DataProviderService.enrich_arg_parser(p)
        p.add('--clear_live_data_cache', action="store_true", help="Clear live data cache before starting", env_var="CLEAR_LIVE_DATA_CACHE")
