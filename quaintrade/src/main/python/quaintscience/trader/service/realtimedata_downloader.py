import time

from configargparse import ArgParser

from .common import DataProviderService


class RealtimeDataDownloader(DataProviderService):

    default_config_file = ".live.trader.env"

    def __init__(self,
                 *args,
                 **kwargs):
        super().__init__(*args, **kwargs)

    def start(self):
        self.logger.info("Starting stream....")
        self.data_provider.start(self.instruments)
        if hasattr(self.data_provider, "ticker_thread") and self.data_provider.ticker_thread is not None:
            self.data_provider.ticker_thread.join()
            while True: time.sleep(1)

    @classmethod
    def enrich_arg_parser(cls, p: ArgParser):
        return DataProviderService.enrich_arg_parser(p)
