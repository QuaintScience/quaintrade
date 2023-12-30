import time

from .common import DataProviderService


class RealtimeDataDownloader(DataProviderService):

    def __init__(self,
                 *args,
                 **kwargs):
        super().__init__(*args, **kwargs)

    def start(self):
        self.logger.info("Starting stream....")
        self.data_provider.start(self.instruments)
        self.data_provider.ticker_thread.join()
        while True: time.sleep(1)

    @classmethod
    def get_arg_parser(cls):
        p = DataProviderService.get_arg_parser()
        p._default_config_files = [".live.trader.env"]
        return p
