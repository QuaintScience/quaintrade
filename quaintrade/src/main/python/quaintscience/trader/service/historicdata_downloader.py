from .common import DataProviderService
from ..core.util import get_datetime


class HistoricDataDownloader(DataProviderService):

    def __init__(self,
                 *args, from_date=None, to_date=None,
                 **kwargs):
        super().__init__(*args, **kwargs)
        self.from_date = get_datetime(from_date)
        self.to_date = get_datetime(to_date)

    def start(self):
        self.logger.info("Getting historic data....")
        for instrument in self.instruments:
            self.logger.info(f"Downloading instrument {instrument}")
            self.data_provider.download_data_in_batches(scrip=instrument["scrip"],
                                                        exchange=instrument["exchange"],
                                                        from_date=self.from_date,
                                                        to_date=self.to_date)

    @classmethod
    def get_arg_parser(cls):
        p = DataProviderService.get_arg_parser()
        p._default_config_files = [".historic.trader.env"]
        p.add('--from_date', help="From date", env_var="FROM_DATE")
        p.add('--to_date', help="To date", env_var="TO_DATE")
        return p
