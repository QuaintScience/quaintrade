from typing import Union
import datetime
from configargparse import ArgParser
from .common import DataProviderService
from ..core.util import get_datetime


class HistoricDataDownloader(DataProviderService):

    default_config_file = ".historic.trader.env"

    def __init__(self,
                 *args,
                 from_date: Union[str, datetime.datetime] = None,
                 to_date: Union[str, datetime.datetime] = None,
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
    def enrich_arg_parser(cls, p: ArgParser):
        DataProviderService.enrich_arg_parser(p)
        p.add('--from_date', help="From date", env_var="FROM_DATE")
        p.add('--to_date', help="To date", env_var="TO_DATE")
