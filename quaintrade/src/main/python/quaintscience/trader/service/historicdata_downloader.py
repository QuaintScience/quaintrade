import time

import datetime
import configargparse

from .common import DataProviderService


class HistoricDataDownloader(DataProviderService):

    def __init__(self,
                 *args, from_date=None, to_date=None,
                 instruments: Union[str, list]=None,
                 **kwargs):
        super().__init__(*args, **kwargs)
        self.from_date = self.__correct(from_date)
        self.to_date = self.__correct(to_date)
        if isinstance(instruments, str):
            instruments = [{"scrip": value.split(":")[0], "exchange": value.split(":")[1]}
                           for value in instruments.split(",")]
        self.instruments = instruments

    def start(self):
        self.logger.info("Getting historic data....")
        for instrument in self.instruments:
            self.logger.info(f"Downloading instrument {instrument}")
            self.data_provider.get_historic_data(scrip=instrument["scrip"],
                                                 exchange=instrument["exchange"],
                                                 interval="1m",
                                                 from_date=self.from_date,
                                                 to_date=self.to_date)

    @classmethod
    def get_arg_parser(cls):
        p = DataProviderService.get_arg_parser()
        p.add('--from_date', help="From date", env_var="FROM_DATE")
        p.add('--to_date', help="To date", env_var="TO_DATE")
        p.add('--instruments', help="Instruments", env_var="INSTRUMENTS")
        return p
