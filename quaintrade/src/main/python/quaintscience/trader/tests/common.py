import unittest
import os

import pandas as pd

from ..service.backtester import BackTesterService
from ..core.logging import LoggerMixin

class Unittest(unittest.TestCase, LoggerMixin):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def setUp(self):
        self.resources_dir = os.environ.get("QTRADE_PY_UNITTEST_DATA_DIR")
        LoggerMixin.__init__(self)
        self.customSetUp()

    def customSetUp(self):
        pass

    def get_resource_file(self, filepath):
        return os.path.join(self.resources_dir, filepath)

    def read_pd_resource_file(self, filepath):
        return pd.read_csv(self.get_resource_file(filepath))

    def tearDown(self):
        self.customTearDown()

    def customTearDown(self):
        pass

    def get_historic_data(self,
                          scrip,
                          exchange,
                          from_date,
                          to_date,
                          cache_path=None,
                          interval='10min',
                          redis_server='localhost',
                          redis_port=6379):
        if cache_path is None:
            cache_path = os.environ.get("QTRADE_PY_UNITTEST_CACHE_DIR")
        instrument = {"scrip": scrip, "exchange": exchange}
        service = BackTesterService(from_date=from_date,
                                    to_date=to_date,
                                    interval=interval,
                                    redis_server=redis_server,
                                    redis_port=redis_port,
                                    instruments=f"{scrip}:{exchange}",
                                    cache_path=cache_path,
                                    init=False)
        return service.trade_manager.get_historic_data(scrip=instrument["scrip"],
                                                       exchange=instrument["exchange"],
                                                       interval=service.interval,
                                                       from_date=service.from_date,
                                                       to_date=service.to_date,
                                                       download=False)

    @staticmethod
    def cli_execution():
        unittest.main()
