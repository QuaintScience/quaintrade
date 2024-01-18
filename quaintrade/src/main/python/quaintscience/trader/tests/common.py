import unittest
import os

import pandas as pd

from quaintscience.trader.integration.kite import KiteHistoricDataProvider
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
                          data_path=None,
                          interval='10min'):
        if data_path is None:
            data_path = os.environ.get("QTRADE_PY_UNITTEST_CACHE_DIR")
        provider = KiteHistoricDataProvider(auth_credentials=None, auth_cache_filepath="auth_cache/", data_path=data_path)
        return provider.get_data_as_df(scrip=scrip, exchange=exchange, from_date=from_date, to_date=to_date, interval=interval)

    @staticmethod
    def cli_execution():
        unittest.main()
