import pandas as pd
import numpy as np

from quaintscience.trader.tests.common import Unittest
from quaintscience.trader.core.indicator import (DonchainIndicator,
                                                 PullbackDetector,
                                                 PastPeriodHighLowIndicator,
                                                 SMAIndicator,
                                                 WMAIndicator,
                                                 ADXIndicator,
                                                 RSIIndicator,
                                                 ATRIndicator,
                                                 BBANDSIndicator,
                                                 CDLPatternIndicator,
                                                 BreakoutIndicator,
                                                 HeikinAshiIndicator,
                                                 SupportIndicator,
                                                 SlopeIndicator)

from quaintscience.trader.core.graphing import backtesting_results_plot


class TestIndicators(Unittest):

    def customSetUp(self):
        self.test_data = self.get_historic_data(scrip="NIFTY 50",
                                                exchange="NSE",
                                                from_date="20230101",
                                                to_date="20230107")

    def validate_indicator_output(self,
                                  indicator,
                                  output_column_count: int,
                                  expected_settings: list[str],
                                  result: tuple):

        self.assertIsInstance(result, tuple)
        self.assertEqual(len(result), 3)
        df, output_columns, settings = result
        self.assertIsInstance(df, pd.DataFrame)
        self.assertIsInstance(output_columns, dict)
        self.assertIsInstance(settings, dict)
        for setting in expected_settings:
            self.assertIn(setting, settings)
            self.logger.info(f"Found setting {setting} in {indicator.__class__.__name__}'s output ")
        self.assertEqual(len(output_columns), output_column_count)
        for value in output_columns.values():
            self.assertIn(value, df.columns)
            self.logger.info(f'Found output column {value} using {indicator.__class__.__name__}')

    def test_donchain_indicator(self):
        indicator = DonchainIndicator()
        result = indicator.compute(self.test_data)
        self.validate_indicator_output(indicator, 3, ["period"], result)

    def test_pullback_detector(self):
        detector = PullbackDetector(breakout_column="donchainUpper_15",
                                    price_column="high",
                                    pullback_direction=PullbackDetector.PULLBACK_DIRECTION_DOWN)
        indicator = DonchainIndicator()
        result = indicator.compute(self.test_data)
        df = result[0]
        result = detector.compute(df)
        self.validate_indicator_output(detector, 2, ["pullback_direction",
                                                     "breakout_column",
                                                     "price_column",
                                                     "data_period",
                                                     "wick_threshold"], result)
        result = detector.compute(df, settings={"pullback_direction": PullbackDetector.PULLBACK_DIRECTION_UP,
                                                "breakout_column": "donchainLower_15",
                                                "price_column": "low"})
        self.validate_indicator_output(detector, 2, ["pullback_direction",
                                                     "breakout_column",
                                                     "price_column",
                                                     "data_period",
                                                     "wick_threshold"], result)
        #print(result[0])
        #backtesting_results_plot(df, [], indicator_fields=[{"field": "donchainUpper_15_pullback_start", "panel": 2},
        #                                                   {"field": "donchainUpper_15_pullback_end", "panel": 2},
        #                                                   {"field": "donchainLower_15_pullback_start", "panel": 3},
        #                                                   {"field": "donchainLower_15_pullback_end", "panel": 3},
        #                                                   {"field": "_breakouts", "panel": 1},
        #                                           "donchainUpper_15",
        #                                           "donchainMiddle_15",
        #                                           "donchainLower_15"])

    def test_past_period_high_low_indicator(self):

        indicator = PastPeriodHighLowIndicator(period_interval="1d",
                                               data_interval="10min")
        result = indicator.compute(self.test_data)
        self.validate_indicator_output(indicator, 2, ["period_interval",
                                                      "data_interval",
                                                      "shift"], result)
        #backtesting_results_plot(result[0], [], indicator_fields=["previous_high_1d_1",
        #                                                   "previous_low_1d_1"])

    def test_sma_indicator(self):

        indicator = SMAIndicator()
        result = indicator.compute(self.test_data)
        self.validate_indicator_output(indicator, 1, ["period"], result)

    def test_wma_indicator(self):

        indicator = WMAIndicator()
        result = indicator.compute(self.test_data)
        self.validate_indicator_output(indicator, 1, ["period"], result)

    def test_adx_indicator(self):
        indicator = ADXIndicator()
        result = indicator.compute(self.test_data)
        self.validate_indicator_output(indicator, 1, [], result)

    def test_rsi_indicator(self):
        indicator = RSIIndicator()
        result = indicator.compute(self.test_data)
        self.validate_indicator_output(indicator, 1, [], result)

    def test_atr_indicator(self):
        indicator = ATRIndicator()
        result = indicator.compute(self.test_data)
        self.validate_indicator_output(indicator, 1, [], result)

    def test_bbands_indicator(self):
        indicator = BBANDSIndicator()
        result = indicator.compute(self.test_data)
        self.validate_indicator_output(indicator, 3, ["period",
                                                      "nbdevup",
                                                      "nbdevdown"], result)

    def test_cdl_indicator(self):
        indicator = CDLPatternIndicator()
        result = indicator.compute(self.test_data)
        self.validate_indicator_output(indicator, 61, [], result)

    def test_breakout_indicator(self):
        indicator = DonchainIndicator()
        result = indicator.compute(self.test_data)
        indicator = BreakoutIndicator(upper_breakout_column="donchainUpper_15",
                                      lower_breakout_column="donchainLower_15")
        result = indicator.compute(self.test_data)
        self.validate_indicator_output(indicator, 2, [], result)

    def test_heikinashi_indicator(self):
        indicator = HeikinAshiIndicator()
        result = indicator.compute(self.test_data)
        self.validate_indicator_output(indicator, 4, [], result)

    def test_support_indicator(self):
        indicator = WMAIndicator(period=22)
        result = indicator.compute(self.test_data)
        indicator = SupportIndicator(direction=SupportIndicator.SUPPORT_DIRECTION_UP,
                                     signal="WMA_22",
                                     factor=0.04/100)
        result = indicator.compute(result[0])
        result = indicator.compute(result[0], settings={"direction": SupportIndicator.SUPPORT_DIRECTION_DOWN})
        self.validate_indicator_output(result, 1, ["direction", "signal", "factor"], result)
        """
        print(result[0])
        backtesting_results_plot(result[0], [], indicator_fields=[{"field": "WMA_22_down_support", "panel": 2},
                                                           {"field": "WMA_22_up_support", "panel": 1},
                                                           "WMA_22"],
                                                           mpf_custom_kwargs={"ylim": (np.mean(result[0]["close"]) - 1.5 * result[0]["close"].std(),
                                                                                       np.mean(result[0]["close"]) + 1.5 * result[0]["close"].std()),
                                                                              "fill_between": {"y1": result[0]["_support_zone_upper"].fillna(0.).values,
                                                                                               "y2": result[0]["_support_zone_lower"].fillna(0.).values, "alpha": 0.5}})
        """

    def test_slope_indicator(self):
        indicator = WMAIndicator(period=22)
        result = indicator.compute(self.test_data)
        indicator = SlopeIndicator(signal="WMA_22")
        result = indicator.compute(result[0])

        self.validate_indicator_output(result, 2, ["signal", "shift"], result)
        """
        backtesting_results_plot(result[0], [], indicator_fields=[{"field": "WMA_22_acceleration", "panel": 2},
                                                           {"field": "WMA_22_slope", "panel": 1},
                                                           "WMA_22"])
        """

if __name__ == "__main__":
    TestIndicators.cli_execution()