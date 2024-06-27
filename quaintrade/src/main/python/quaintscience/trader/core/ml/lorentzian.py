# Adapted from advanced-ta package...
from typing import Optional
from enum import IntEnum
import math

import numpy as np
import pandas as pd
import talib as ta
from sklearn.preprocessing import MinMaxScaler


"""
====================
==== Background ====
====================

When using Machine Learning algorithms like K-Nearest Neighbors, choosing an
appropriate distance metric is essential. Euclidean Distance is often used as
the default distance metric, but it may not always be the best choice. This is
because market data is often significantly impacted by proximity to significant
world events such as FOMC Meetings and Black Swan events. These major economic
events can contribute to a warping effect analogous a massive object's 
gravitational warping of Space-Time. In financial markets, this warping effect 
operates on a continuum, which can analogously be referred to as "Price-Time".

To help to better account for this warping effect, Lorentzian Distance can be
used as an alternative distance metric to Euclidean Distance. The geometry of
Lorentzian Space can be difficult to visualize at first, and one of the best
ways to intuitively understand it is through an example involving 2 feature
dimensions (z=2). For purposes of this example, let's assume these two features
are Relative Strength Index (RSI) and the Average Directional Index (ADX). In
reality, the optimal number of features is in the range of 3-8, but for the sake
of simplicity, we will use only 2 features in this example.

Fundamental Assumptions:
(1) We can calculate RSI and ADX for a given chart.
(2) For simplicity, values for RSI and ADX are assumed to adhere to a Gaussian 
    distribution in the range of 0 to 100.
(3) The most recent RSI and ADX value can be considered the origin of a coordinate 
    system with ADX on the x-axis and RSI on the y-axis.

Distances in Euclidean Space:
Measuring the Euclidean Distances of historical values with the most recent point
at the origin will yield a distribution that resembles Figure 1 (below).

                       [RSI]
                         |                      
                         |                   
                         |                 
                     ...:::....              
               .:.:::••••••:::•::..             
             .:•:.:•••::::••::••....::.            
            ....:••••:••••••••::••:...:•.          
           ...:.::::::•••:::•••:•••::.:•..          
           ::•:.:•:•••••••:.:•::::::...:..         
 |--------.:•••..•••••••:••:...:::•:•:..:..----------[ADX]    
 0        :•:....:•••••::.:::•••::••:.....            
          ::....:.:••••••••:•••::••::..:.          
           .:...:••:::••••••••::•••....:          
             ::....:.....:•::•••:::::..             
               ..:..::••..::::..:•:..              
                   .::..:::.....:                
                         |            
                         |                   
                         |
                         |
                        _|_ 0        
                         
       Figure 1: Neighborhood in Euclidean Space

Distances in Lorentzian Space:
However, the same set of historical values measured using Lorentzian Distance will 
yield a different distribution that resembles Figure 2 (below).

                        
                        [RSI] 
 ::..                     |                    ..:::  
  .....                   |                  ......
   .••••::.               |               :••••••. 
    .:•••••:.             |            :::••••••.  
      .•••••:...          |         .::.••••••.    
        .::•••••::..      |       :..••••••..      
           .:•••••••::.........::••••••:..         
             ..::::••••.•••••••.•••••••:.            
               ...:•••••••.•••••••••::.              
                 .:..••.••••••.••••..                
 |---------------.:•••••••••••••••••.---------------[ADX]          
 0             .:•:•••.••••••.•••••••.                
             .••••••••••••••••••••••••:.            
           .:••••••••••::..::.::••••••••:.          
         .::••••••::.     |       .::•••:::.       
        .:••••••..        |          :••••••••.     
      .:••••:...          |           ..•••••••:.   
    ..:••::..             |              :.•••••••.   
   .:•....                |               ...::.:••.  
  ...:..                  |                   :...:••.     
 :::.                     |                       ..::  
                         _|_ 0

      Figure 2: Neighborhood in Lorentzian Space 


Observations:
(1) In Lorentzian Space, the shortest distance between two points is not 
    necessarily a straight line, but rather, a geodesic curve.
(2) The warping effect of Lorentzian distance reduces the overall influence  
    of outliers and noise.
(3) Lorentzian Distance becomes increasingly different from Euclidean Distance 
    as the number of nearest neighbors used for comparison increases.
"""

from ..indicator import Indicator

class Direction(IntEnum):
    LONG = 1
    SHORT = -1
    NEUTRAL = 0


class LorentzianClassificationIndicator(Indicator):

    # Feature Variables: User-Defined Inputs for calculating Feature Series.
    # Options: ["RSI", "WT", "CCI", "ADX"]
    # FeatureSeries Object: Calculated Feature Series based on Feature Variables

    def rationalQuadratic(self, src: pd.Series,
                          lookback: int,
                          relativeWeight: float,
                          startAtBar: int):
        """
        vectorized calculate for rational quadratic curve
        :param src:
        :param lookback:
        :param relativeWeight:
        :param startAtBar:
        :return:
        """
        currentWeight = [0.0]*len(src)
        cumulativeWeight = 0.0
        for i in range(startAtBar + 2):
            y = src.shift(i, fill_value=0.0)
            w = (1 + (i ** 2 / (lookback ** 2 * 2 * relativeWeight))) ** -relativeWeight
            currentWeight += y.values * w
            cumulativeWeight += w
        val = currentWeight / cumulativeWeight
        val[:startAtBar + 1] = 0.0

        return val


    def gaussian(self, src: pd.Series,
                 lookback: int,
                 startAtBar: int):
        """
        vectorized calculate for gaussian curve
        :param src:
        :param lookback:
        :param startAtBar:
        :return:
        """
        currentWeight = [0.0]*len(src)
        cumulativeWeight = 0.0
        for i in range(startAtBar + 2):
            y = src.shift(i, fill_value=0.0)
            w = math.exp(-(i ** 2) / (2 * lookback ** 2))
            currentWeight += y.values * w
            cumulativeWeight += w
        val = currentWeight / cumulativeWeight
        val[:startAtBar + 1] = 0.0

        return val


    def __init__(self,
                 *args,
                 source: str = "close",
                 feature_def: Optional[list[tuple]] = None,
                 neighbors_count: int = 8,
                 max_bars_back: int = 20000,
                 use_dynamic_exists: bool = False,
                 use_ema_filter: bool = False,
                 ema_period: int = 200,
                 use_sma_filter: bool = False,
                 sma_period: int = 200,
                 signals: Optional[list[str]] = None,
                 use_volatility_filter: bool = False,
                 use_regime_filter: bool = False,
                 use_adx_filter: bool = False,
                 regime_threshold: float = -0.1,
                 adx_threshold: int = 20,
                 use_kernel_filter: bool = True,
                 use_kernel_smoothing: bool = False,
                 lookback_window: int = 8,
                 relative_weight: float = 8.0,
                 regression_level: int = 25,
                 crossover_lag: int = 2, **kwargs):
        
        self.source = source
        self.neighbors_count = neighbors_count
        self.max_bars_back = max_bars_back
        self.use_dynamic_exists = use_dynamic_exists
        self.use_ema_filter = use_ema_filter
        self.ema_period = ema_period
        self.use_sma_filter = use_sma_filter
        self.sma_period = sma_period
        self.signals = signals
        self.use_volatility_filter = use_volatility_filter
        self.use_regime_filter = use_regime_filter
        self.use_adx_filter = use_adx_filter
        self.regime_threshold = regime_threshold
        self.adx_threshold = adx_threshold
        self.use_kernel_filter = use_kernel_filter
        self.use_kernel_smoothing = use_kernel_smoothing
        self.lookback_window = lookback_window
        self.relative_weight = relative_weight
        self.regression_level = regression_level
        self.crossover_lag = crossover_lag

        if feature_def is None:
            feature_def = [("RSI", 14, 2),  # f1
                        ("WT", 10, 11),  # f2
                        ("CCI", 20, 2),  # f3
                        ("ADX", 20, 2),  # f4
                        ("RSI", 9, 2)]  # f5
        self.feature_def = feature_def
        self.filterSettings = None
        self.settings = None
        self.filter = None
        self.yhat1 = None
        self.yhat2 = None
        super().__init__(*args, **kwargs)



    # ==========================
    # ==== Helper Functions ====
    # ==========================


    def normalize(self,
                  src: np.array,
                  range_min=0,
                  range_max=1) -> np.array:
        """
        function Rescales a source value with an unbounded range to a bounded range
        param src: <np.array> The input series
        param range_min: <float> The minimum value of the unbounded range
        param range_max: <float> The maximum value of the unbounded range
        returns <np.array> The normalized series
        """
        scaler = MinMaxScaler(feature_range=(0, 1))
        return range_min + (range_max - range_min) * scaler.fit_transform(src.reshape(-1,1))[:,0]


    def rescale(self, src: np.array,
                old_min, old_max,
                new_min=0, new_max=1) -> np.array:
        """
        function Rescales a source value with a bounded range to anther bounded range
        param src: <np.array> The input series
        param old_min: <float> The minimum value of the range to rescale from
        param old_max: <float> The maximum value of the range to rescale from
        param new_min: <float> The minimum value of the range to rescale to
        param new_max: <float> The maximum value of the range to rescale to 
        returns <np.array> The rescaled series
        """
        rescaled_value = new_min + (new_max - new_min) * (src - old_min) / max(old_max - old_min, 10e-10)
        return rescaled_value


    def n_rsi(self, src: pd.Series, n1, n2) -> np.array:
        """
        function Returns the normalized RSI ideal for use in ML algorithms
        param src: <np.array> The input series
        param n1: <int> The length of the RSI
        param n2: <int> The smoothing length of the RSI
        returns <np.array> The normalized RSI
        """
        return self.rescale(ta.EMA(ta.RSI(src.values, n1), n2), 0, 100)


    def n_cci(self, highSrc: pd.Series,
              lowSrc: pd.Series,
              closeSrc: pd.Series, n1, n2) -> np.array:
        """
        function Returns the normalized CCI ideal for use in ML algorithms
        param highSrc: <np.array> The input series for the high price
        param lowSrc: <np.array> The input series for the low price
        param closeSrc: <np.array> The input series for the close price
        param n1: <int> The length of the CCI
        param n2: <int> The smoothing length of the CCI
        returns <np.array> The normalized CCI
        """
        return self.normalize(ta.EMA(ta.CCI(highSrc.values,
                                            lowSrc.values,
                                            closeSrc.values, n1), n2))

    def n_wt(self, highSrc: pd.Series,
             lowSrc: pd.Series,
             closeSrc: pd.Series,
             n1=10, n2=11) -> np.array:
        """
        function Returns the normalized WaveTrend Classic series ideal for use in ML algorithms
        param src: <np.array> The input series
        param n1: <int> The first smoothing length for WaveTrend Classic
        param n2: <int> The second smoothing length for the WaveTrend Classic
        returns <np.array> The normalized WaveTrend Classic series
        """
        src = (highSrc + lowSrc + closeSrc) / 3
        ema1 = ta.EMA(src.values, n1)
        ema2 = ta.EMA(abs(src.values - ema1), n1)
        ci = (src.values - ema1) / (0.015 * ema2)
        wt1 = ta.EMA(ci, n2)  # tci
        wt2 = ta.SMA(wt1, 4)
        return self.normalize(wt1 - wt2)

    def n_adx(self,
              highSrc: pd.Series,
              lowSrc: pd.Series,
              closeSrc: pd.Series, n1, n2 = None) -> np.array:
        """
        function Returns the normalized ADX ideal for use in ML algorithms
        param highSrc: <np.array> The input series for the high price
        param lowSrc: <np.array> The input series for the low price
        param closeSrc: <np.array> The input series for the close price
        param n1: <int> The length of the ADX
        """
        return self.rescale(ta.ADX(highSrc.values,
                                   lowSrc.values,
                                   closeSrc.values, n1), 0, 100)
        # TODO: Replicate ADX logic from jdehorty


    # =================
    # ==== Filters ====
    # =================
  
    def regime_filter(self,
                      src: pd.Series,
                      high: pd.Series,
                      low: pd.Series,
                      useRegimeFilter, threshold) -> np.array:
        """
        regime_filter
        param src: <np.array> The source series
        param high: <np.array> The input series for the high price
        param low: <np.array> The input series for the low price
        param useRegimeFilter: <bool> Whether to use the regime filter
        param threshold: <float> The threshold
        returns <np.array> Boolean indicating whether or not to let the signal pass through the filter
        """
        if not useRegimeFilter: return np.array([True]*len(src))

        # @njit(parallel=True, cache=True)
        def klmf(src: np.array, high: np.array, low: np.array):
            value1 = np.array([0.0]*len(src))
            value2 = np.array([0.0]*len(src))
            klmf = np.array([0.0]*len(src))

            for i in range(len(src)):
                if (high[i] - low[i]) == 0: continue
                value1[i] = 0.2 * (src[i] - src[i - 1 if i >= 1 else 0]) + 0.8 * value1[i - 1 if i >= 1 else 0]
                value2[i] = 0.1 * (high[i] - low[i]) + 0.8 * value2[i - 1 if i >= 1 else 0]

            with np.errstate(divide='ignore',invalid='ignore'):
                omega = np.nan_to_num(np.abs(np.divide(value1, value2)))
            alpha = (-(omega ** 2) + np.sqrt((omega ** 4) + 16 * (omega ** 2))) / 8

            for i in range(len(src)):
                klmf[i] = alpha[i] * src[i] + (1 - alpha[i]) * klmf[i - 1 if i >= 1 else 0]

            return klmf

        filter = np.array([False]*len(src))
        absCurveSlope = np.abs(np.diff(klmf(src.values, high.values, low.values), prepend=0.0))
        exponentialAverageAbsCurveSlope = ta.EMA(absCurveSlope, 200)
        with np.errstate(divide='ignore',invalid='ignore'):
            normalized_slope_decline = (absCurveSlope - exponentialAverageAbsCurveSlope) / exponentialAverageAbsCurveSlope
        flags = (normalized_slope_decline >= threshold)
        filter[(len(filter) - len(flags)):] = flags
        return filter

    def filter_adx(self,
                   src: pd.Series,
                   high: pd.Series,
                   low: pd.Series,
                   adxThreshold: int,
                   useAdxFilter: bool,
                   length: int = 14) -> np.array:
        """
        function filter_adx
        param src: <np.array> The source series
        param high: <np.array> The input series for the high price
        param low: <np.array> The input series for the low price
        param adxThreshold: <int> The ADX threshold
        param useAdxFilter: <bool> Whether to use the ADX filter
        param length: <int> The length of the ADX
        returns <np.array> Boolean indicating whether or not to let the signal pass through the filter
        """
        if not useAdxFilter: return np.array([True]*len(src))
        adx = ta.ADX(high.values, low.values, src.values, length)
        return (adx > adxThreshold)


    def filter_volatility(self,
                          high: pd.Series,
                          low: pd.Series,
                          close: pd.Series,
                          useVolatilityFilter: bool,
                          minLength: int = 1,
                          maxLength: int = 10) -> np.array:
        """
        function filter_volatility
        param high: <np.array> The input series for the high price
        param low: <np.array> The input series for the low price
        param close: <np.array> The input series for the close price
        param useVolatilityFilter: <bool> Whether to use the volatility filter
        param minLength: <int> The minimum length of the ATR
        param maxLength: <int> The maximum length of the ATR
        returns <np.array> Boolean indicating whether or not to let the signal pass through the filter
        """
        if not useVolatilityFilter: return np.array([True]*len(close))
        recentAtr = ta.ATR(high.values, low.values, close.values, minLength)
        historicalAtr = ta.ATR(high.values, low.values, close.values, maxLength)
        return (recentAtr > historicalAtr)

    def get_default_column_names_impl(self, output_column_names: dict[str, str], settings: dict) -> dict[str, str]:
        return {}

    def get_feature(self, df: pd.DataFrame, f: str):
        match f[0]:
            case "RSI":
                return self.n_rsi(df['close'], *f[1:])
            case "WT":
                return self.n_wt(df["high"], df["low"], df["close"], *f[1:])
            case "CCI":
                return self.n_cci(df['high'], df['low'], df['close'], *f[1:])
            case "ADX":
                return self.n_adx(df['high'], df['low'], df['close'], *f[1:])

    def shift(self, arr, len, fill_value=0.0):
        return np.pad(arr, (len,), mode='constant', constant_values=(fill_value,))[:arr.size]


    def barssince(self, s: np.array):
        val = np.array([0.0]*s.size)
        c = math.nan
        for i in range(s.size):
            if s[i]: c = 0; continue
            if c >= 0: c += 1
            val[i] = c
        return val


    def compute_impl(self, df: pd.DataFrame,
                     output_column_names: str | dict[str, str] | None = None,
                     settings: dict | None = None) -> pd.DataFrame:

        features = []
        for f in self.feature_def:
            features.append(self.get_feature(df, f))


        ohlc4 = (df['open'] + df['high'] + df['low'] + df['close']) / 4
        
        volatility = self.filter_volatility(df['high'], df['low'], df['close'],
                                            self.use_volatility_filter,
                                            1,
                                            10)
        regime = self.regime_filter(ohlc4,
                                    df['high'],
                                    df['low'],
                                    self.use_regime_filter,
                                    self.regime_threshold)
        adx = self.filter_adx(df[self.source],
                              df['high'], df['low'],
                              self.adx_threshold,
                              self.use_adx_filter, 14)

        # Derived from General Settings
        maxBarsBackIndex = (len(df.index) - self.max_bars_back) if (len(df.index) >= self.max_bars_back) else 0

        isEmaUptrend = np.where(self.use_ema_filter,
                                (df["close"] > ta.EMA(df["close"],
                                                      self.ema_period)),
                                 True)
        isEmaDowntrend = np.where(self.use_ema_filter,
                                  (df["close"] < ta.EMA(df["close"],
                                                        self.ema_period)),
                                  True)
        isSmaUptrend = np.where(self.use_ema_filter,
                                (df["close"] > ta.SMA(df["close"],
                                                      self.sma_period)),
                                 True)
        isSmaDowntrend = np.where(self.use_sma_filter,
                                  (df["close"] < ta.SMA(df["close"],
                                                        self.sma_period)),
                                  True)

        """
        =================================
        ==== Next Bar Classification ====
        =================================

        This model specializes specifically in predicting the direction of price action over the course of the next 4 bars. 
        To avoid complications with the ML model, this value is hardcoded to 4 bars but support for other training lengths may be added in the future.

        =========================
        ====  Core ML Logic  ====
        =========================

        Approximate Nearest Neighbors Search with Lorentzian Distance:
        A novel variation of the Nearest Neighbors (NN) search algorithm that ensures a chronologically uniform distribution of neighbors.

        In a traditional KNN-based approach, we would iterate through the entire dataset and calculate the distance between the current bar 
        and every other bar in the dataset and then sort the distances in ascending order. We would then take the first k bars and use their 
        labels to determine the label of the current bar. 

        There are several problems with this traditional KNN approach in the context of real-time calculations involving time series data:
        - It is computationally expensive to iterate through the entire dataset and calculate the distance between every historical bar and
          the current bar.
        - Market time series data is often non-stationary, meaning that the statistical properties of the data change slightly over time.
        - It is possible that the nearest neighbors are not the most informative ones, and the KNN algorithm may return poor results if the
          nearest neighbors are not representative of the majority of the data.

        Previously, the user @capissimo attempted to address some of these issues in several of his PineScript-based KNN implementations by:
        - Using a modified KNN algorithm based on consecutive furthest neighbors to find a set of approximate "nearest" neighbors.
        - Using a sliding window approach to only calculate the distance between the current bar and the most recent n bars in the dataset.

        Of these two approaches, the latter is inherently limited by the fact that it only considers the most recent bars in the overall dataset. 

        The former approach has more potential to leverage historical price action, but is limited by:
        - The possibility of a sudden "max" value throwing off the estimation
        - The possibility of selecting a set of approximate neighbors that are not representative of the majority of the data by oversampling 
          values that are not chronologically distinct enough from one another
        - The possibility of selecting too many "far" neighbors, which may result in a poor estimation of price action

        To address these issues, a novel Approximate Nearest Neighbors (ANN) algorithm is used in this indicator.

        In the below ANN algorithm:
        1. The algorithm iterates through the dataset in chronological order, using the modulo operator to only perform calculations every 4 bars.
           This serves the dual purpose of reducing the computational overhead of the algorithm and ensuring a minimum chronological spacing 
           between the neighbors of at least 4 bars.
        2. A list of the k-similar neighbors is simultaneously maintained in both a predictions array and corresponding distances array.
        3. When the size of the predictions array exceeds the desired number of nearest neighbors specified in settings.neighborsCount, 
           the algorithm removes the first neighbor from the predictions array and the corresponding distance array.
        4. The lastDistance variable is overriden to be a distance in the lower 25% of the array. This step helps to boost overall accuracy 
           by ensuring subsequent newly added distance values increase at a slower rate.
        5. Lorentzian distance is used as a distance metric in order to minimize the effect of outliers and take into account the warping of 
           "price-time" due to proximity to significant economic events.
        """

        src = df[self.source]

        def get_lorentzian_predictions():
            for bar_index in range(maxBarsBackIndex): yield 0

            predictions = []
            distances = []
            y_train_array = np.where(src.shift(4) < src.shift(0),
                                     Direction.SHORT,
                                     np.where(src.shift(4) > src.shift(0),
                                              Direction.LONG,
                                              Direction.NEUTRAL))

            class Distances(object):
                batchSize = 50
                lastBatch = 0

                def __init__(self, distances_features):
                    self.size = (len(src) - maxBarsBackIndex)
                    self.features = distances_features
                    self.maxBarsBackIndex = maxBarsBackIndex
                    self.dists = np.array([[0.0] * self.size] * self.batchSize)
                    self.rows = np.array([0.0] * self.batchSize)

                def __getitem__(self, item):
                    batch = math.ceil((item + 1)/self.batchSize) * self.batchSize
                    if batch > self.lastBatch:
                        self.dists.fill(0.0)
                        for feature in self.features:
                            self.rows.fill(0.0)
                            fBatch = feature[(self.maxBarsBackIndex + self.lastBatch):(self.maxBarsBackIndex + batch)]
                            self.rows[:fBatch.size] = fBatch.reshape(-1,)
                            val = np.log(1 + np.abs(self.rows.reshape(-1,1) - feature[:self.size].reshape(1,-1)))
                            self.dists += val
                        self.lastBatch = batch

                    return self.dists[item % self.batchSize]

            dists = Distances(features)
            for bar_index in range(maxBarsBackIndex, len(src)):
                lastDistance = -1.0
                span = min(self.max_bars_back,
                           bar_index + 1)
                for i, d in enumerate(dists[bar_index - maxBarsBackIndex][:span]):
                    if d >= lastDistance and i % 4:
                        lastDistance = d
                        distances.append(d)
                        predictions.append(round(y_train_array[i]))
                        if len(predictions) > self.neighbors_count:
                            lastDistance = distances[round(self.neighbors_count * 3 / 4)]
                            distances.pop(0)
                            predictions.pop(0)
                yield sum(predictions)


        prediction = np.array([p for p in get_lorentzian_predictions()])


        # ============================
        # ==== Prediction Filters ====
        # ============================

        # User Defined Filters: Used for adjusting the frequency of the ML Model's predictions
        filter_all = volatility & regime & adx

        # Filtered Signal: The model's prediction of future price movement direction with user-defined filters applied
        signal = np.where(((prediction > 0) & filter_all),
                          Direction.LONG,
                          np.where(((prediction < 0) & filter_all),
                                   Direction.SHORT,
                                   None))
        signal[0] = (0 if signal[0] == None else signal[0])
        for i in np.where(signal == None)[0]: signal[i] = signal[i - 1 if i >= 1 else 0]
        
        change = lambda ser, i: (self.shift(ser,
                                            i,
                                            fill_value=ser[0]) != self.shift(ser,
                                                                             i + 1,
                                                                             fill_value=ser[0]))

        # Bar-Count Filters: Represents strict filters based on a pre-defined holding period of 4 bars
        barsHeld = []
        isDifferentSignalType = (signal != self.shift(signal,
                                                      1,
                                                      fill_value=signal[0]))
        _sigFlip = np.where(isDifferentSignalType)[0].tolist()
        if not (len(isDifferentSignalType) in _sigFlip): _sigFlip.append(len(isDifferentSignalType))
        for i, x in enumerate(_sigFlip):
            if i > 0: barsHeld.append(0)
            barsHeld += range(1, x-(-1 if i == 0 else _sigFlip[i-1]))
        isHeldFourBars = (pd.Series(barsHeld) == 4).tolist()
        isHeldLessThanFourBars = (pd.Series(barsHeld) < 4).tolist()

        # Fractal Filters: Derived from relative appearances of signals in a given time series fractal/segment with a default length of 4 bars
        isEarlySignalFlip = (change(signal, 0) & change(signal, 1) & change(signal, 2) & change(signal, 3))
        isBuySignal = ((signal == Direction.LONG) & isEmaUptrend & isSmaUptrend)
        isSellSignal = ((signal == Direction.SHORT) & isEmaDowntrend & isSmaDowntrend)
        isLastSignalBuy = (self.shift(signal, 4) == Direction.LONG) & self.shift(isEmaUptrend, 4) & self.shift(isSmaUptrend, 4)
        isLastSignalSell = (self.shift(signal, 4) == Direction.SHORT) & self.shift(isEmaDowntrend, 4) & self.shift(isSmaDowntrend, 4)
        isNewBuySignal = (isBuySignal & isDifferentSignalType)
        isNewSellSignal = (isSellSignal & isDifferentSignalType)

        crossover   = lambda s1, s2: (s1 > s2) & (self.shift(s1, 1) < self.shift(s2, 1))
        crossunder  = lambda s1, s2: (s1 < s2) & (self.shift(s1, 1) > self.shift(s2, 1))

        # Kernel Regression Filters: Filters based on Nadaraya-Watson Kernel Regression using the Rational Quadratic Kernel
        # For more information on this technique refer to my other open source indicator located here:
        # https://www.tradingview.com/script/AWNvbPRM-Nadaraya-Watson-Rational-Quadratic-Kernel-Non-Repainting/
        #kFilter = self.filterSettings.kernelFilter
        yhat1 = self.rationalQuadratic(src,
                                       self.lookback_window,
                                       self.relative_weight,
                                       self.regression_level)
        yhat2 = self.gaussian(src, self.lookback_window - self.crossover_lag,
                              self.regression_level)

        # Kernel Rates of Change
        wasBearishRate = np.where(self.shift(yhat1, 2) > self.shift(yhat1, 1), True, False)
        wasBullishRate = np.where(self.shift(yhat1, 2) < self.shift(yhat1, 1), True, False)
        isBearishRate = np.where(self.shift(yhat1, 1) > yhat1, True, False)
        isBullishRate = np.where(self.shift(yhat1, 1) < yhat1, True, False)
        isBearishChange = isBearishRate & wasBullishRate
        isBullishChange = isBullishRate & wasBearishRate
        # Kernel Crossovers
        isBullishCrossAlert = crossover(yhat2, yhat1)
        isBearishCrossAlert = crossunder(yhat2, yhat1)
        isBullishSmooth = (yhat2 >= yhat1)
        isBearishSmooth = (yhat2 <= yhat1)
        # Kernel Colors
        # plot(kernelEstimate, color=plotColor, linewidth=2, title="Kernel Regression Estimate")
        # Alert Variables
        alertBullish = np.where(self.use_kernel_smoothing,
                                isBullishCrossAlert,
                                isBullishChange)
        alertBearish = np.where(self.use_kernel_smoothing,
                                isBearishCrossAlert,
                                isBearishChange)
        # Bullish and Bearish Filters based on Kernel
        isBullish = np.where(self.use_kernel_filter,
                             np.where(self.use_kernel_smoothing,
                                      isBullishSmooth,
                                      isBullishRate),
                             True)
        isBearish = np.where(self.use_kernel_filter,
                             np.where(self.use_kernel_smoothing,
                                      isBearishSmooth,
                                      isBearishRate),
                             True)
        yhat2[yhat2 == 0.] = np.nan
        yhat1[yhat1 == 0.] = np.nan
        # ===========================
        # ==== Entries and Exits ====
        # ===========================

        # Entry Conditions: Booleans for ML Model Position Entries
        startLongTrade = isNewBuySignal & isBullish & isEmaUptrend & isSmaUptrend
        startShortTrade = isNewSellSignal & isBearish & isEmaDowntrend & isSmaDowntrend

        # Dynamic Exit Conditions: Booleans for ML Model Position Exits based on Fractal Filters and Kernel Regression Filters
        # lastSignalWasBullish = barssince(startLongTrade) < barssince(startShortTrade)
        # lastSignalWasBearish = barssince(startShortTrade) < barssince(startLongTrade)
        barsSinceRedEntry = self.barssince(startShortTrade)
        barsSinceRedExit = self.barssince(alertBullish)
        barsSinceGreenEntry = self.barssince(startLongTrade)
        barsSinceGreenExit = self.barssince(alertBearish)
        isValidShortExit = barsSinceRedExit > barsSinceRedEntry
        isValidLongExit = barsSinceGreenExit > barsSinceGreenEntry
        endLongTradeDynamic = isBearishChange & self.shift(isValidLongExit, 1)
        endShortTradeDynamic = isBullishChange & self.shift(isValidShortExit, 1)

        # Fixed Exit Conditions: Booleans for ML Model Position Exits based on Bar-Count Filters
        endLongTradeStrict = ((isHeldFourBars & isLastSignalBuy) | (isHeldLessThanFourBars & isNewSellSignal & isLastSignalBuy)) & self.shift(startLongTrade, 4)
        endShortTradeStrict = ((isHeldFourBars & isLastSignalSell) | (isHeldLessThanFourBars & isNewBuySignal & isLastSignalSell)) & self.shift(startShortTrade, 4)
        isDynamicExitValid = ~self.use_ema_filter & ~self.use_sma_filter & ~self.use_kernel_smoothing
        endLongTrade = self.use_dynamic_exists & isDynamicExitValid & endLongTradeDynamic | endLongTradeStrict
        endShortTrade = self.use_dynamic_exists & isDynamicExitValid & endShortTradeDynamic | endShortTradeStrict

        df['isEmaUptrend'] = isEmaUptrend
        df['isEmaDowntrend'] = isEmaDowntrend
        df['isSmaUptrend'] = isSmaUptrend
        df['isSmaDowntrend'] = isSmaDowntrend
        df["prediction"] = prediction
        df["signal"] = signal
        df["barsHeld"] = barsHeld
        # df["isHeldFourBars"] = isHeldFourBars
        # df["isHeldLessThanFourBars"] = isHeldLessThanFourBars
        df["isEarlySignalFlip"] = isEarlySignalFlip
        # df["isBuySignal"] = isBuySignal
        # df["isSellSignal"] = isSellSignal
        df["isLastSignalBuy"] = isLastSignalBuy
        df["isLastSignalSell"] = isLastSignalSell
        df["isNewBuySignal"] = isNewBuySignal
        df["isNewSellSignal"] = isNewSellSignal

        df["startLongTrade"] = np.where(startLongTrade, df['low'], np.NaN)
        df["startShortTrade"] = np.where(startShortTrade, df['high'], np.NaN)
        df["endLongTrade"] = np.where(endLongTrade, df['high'], np.NaN)
        df["endShortTrade"] = np.where(endShortTrade, df['low'], np.NaN)
        df["isBearish"] = isBearishSmooth
        df["isBullish"] = isBullishSmooth
        df["yhat1"] = yhat1
        df["yhat2"] = yhat2
        #self.plot(df, yhat1, yhat2, "test")
        return df, output_column_names, settings

    # =============================
    # ==== Dump or Return Data ====
    # =============================

    def dump(self, df: pd.DataFrame, name: str):
        df.to_csv(name)


    # =========================
    # ====    Plotting     ====
    # =========================

    def plot(self, df: pd.DataFrame, yhat1, yhat2, name: str):
        import mplfinance as mpf
        import matplotlib
        matplotlib.use('qtagg')
        siz = df.index.size

        # yhat1_g = [self.yhat1[v] if np.where(useKernelSmoothing, isBullishSmooth, isBullishRate)[v] else np.NaN for v in range(self.df.head(len).index.size)]
        # yhat1_r = [self.yhat1[v] if ~np.where(useKernelSmoothing, isBullishSmooth, isBullishRate)[v] else np.NaN for v in range(self.df.head(len).index.size)]
        sub_plots = [
            mpf.make_addplot(yhat1, ylabel="Kernel Regression Estimate", color='blue', type="line"),
            mpf.make_addplot(yhat2, ylabel="yhat2", color='gray'),
            mpf.make_addplot(df["isBearish"], ylabel="prediction", color='gray', panel=1),
            mpf.make_addplot(df["isBullish"], ylabel="prediction", color='green', panel=1),
            mpf.make_addplot(df["startLongTrade"], ylabel="startLongTrade", color='green', type='scatter', markersize=120, marker='^'),
            mpf.make_addplot(df["endLongTrade"], ylabel="endLongTrade", color='black', type='scatter', markersize=120, marker='.'),
            mpf.make_addplot(df["startShortTrade"], ylabel="startShortTrade", color='red', type='scatter', markersize=120, marker='v'),
            mpf.make_addplot(df["endShortTrade"], ylabel="endShortTrade", color='orange', type='scatter', markersize=120, marker='.'),
        ]
        s = mpf.make_mpf_style(**{"base_mpf_style": 'yahoo', "rc": {'font.size': 6}})
        fig, axlist = mpf.plot(df, type='candle', style=s, addplot=sub_plots, returnfig=True)

        for x in range(siz):
            y = df.loc[df.index[x], 'low']
            axlist[0].text(x, y, df.loc[df.index[x], "prediction"])

        #fig.figure.savefig(fname=name)
        mpf.show()
