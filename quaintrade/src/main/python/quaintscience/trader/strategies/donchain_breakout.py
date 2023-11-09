


from ..core.strategy import StrategyExecutor
from ..core.indicator import IndicatorPipeline, DonchainIndicator



class DonchainBreakoutStrategy(StrategyExecutor):

    def __init__(self, *args, **kwargs):
        kwargs["indicator_pipeline"] = IndicatorPipeline(indicators=[DonchainIndicator()])
        