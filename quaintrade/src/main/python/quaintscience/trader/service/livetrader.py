from configargparse import ArgParser

from .common import BotService


class LiveTraderService(BotService):

    default_config_file = ".livetrading.trader.env"

    def __init__(self,
                 *args,
                 interval: str = "3min",
                 clear_tradebook_for_scrip_and_exchange: bool = False,
                 **kwargs):
        self.interval = interval
        self.clear_tradebook_for_scrip_and_exchange = clear_tradebook_for_scrip_and_exchange
        kwargs["data_provider_login"] = True
        kwargs["data_provider_init"] = True
        kwargs["bot_online_mode"] = True
        BotService.__init__(self, *args, **kwargs)

    def start(self):
        self.logger.info("Running live trader...")
        self.bot.live(self.instruments, self.interval)

    @classmethod
    def enrich_arg_parser(cls, p: ArgParser):
        BotService.enrich_arg_parser(p)
        p.add('--interval', help="To date", env_var="INTERVAL")
        p.add('--clear_tradebook_for_scrip_and_exchange',
              action="store_true",
              help="Clear tradebook for scrip and exchange",
              env_var="CLEAR_TRADEBOOK_FOR_SCRIP_AND_EXCHANGE")
