from abc import abstractmethod
from typing import Union

from ..core.logging import LoggerMixin
from ..integration.kite import KiteManager
from ..integration.common import TradeManager


class Service(LoggerMixin):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @abstractmethod
    def start(self):
        pass

    @staticmethod
    def get_args():
        raise NotImplementedError("get_args not implemented; needs to return options")

    @classmethod
    def create_service(cls):
        p = cls.get_args()
        if isinstance(p, tuple):
            p = p[0]
        return cls(**p.__dict__)


class TradeManagerService(Service):

    def __init__(self,
                 *args,
                 redis_server: str,
                 redis_port: int,
                 cache_path: str,
                 api_key: str=None,
                 api_secret: str=None,
                 request_token: str=None,
                 access_token: str=None,
                 instruments: Union[str, list]=None,
                 provider: str = "kite",
                 login_needed: bool = True,
                 **kwargs):

        super().__init__(*args, **kwargs)
        if isinstance(instruments, str):
            instruments = [{"scrip": value.split(":")[0], "exchange": value.split(":")[1]}
                           for value in instruments.split(",")]
        self.instruments = instruments

        if provider == "kite":
            self.manager = KiteManager(user_credentials={"API_KEY": api_key,
                                                        "API_SECRET": api_secret},
                                    cache_path=cache_path,
                                    redis_server=redis_server,
                                    redis_port=redis_port)
            if login_needed:
                res = self.manager.start_login()
                self.logger.info(f"Start Login Result {res}")
                if res is not None:
                    if request_token is not None:
                        self.manager.finish_login(request_token)
                    elif access_token is not None:
                        self.manager.kite.set_access_token(access_token)
                        self.manager.auth_state["access_token"] = access_token
        elif provider == "vanilla":
            self.manager = TradeManager(cache_path=cache_path,
                                        redis_server=redis_server,
                                        redis_port=redis_port)
        else:
            raise ValueError(f"Provider {provider} not found.")
        if self.manager is not None:
            self.manager.init()
