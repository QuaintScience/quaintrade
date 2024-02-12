"""Base Logger Classes"""

from __future__ import annotations
from abc import ABC, abstractmethod
from typing import Union
from enum import Enum
import os
import platform
import datetime
import logging


WINDOWS = "windows"


class LogLevel(Enum):
    """Log Levels"""

    DEBUG = "debug"
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


class Handler:  # pylint: disable=too-few-public-methods
    """Handler Base"""

    def __init__(self,
                 handler_def=None,
                 formatter_def=None):
        self.handler_def = handler_def
        self.formatter_def = formatter_def


class Logger(ABC):  # pylint: disable=too-many-instance-attributes
    """Logger Base"""

    def __init__(self, name,  # pylint: disable=too-many-arguments
                 *args,
                 default_log_level=None,
                 handlers=None,
                 default_opts=None,
                 default_context=None, **kwargs):
        if default_log_level is None:
            default_log_level = LogLevel.DEBUG
        if isinstance(default_log_level, str):
            default_log_level = LogLevel[default_log_level]
        if default_opts is None:
            default_opts = {}
        if default_context is None:
            default_context = {}
        if handlers is None:
            handlers = []
        self.name = name
        self.default_log_level = default_log_level
        self.handlers = handlers
        self.default_opts = default_opts
        self.default_context = default_context
        self.init()
        self.post_init()
        super().__init__(*args, **kwargs)

    def init(self) -> Logger:
        """Initialize Logger"""
        self.reset_opts()
        self.reset_context()
        self.system_context = {}
        self.update_system_context()
        return self

    def update_system_context(self):
        """Update system context variables"""
        if platform.system() == WINDOWS:
            self.system_context["hostname"] = platform.uname().node
        else:
            self.system_context["hostname"] = os.uname().nodename

    def post_init(self):
        """Post init work"""
        self.reset_log_level()

    @abstractmethod
    def set_log_level(self, log_level: LogLevel) -> Logger:
        """Set log level"""
        pass  # pylint: disable=unnecessary-pass

    def reset_log_level(self) -> Logger:
        """Reset log level"""
        self.set_log_level(self.default_log_level)
        return self

    @abstractmethod
    def add_handler(self, handler: Handler) -> Logger:
        """Add handler"""
        pass  # pylint: disable=unnecessary-pass

    @abstractmethod
    def remove_handler(self, handler: Handler) -> Logger:
        """Remove handler"""
        pass  # pylint: disable=unnecessary-pass

    @abstractmethod
    def log_impl(self, typ: str, msg: str, *args, **kwargs) -> Logger:
        """Log function implementation"""
        pass  # pylint: disable=unnecessary-pass

    @property
    def native_object(self) -> object:
        """Return native (internal) logging object"""

        if hasattr(self, "_logger"):
            # pylint: disable=no-member
            return self._logger   # pylint: disable=unnecessary-pass
        return None

    def set_context(self, context: dict, update=True) -> Logger:
        """Set context"""
        if update:
            self.context.update(context)
        else:
            self.context = context  # pylint: disable=attribute-defined-outside-init
        return self

    def reset_context(self) -> Logger:
        """Reset context"""

        self.context = self.default_context  # pylint: disable=attribute-defined-outside-init
        return self

    def apply_opts(self) -> Logger:
        """Apply options"""

        return self

    def set_opts(self, opts: dict,
                 update: bool = True) -> Logger:  # pylint: disable=unused-argument
        """Set options"""
        self.opts = opts  # pylint: disable=attribute-defined-outside-init
        self.apply_opts()
        return self

    def reset_opts(self) -> Logger:
        """Reset opts"""

        self.opts = self.default_opts  # pylint: disable=attribute-defined-outside-init
        self.apply_opts()
        return self

    def log(self, typ: str,
            msg: str,
            *args,
            opts: dict = None,
            context: dict = None,
            **kwargs) -> Logger:
        """Log a message"""

        if context is None:
            context = {}
        if opts is None:
            opts = {}
        curr_opts = self.opts
        self.set_opts(opts)
        context.update(self.context)
        self.log_impl(typ,
                      msg,
                      *args, **kwargs)
        self.set_opts(curr_opts)
        return self

    def debug(self, msg: str,
              *args,
              opts: dict = None,
              context: dict = None,
              **kwargs) -> Logger:

        """Log a debug message"""
        self.log("debug", msg, *args, opts=opts, context=context, **kwargs)
        return self

    def info(self, msg: str,
             *args,
             opts: dict = None,
             context: dict = None,
             **kwargs) -> Logger:
        """Log an info message"""

        self.log("info", msg, *args, opts=opts, context=context, **kwargs)
        return self

    def warn(self, msg: str,
             *args,
             opts: dict = None,
             context: dict = None,
             **kwargs) -> Logger:
        """Log a warning message"""

        self.log("warn", msg, *args, opts=opts, context=context, **kwargs)
        return self

    def error(self, msg: str,
              *args,
              opts: dict = None,
              context: dict = None,
              **kwargs) -> Logger:
        """Log an error message"""

        self.log("error", msg, *args, opts=opts, context=context, **kwargs)
        return self

    def critical(self, msg: str,
                 *args,
                 opts: dict = None,
                 context: dict = None,
                 **kwargs) -> Logger:
        """Log a critical message"""
        self.log("critical", msg, *args, opts=opts, context=context, **kwargs)
        return self


class DefaultLoggerNames(Enum):
    """List of default logger channels"""

    DEV = "DEV"
    ML = "ML"
    AUD = "AUD"
    APP = "APP"
    AUDIT = "AUD"


class LogBundle:
    """Logbundle -- which contains a bundle of loggers"""

    LOGGER_OBJECT_CACHE = {}

    def __init__(self, loggers=None):
        if loggers is None:
            loggers = {}
        self.loggers = loggers

    def register_logger(self, name: str,
                        logger: Logger) -> LogBundle():
        """Register a logger"""
        self.loggers[name] = logger
        return self

    def apply_to_loggers(self,
                         func,
                         args=None,
                         kwargs=None,
                         name=None) -> LogBundle:
        """Apply func to loggers"""
        if args is None:
            args = []
        if kwargs is None:
            kwargs = {}
        if name is None:
            for key in self.loggers.keys():
                self.apply_to_loggers(func=func,
                                      kwargs=kwargs,
                                      name=key)
        else:
            if isinstance(name, DefaultLoggerNames):
                name = name.name
            getattr(self.loggers[name], func)(*args, **kwargs)
        return self

    def set_context(self, context: dict, update=True, name=None) -> LogBundle:
        """Set context"""
        self.apply_to_loggers(func="set_context", name=name,
                              kwargs={"context": context,
                                      "update": update})
        return self

    def reset_context(self, name=None) -> LogBundle:
        """Reset context"""
        self.apply_to_loggers(func="reset_context", name=name)
        return self

    def set_opts(self, opts: dict, update=True, name=None) -> LogBundle:
        """Set opts"""
        self.apply_to_loggers(func="set_opts", name=name,
                              kwargs={"opts": opts,
                                      "update": update})
        return self

    def reset_opts(self, name=None) -> LogBundle:
        """Reset opts"""
        self.apply_to_loggers(func="reset_opts", name=name)
        return self

    def log(self, typ: str, name: Union[str, list],  # pylint: disable=too-many-arguments
            msg: str,
            opts: dict,
            context: dict,
            *args, **kwargs) -> LogBundle:
        """Log something"""
        if name is None:
            name = list(self.loggers.keys())
        if isinstance(name, DefaultLoggerNames):
            name = name.name
        if isinstance(name, str):
            name = [name]
        for logn in name:
            logger = self.loggers.get(logn)
            if logger is None:
                continue
            getattr(logger, typ)(msg,
                                 *args,
                                 opts=opts,
                                 context=context,
                                 **kwargs)
        return self

    def debug(self, name: Union[str, list],
              msg: str,
              *args,
              opts: dict = None,
              context: dict = None,
              **kwargs) -> LogBundle:
        """Log debug msg"""
        self.log("debug", name, msg, opts, context, *args, **kwargs)
        return self

    def info(self, name: Union[str, list],
             msg: str,
             *args,
             opts: dict = None,
             context: dict = None,
             **kwargs) -> LogBundle:
        """Log info msg"""
        self.log("info", name, msg, opts, context, *args, **kwargs)
        return self

    def warn(self, name: Union[str, list],
             msg: str,
             *args,
             opts: dict = None,
             context: dict = None,
             **kwargs) -> LogBundle:
        """Log warn msg"""

        self.log("warn", name, msg, opts, context, *args, **kwargs)
        return self

    def error(self, name: Union[str, list],
              msg: str,
              *args,
              opts: dict = None,
              context: dict = None,
              **kwargs) -> LogBundle:
        """Log errormsg"""

        self.log("error", name, msg, opts, context, *args, **kwargs)
        return self

    def critical(self,
                 name: Union[str, list], msg: str,
                 *args,
                 opts: dict = None,
                 context: dict = None,
                 **kwargs) -> LogBundle:
        """Log critical msg"""
        self.log("critical", name, msg, opts, context, *args, **kwargs)
        return self


class LoggerMixin:  # pylint: disable=too-few-public-methods
    """Logger Mixin Class"""

    def __init__(self, *args, logger=None, **kwargs):  # pylint: disable=unused-argument
        if logger is None:
            logger = DefaultPythonLogger("")
        self.logger = logger


class DefaultPythonLogger(Logger):
    """Default Python Logger"""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def init(self):
        super().init()
        self._logger = logging.getLogger()
        for handler in self.handlers:
            self.add_handler(handler)

    def add_handler(self, handler: Handler) -> Logger:
        (py_handler,
         py_formatter) = handler.construct(system_context=self.system_context)
        py_handler.setFormatter(py_formatter)
        self._logger.addHandler(py_handler)
        return self

    def remove_handler(self, handler: Handler) -> Logger:
        raise NotImplementedError("remove_handler is not supported.")

    @staticmethod
    def translate(log_level: LogLevel) -> int:
        """Translate log level to internal python logger loglevel"""
        return getattr(logging, log_level.name)

    def set_log_level(self, log_level: LogLevel) -> Logger:
        self._logger.setLevel(DefaultPythonLogger.translate(log_level))
        return self

    def log_impl(self, typ: str, msg: str, *args, **kwargs):
        logger = logging.LoggerAdapter(self._logger, self.context)
        if typ == "warn":
            typ = "warning"  # Python deprecation of warn
<<<<<<< HEAD
        print(f"LOG [{self.__class__.__name__}] [{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}]| {typ}: {msg}", flush=True)
=======
        print(f"LOG [{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}]| {typ}: {msg}", flush=True)
>>>>>>> 1e314e13b6fa1d64fdc5ea31562aa7266bece468
        #getattr(logger, typ)(msg)
        return self
