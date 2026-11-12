import logging
from collections.abc import Mapping

from .utils import load_toml

logger = logging.getLogger(__name__)


def make_sessionmaker(db_path):
    from sqlalchemy import create_engine, event
    from sqlalchemy.orm import sessionmaker
    engine = create_engine(f'sqlite:///{db_path}', echo=False, connect_args={'timeout': 30})

    @event.listens_for(engine, "connect")
    def set_sqlite_pragma(dbapi_connection, connection_record):
        cursor = dbapi_connection.cursor()
        cursor.execute("PRAGMA journal_mode=WAL")
        cursor.close()

    return sessionmaker(bind=engine, expire_on_commit=False), engine


class App:
    def __init__(self, config=None):
        self._source = config
        self._config = None
        self._database = None
        self._inspector = None
        self._processor = None

    @property
    def config(self):
        if self._config is None:
            if self._source is None:
                raise RuntimeError("App config not set. Use App(config) or pass --config")
            self._config = dict(self._source) if isinstance(self._source, Mapping) else load_toml(self._source)
        return self._config

    def use_config(self, source):
        self._source = source
        self._config = None
        self._database = None

    @property
    def sessionmaker(self):
        return self._open()[0]

    @property
    def engine(self):
        return self._open()[1]

    @property
    def has_inspector(self):
        return self._inspector is not None

    def _open(self):
        if self._database is None:
            self._database = make_sessionmaker(self.config['data']['db_path'])
        return self._database

    def inspector(self, fn=None):
        if not callable(fn):
            raise TypeError("use @app.inspector without parentheses")
        if self._inspector is not None and self._inspector is not fn:
            logger.warning(f"Replacing inspector {self._inspector.__name__!r} with {fn.__name__!r}")
        self._inspector = fn
        return fn

    def processor(self, fn=None):
        if not callable(fn):
            raise TypeError("use @app.processor without parentheses")
        if self._processor is not None and self._processor is not fn:
            logger.warning(f"Replacing processor {self._processor.__name__!r} with {fn.__name__!r}")
        self._processor = fn
        return fn


    def inspect_worker(self):
        from .inspection import InspectWorker
        if self._inspector is None:
            raise RuntimeError("Inspect worker ran but no inspector registered. Decorate a function with @app.inspector")
        return InspectWorker(self.config, self.sessionmaker, self._inspector)