import logging
from collections.abc import Mapping, Callable
from typing import TYPE_CHECKING

from .utils import load_toml


logger = logging.getLogger(__name__)


if TYPE_CHECKING:
    from .actions import Actions
    from .workers import Snapshot, ProcessInfo
    InspectorFn = Callable[["Snapshot"], "Actions"]
    ProcessorFn = Callable[["ProcessInfo"], None]


class Queue:
    def __init__(self, config=None):
        self._source = config
        self._config = None
        self._database = None
        self.inspector_fn = None
        self.processor_fn = None

    @property
    def config(self):
        if self._config is None:
            if self._source is None:
                raise RuntimeError("Config not set. Call tq.configure(...) in your module or pass --config")
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
        return self.inspector_fn is not None

    def _open(self):
        if self._database is None:
            self._database = make_sessionmaker(self.config['data']['db_path'])
        return self._database


    def inspect_worker(self):
        from .workers import InspectWorker
        if self.inspector_fn is None:
            raise RuntimeError("Inspect worker ran but no inspector registered. Decorate a function with @tq.inspector")
        return InspectWorker(self.config, self.sessionmaker, self.inspector_fn)

    def process_worker(self):
        from .workers import ProcessWorker
        if self.processor_fn is None:
            raise RuntimeError("Process worker ran but no processor registered. Decorate a function with @tq.processor")
        return ProcessWorker(self.config, self.sessionmaker, self.processor_fn)


# Global queue instance
queue = Queue()



def configure(source):
    queue.use_config(source)

def inspector(fn: "InspectorFn" = None) -> "InspectorFn":
    if not callable(fn):
        raise TypeError("Use @tq.inspector without parentheses")
    if queue.inspector_fn is not None and queue.inspector_fn is not fn:
        logger.warning(f"Replacing inspector {queue.inspector_fn.__name__!r} with {fn.__name__!r}")
    queue.inspector_fn = fn
    return fn

def processor(fn: "ProcessorFn" = None) -> "ProcessorFn":
    if not callable(fn):
        raise TypeError("Use @tq.processor without parentheses")
    if queue.processor_fn is not None and queue.processor_fn is not fn:
        logger.warning(f"Replacing processor {queue.processor_fn.__name__!r} with {fn.__name__!r}")
    queue.processor_fn = fn
    return fn


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


