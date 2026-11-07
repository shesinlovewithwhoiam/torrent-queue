from unittest import result
from sqlalchemy import String, ForeignKey, JSON, select, insert, func, and_, or_
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship
from typing import Optional, List
from dataclasses import dataclass, field
import enum
import datetime
import polars as pl
import os
import logging
import qbittorrentapi
import time
from .utils import AttrDict, get_hash
from concurrent.futures import ThreadPoolExecutor

logger = logging.getLogger(__name__)

validation_function = None
validation_interval = 0

def watcher(func=None, *, interval=0):
    def register(f):
        global validation_function, validation_interval
        validation_function = f
        validation_interval = interval
        return f

    if func is not None:
        return register(func)
    return register


class Base(DeclarativeBase):
    pass


class ClientState(enum.StrEnum):
    NONE = 'NONE'
    DOWNLOADING = 'DOWNLOADING'
    COMPLETED = 'COMPLETED'
    PAUSED = 'PAUSED'
    ERRORED = 'ERRORED'

class QueueState(enum.StrEnum):
    PENDING = 'PENDING' # not in client yet
    ACTIVE = 'ACTIVE' # in client and downloading
    READY = 'READY' # ready to process
    DONE = 'DONE' # processed and ready to delete
    STALLED = 'STALLED'
    INVALID = 'INVALID'


class WatcherResult(enum.StrEnum):
    INVALID = 'INVALID'
    VALID = 'VALID'
    COMPLETE = 'COMPLETE'


STATE_CHANGE_MAP = {
    QueueState.PENDING: {QueueState.ACTIVE},
    QueueState.ACTIVE: {QueueState.READY, QueueState.STALLED, QueueState.INVALID, QueueState.PENDING},
    QueueState.READY: {QueueState.DONE, QueueState.PENDING},
    QueueState.DONE: set(),
    QueueState.STALLED: set(),
    QueueState.INVALID: set(),
}

SHOULD_DELETE = {QueueState.DONE, QueueState.STALLED, QueueState.INVALID}


@dataclass
class ClientUpdate:
    add: list[tuple[str, str]] = field(default_factory=list)
    pause: list[str] = field(default_factory=list)
    resume: list[str] = field(default_factory=list)
    delete: list[str] = field(default_factory=list)


def set_queue_state(t, new):          # module-level so processing.py can use it too
    old = t.queue_state
    if new == old:
        return
    if new not in STATE_CHANGE_MAP[old]:
        raise ValueError(f"Illegal transition {old} -> {new} for {t}")
    logger.info(f"Queue state of {t}: {old} -> {new}")
    t.queue_state = new



class Torrent(Base):
    __tablename__ = "torrents"

    hash: Mapped[str] = mapped_column(String(40), primary_key=True)
    name: Mapped[Optional[str]]
    time_began: Mapped[Optional[datetime.datetime]]

    client_state: Mapped[ClientState] = mapped_column(default=ClientState.NONE)
    queue_state: Mapped[QueueState] = mapped_column(default=QueueState.PENDING)

    is_slow: Mapped[bool] = mapped_column(default=False)
    slow_since: Mapped[Optional[datetime.datetime]]
    dead_since: Mapped[Optional[datetime.datetime]]

    meta: Mapped[Optional[dict]] = mapped_column(JSON)
    claimed_at: Mapped[Optional[datetime.datetime]]
    magnet: Mapped[str] = mapped_column(unique=True)

    files: Mapped[List["File"]] = relationship(back_populates="torrent", cascade="all, delete-orphan")
    queue_position: Mapped[int] = mapped_column(unique=True)

    def __repr__(self):
        return f'Torrent(queue_position={self.queue_position!r}, name={self.name!r}, hash={self.hash!r})'




class File(Base):
    __tablename__ = "files"

    hash: Mapped[str] = mapped_column(ForeignKey("torrents.hash"), primary_key=True)
    file_id: Mapped[int] = mapped_column(primary_key=True)
    filename: Mapped[str]
    time_added: Mapped[datetime.datetime] = mapped_column(default=datetime.datetime.now)
    size: Mapped[int]

    torrent: Mapped["Torrent"] = relationship(back_populates="files")


class TorrentClient:
    def __init__(self, config):
        self.category = config.category
        self.sequential = config.get('sequential', False)

        client = qbittorrentapi.Client(
            host="localhost",
            port=config.port,
            username=config.username,
            password=config.password,
        )
        try:
            client.auth_log_in()
        except Exception as e:
            raise RuntimeError("Failed to connect to qBittorrent") from e

        os.makedirs(config.save_path, exist_ok=True)

        if self.category not in client.torrent_categories.categories:
            client.torrents_create_category(
                name=self.category,
                save_path=os.path.abspath(config.save_path),
            )

        self._client = client

    def torrents_info(self):
        return self._client.torrents_info(
            category=self.category,
            include_files=True,
        )

    def apply(self, updates):
        if updates.add:
            logger.info(f"Adding {len(updates.add)} torrents to client: {updates.add}")
            self._client.torrents_add(
                urls=[magnet for _, magnet in updates.add],
                category=self.category,
                use_auto_torrent_management=True,
                is_sequential_download=self.sequential,
                is_first_last_piece_priority=self.sequential,
            )
            self._client.torrents_set_share_limits(
                torrent_hashes=[torrent_hash for torrent_hash, _ in updates.add],
                ratio_limit=0,
                seeding_time_limit=0,
                inactive_seeding_time_limit=-1,
            )
        if updates.pause:
            logger.info(f"Pausing {len(updates.pause)} torrents in client: {updates.pause}")
            self._client.torrents_pause(torrent_hashes=updates.pause)
        if updates.resume:
            logger.info(f"Resuming {len(updates.resume)} torrents in client: {updates.resume}")
            self._client.torrents_resume(torrent_hashes=updates.resume)
        if updates.delete:
            logger.info(f"Removing {len(updates.delete)} torrents from client: {updates.delete}")
            self._client.torrents_delete(
                torrent_hashes=updates.delete,
                delete_files=True,
            )

    def refresh_interval(self):
        return self._client.app_preferences()['refresh_interval'] / 1000








class TorrentManager:
    def __init__(self, config, session):
        self.config = AttrDict(config)
        self.session = session

        self.client = TorrentClient(self.config.qbit)
        self.running = True
        self.last_checked = {}
        self.pending = {}
        self.pool = ThreadPoolExecutor(max_workers=self.config.management.get('watcher_workers', 2))



    def build_db(self, df, engine):
        logger.info(f"Rebuilding database...")

        assert 'queue_position' not in df.columns
        df = df.with_row_index('queue_position', offset=1)

        Base.metadata.drop_all(engine)
        Base.metadata.create_all(engine)
        self._insert_tasks(df)
        logger.info(f"Database built with {len(df)} torrents")

    
    def _insert_tasks(self, df):
        cols = df.columns

        if 'magnet' not in cols:
            raise ValueError("CSV data DataFrame must contain a 'magnet' column with magnet links")
        if 'hash' not in cols:
            logger.info("No 'hash' column found in CSV data. Computing hashes from magnet links...")
            df = df.with_columns(
                pl.col('magnet').map_elements(get_hash, return_dtype=pl.String).alias('hash')
            )
            logger.info("Hashes computed and added to DataFrame")


        df = df.with_columns(
            pl.struct(pl.all().exclude(['hash', 'magnet'])).alias('meta')
        )
        rows = df.to_dicts()
        with self.session() as session:
            session.execute(insert(Torrent), rows)
            session.commit()



    def update(self):
        try:
            while self.running:
                self._tick()
                time.sleep(self.client.refresh_interval())
        finally:
            self.pool.shutdown(wait=False, cancel_futures=True)

    def _tick(self):
        info = self.client.torrents_info()
        updates = self._update_state(info)
        self.client.apply(updates)

    def _update_state(self, info):
        updates = ClientUpdate()
        with self.session() as session:
            in_client = set()
            for t in info:
                in_client.add(t.hash)
                db_t = session.get(Torrent, t.hash)
                if db_t is None:
                    continue

                self._set_name(db_t, t)
                self._update_client_state(db_t, t)

                if db_t.queue_state == QueueState.ACTIVE:
                    self._handle_queued(session, db_t, t, updates)
                
                if db_t.queue_state in SHOULD_DELETE:
                    logger.info(f"Deleting ({db_t.queue_state}): {db_t}")
                    updates.delete.append(db_t.hash)
            
            self._reset_removed(session, in_client)
            self._limit(session, updates)
            session.commit()

        return updates

    def _handle_queued(self, session, db_t, t, updates):
        result = self._validate(db_t, t)
        if result == WatcherResult.INVALID:
            set_queue_state(db_t, QueueState.INVALID)
        elif result == WatcherResult.COMPLETE or db_t.client_state == ClientState.COMPLETED:
            self._mark_ready(session, db_t, t, updates)
        elif self._is_dead(db_t, t):
            set_queue_state(db_t, QueueState.STALLED)
        else:
            self._set_if_slow(db_t, t)

    def _validate(self, db_t, t):
        if validation_function is None or t.size == 0 or not t.files:
            return WatcherResult.VALID

        fut = self.pending.get(db_t.hash)
        if fut is not None:
            if not fut.done():
                return WatcherResult.VALID
            del self.pending[db_t.hash]
            self.last_checked[db_t.hash] = datetime.datetime.now()

            try:
                result = fut.result()
            except Exception as e:
                logger.exception(f"Validator failed for {db_t}")
                raise RuntimeError(f"Validator failed for {db_t}") from e
            if isinstance(result, bool):
                return WatcherResult.VALID if result else WatcherResult.INVALID
            return WatcherResult(result)

        last = self.last_checked.get(db_t.hash)
        if db_t.client_state == ClientState.PAUSED and last is not None:
            return WatcherResult.VALID
        if validation_interval and last is not None and (datetime.datetime.now() - last).total_seconds() < validation_interval:
            return WatcherResult.VALID
        self.pending[db_t.hash] = self.pool.submit(validation_function, t)
        return WatcherResult.VALID

    def _mark_ready(self, session, db_t, t, updates):
        self._init_files(session, db_t, t.files)
        set_queue_state(db_t, QueueState.READY)
        db_t.is_slow, db_t.slow_since, db_t.dead_since = False, None, None
        if db_t.client_state != ClientState.COMPLETED:     # early COMPLETE: stop the client writing
            updates.pause.append(db_t.hash)

    def _update_client_state(self, db_t, torrent):
        state = torrent.state_enum
        if state.is_errored:
            new = ClientState.ERRORED
        elif state.is_complete:
            new = ClientState.COMPLETED
        elif state.is_stopped:
            new = ClientState.PAUSED
        else:
            new = ClientState.DOWNLOADING

        if db_t.client_state != new:
            logger.info(f"Client state of {db_t}: {db_t.client_state} -> {new}")
            db_t.client_state = new

        # torrent was just added to client
        if db_t.queue_state == QueueState.PENDING:
            set_queue_state(db_t, QueueState.ACTIVE)
            db_t.time_began = datetime.datetime.now()

    def _set_name(self, db_t, t):
        if db_t.name != t.name:
            logger.info(f'Updating name for {db_t.hash}: {db_t.name} -> {t.name}')
            db_t.name = t.name
            

    def _set_if_slow(self, db_t, t):
        if db_t.client_state != ClientState.DOWNLOADING:
            db_t.slow_since = None
            db_t.is_slow = False
            return
        
        speed = t.dlspeed / 1000**2 # B/s -> MB/s
        now = datetime.datetime.now()

        if speed < self.config.management.slow_cls.slow_threshold:
            if db_t.slow_since is None:
                db_t.slow_since = now
            elif not db_t.is_slow and (now - db_t.slow_since).total_seconds() >= self.config.management.slow_cls.min_time:
                db_t.is_slow = True
                # self.client.torrents_add_tags(tags='slow', torrent_hashes=db_t.hash)
                logger.info(f"Slowed: {db_t}")
        elif speed > self.config.management.slow_cls.unslow_threshold:
            if db_t.is_slow:
                logger.info(f"Recovered: {db_t}")
            db_t.slow_since = None
            db_t.is_slow = False
            # self.client.torrents_remove_tags(tags='slow', torrent_hashes=db_t.hash)


    def _is_dead(self, db_t, t):
        if db_t.client_state != ClientState.DOWNLOADING:
            db_t.dead_since = None
            return False
        
        now = datetime.datetime.now()

        if t.dlspeed == 0:
            if db_t.dead_since is None:
                db_t.dead_since = now
            elif (now - db_t.dead_since).total_seconds() >= self.config.management.dead_cls.min_time:
                logger.info(f"Stalled (dead): {db_t}")
                db_t.dead_since = None
                db_t.is_slow = False    
                db_t.slow_since = None
                return True
        else:
            db_t.dead_since = None

        return False



    # main queue management logic
    def _limit(self, session, updates):
        active = self._count_active(session)
        maxa = self.config.management.max_active

        diff = maxa - active

        if diff > 0:
            stmt = (
                select(Torrent)
                .where(
                    Torrent.queue_state == QueueState.ACTIVE,
                    Torrent.client_state == ClientState.PAUSED,
                )
                .order_by(Torrent.time_began)
                .limit(diff)
            )

            updates.resume.extend(t.hash for t in session.scalars(stmt))
            num_to_add = diff - len(updates.resume)

            if num_to_add > 0:
                stmt = (
                    select(Torrent)
                    .where(Torrent.queue_state == QueueState.PENDING)
                    .order_by(Torrent.queue_position) # TODO: add ability for custom ordering
                    .limit(num_to_add)
                )
                updates.add.extend((t.hash, t.magnet) for t in session.scalars(stmt))
        elif diff < 0:
            stmt = (
                select(Torrent)
                .where(
                    Torrent.queue_state == QueueState.ACTIVE,
                    Torrent.client_state == ClientState.DOWNLOADING,
                    ~Torrent.is_slow
                )
                .order_by(Torrent.time_began.desc())
                .limit(abs(diff))
            )
            updates.pause.extend(t.hash for t in session.scalars(stmt))



    def _count_active(self, session):
        stmt = select(func.count()).select_from(Torrent).where(
            or_(
                and_(
                    Torrent.queue_state == QueueState.ACTIVE,
                    Torrent.client_state == ClientState.DOWNLOADING,
                    ~Torrent.is_slow
                ),
                Torrent.queue_state == QueueState.READY
            )
        )
        return session.scalar(stmt)


    def _reset_removed(self, session, in_client):
            stmt = select(Torrent).where(
                Torrent.queue_state.in_([QueueState.ACTIVE, QueueState.READY])
            )
            in_db_torrents = list(session.scalars(stmt))

            to_remove = {t.hash for t in in_db_torrents} - in_client


            if to_remove:
                to_remove_torrents = [t for t in in_db_torrents if t.hash in to_remove]
                logger.info(f"Resetting and deprioritizing unprocessed torrents removed from client: {to_remove_torrents}")

                max_pos = session.scalar(select(func.max(Torrent.queue_position)))

                for i, t in enumerate(to_remove_torrents, start=1):
                    set_queue_state(t, QueueState.PENDING)
                    t.client_state = ClientState.NONE
                    t.time_began = None
                    t.is_slow = False
                    t.slow_since = None
                    t.dead_since = None
                    t.claimed_at = None
                    t.files.clear()
                    logger.info(f"Deprioritizing removed torrent: {t}. Queue position: {t.queue_position} -> {max_pos + i}")
                    t.queue_position = max_pos + i


    def _init_files(self, session, torrent, files):
        to_insert = [
            {
                'hash': torrent.hash,
                'file_id': f.index,
                'filename': f.name,
                'size': f.size,
            } 
            for f in files
        ]


        logger.info(f"Initializing {len(files)} files for torrent {torrent}")
        session.execute(insert(File), to_insert)