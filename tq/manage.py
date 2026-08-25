from sqlalchemy import String, ForeignKey, JSON, select, insert, func
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship
from typing import Optional, List
import enum
import datetime
import polars as pl
import os
import logging
import qbittorrentapi
import time
from .utils import AttrDict, get_hash

logger = logging.getLogger(__name__)

validation_function = None

def validator(func):
    global validation_function
    validation_function = func
    return func


class Base(DeclarativeBase):
    pass

class TorrentStatus(enum.StrEnum):
    PENDING = "PENDING"
    DOWNLOADING = "DOWNLOADING"
    COMPLETED = "COMPLETED"
    PAUSED = "PAUSED"
    ERRORED = "ERRORED"
    STALLED = "STALLED"
    INVALID = "INVALID"




class Torrent(Base):
    __tablename__ = "torrents"

    hash: Mapped[str] = mapped_column(String(40), primary_key=True)
    name: Mapped[Optional[str]]
    status: Mapped[TorrentStatus] = mapped_column(default=TorrentStatus.PENDING)
    time_began: Mapped[Optional[datetime.datetime]]
    is_slow: Mapped[bool] = mapped_column(default=False)
    slow_since: Mapped[Optional[datetime.datetime]]
    dead_since: Mapped[Optional[datetime.datetime]]

    meta: Mapped[Optional[dict]] = mapped_column(JSON)
    claimed_at: Mapped[Optional[datetime.datetime]]
    files_processed: Mapped[bool] = mapped_column(default=False)
    metadata_checked: Mapped[bool] = mapped_column(default=False)
    magnet: Mapped[str] = mapped_column(unique=True)

    files: Mapped[List["File"]] = relationship(back_populates="torrent")
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



class TorrentManager:
    def __init__(self, config, session):
        self.config = AttrDict(config)
        self.session = session

        client = qbittorrentapi.Client(
            host="localhost", 
            port=self.config.qbit.port, 
            username=self.config.qbit.username, 
            password=self.config.qbit.password, 
        )
        try:
            client.auth_log_in()
        except Exception as e:
            raise RuntimeError("Failed to connect to qBittorrent") from e

        os.makedirs(self.config.qbit.save_path, exist_ok=True)

        if self.config.qbit.category not in client.torrent_categories.categories:
            client.torrents_create_category(name=self.config.qbit.category, save_path=os.path.abspath(self.config.qbit.save_path))

        self.client = client
        self.running = True



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
        while self.running:
            info = self.client.torrents_info(category=self.config.qbit.category, include_files=True)
            to_add, to_pause, to_resume, to_del = self._update_state(info)

            self._apply_state(to_add, to_pause, to_resume, to_del)

            time.sleep(self.client.app_preferences()['refresh_interval'] / 1000)

    def _apply_state(self, to_add, to_pause, to_resume, to_del):
        if to_add:
            logger.info(f"Adding {len(to_add)} torrents to client: {to_add}")
            self.client.torrents_add(urls=[t.magnet for t in to_add], category=self.config.qbit.category, use_auto_torrent_management=True)
            self.client.torrents_set_share_limits(torrent_hashes=[t.hash for t in to_add], ratio_limit=0, seeding_time_limit=0, inactive_seeding_time_limit=-1)
        if to_pause:
            logger.info(f"Pausing {len(to_pause)} torrents in client: {to_pause}")
            self.client.torrents_pause(torrent_hashes=[t.hash for t in to_pause])
        if to_resume:
            logger.info(f"Resuming {len(to_resume)} torrents in client: {to_resume}")
            self.client.torrents_resume(torrent_hashes=[t.hash for t in to_resume])
        if to_del:
            logger.info(f"Removing {len(to_del)} torrents from client: {to_del}")
            self.client.torrents_delete(torrent_hashes=[t.hash for t in to_del], delete_files=True)


    def _update_state(self, info):
        with self.session() as session:
            in_client = set()
            to_delete = []
            new_completions = False
            for t in info:
                in_client.add(t.hash)

                db_t = session.get(Torrent, t.hash)
                if db_t is None:
                    continue

                self._set_name(db_t, t)

                if self._check_invalid(db_t, t):
                    logger.info(f"Deleting (invalid): {db_t}")
                    to_delete.append(db_t) # invalidate
                    continue

                og_status = db_t.status
                self._update_status(session, db_t, t)

                if db_t.status == TorrentStatus.COMPLETED and og_status != TorrentStatus.COMPLETED:
                    new_completions = True

                if db_t.status == TorrentStatus.COMPLETED and db_t.files_processed:
                    logger.info(f"Deleting (complete): {db_t}")
                    to_delete.append(db_t) # complete
                    continue

                if self._is_dead(db_t, t):
                    logger.info(f"Deleting (dead): {db_t}")
                    to_delete.append(db_t) # dead
                else:
                    self._set_if_slow(db_t, t)
            
            self._reset_removed(session, in_client)
            to_add, to_pause, to_resume = self._limit(session)
            session.commit()

        return to_add, to_pause, to_resume, to_delete

    def _set_name(self, db_t, t):
        if db_t.name != t.name:
            logger.info(f'Updating name for {db_t.hash}: {db_t.name} -> {t.name}')
            db_t.name = t.name
            
    
    def _check_invalid(self, db_t, t):
        if db_t.status == TorrentStatus.INVALID:
            return False
        if db_t.metadata_checked:
            return False
        if t.size == 0 or t.files is None:
            return False


        db_t.metadata_checked = True

        if validation_function is None:
            return False

        if not validation_function(t):
            logger.info(f"Invalid torrent {db_t}. Validation function {validation_function.__name__} returned False")
            db_t.status = TorrentStatus.INVALID
            return True
        
        return False



    def _set_if_slow(self, db_t, t):
        if db_t.status != TorrentStatus.DOWNLOADING:
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
        if db_t.status != TorrentStatus.DOWNLOADING:
            db_t.dead_since = None
            return False
        
        now = datetime.datetime.now()

        if t.dlspeed == 0:
            if db_t.dead_since is None:
                db_t.dead_since = now
            elif (now - db_t.dead_since).total_seconds() >= self.config.management.dead_cls.min_time:
                logger.info(f"Stalled (dead): {db_t}")
                db_t.status = TorrentStatus.STALLED
                db_t.dead_since = None
                db_t.is_slow = False    
                db_t.slow_since = None
                return True
        else:
            db_t.dead_since = None

        return False



    # main queue management logic
    def _limit(self, session):
        active = self._count_active(session)
        maxa = self.config.management.max_active

        to_add, to_pause, to_resume = [], [], []
        diff = maxa - active

        if diff > 0:
            stmt = (
                select(Torrent)
                .where(Torrent.status == TorrentStatus.PAUSED)
                .order_by(Torrent.time_began)
                .limit(diff)
            )

            to_resume = list(session.scalars(stmt))
            num_to_add = diff - len(to_resume)

            if num_to_add > 0:
                stmt = (
                    select(Torrent)
                    .where(Torrent.status == TorrentStatus.PENDING)
                    .order_by(Torrent.queue_position) # TODO: add ability for custom ordering
                    .limit(num_to_add)
                )
                to_add = list(session.scalars(stmt))
        elif diff < 0:
            stmt = (
                select(Torrent)
                .where(
                    Torrent.status == TorrentStatus.DOWNLOADING, 
                    ~Torrent.is_slow
                )
                .order_by(Torrent.time_began.desc())
                .limit(abs(diff))
            )
            to_pause = list(session.scalars(stmt))


        return to_add, to_pause, to_resume



    def _count_active(self, session):
        stmt = select(func.count()).select_from(Torrent).where(
            Torrent.status.in_([TorrentStatus.DOWNLOADING, TorrentStatus.COMPLETED]),
            ~Torrent.is_slow,
            ~Torrent.files_processed
        )
        return session.scalar(stmt)


    def _reset_removed(self, session, in_client):
            stmt = select(Torrent).where(
                Torrent.status.not_in([TorrentStatus.PENDING, TorrentStatus.STALLED, TorrentStatus.INVALID]),
                ~Torrent.files_processed
            )
            in_db_torrents = list(session.scalars(stmt))

            to_remove = {t.hash for t in in_db_torrents} - in_client


            if to_remove:
                to_remove_torrents = [t for t in in_db_torrents if t.hash in to_remove]
                logger.info(f"Resetting and deprioritizing unprocessed torrents removed from client: {to_remove_torrents}")

                max_pos = session.scalar(select(func.max(Torrent.queue_position)))

                for i, t in enumerate(to_remove_torrents, start=1):
                    t.status = TorrentStatus.PENDING
                    t.time_began = None
                    t.is_slow = False
                    t.slow_since = None
                    t.dead_since = None
                    t.files_processed = False
                    t.claimed_at = None
                    logger.info(f"Deprioritizing removed torrent: {t}. Queue position: {t.queue_position} -> {max_pos + i}")
                    t.queue_position = max_pos + i

    def _update_status(self, session, db_t, torrent):
        enum = torrent.state_enum
        if enum.is_errored:
            new_status = TorrentStatus.ERRORED
        elif enum.is_complete:
            new_status = TorrentStatus.COMPLETED
        elif enum.is_stopped:
            new_status = TorrentStatus.PAUSED
        else:
            new_status = TorrentStatus.DOWNLOADING

        if db_t.status != new_status:
            og = db_t.status
            logger.info(f"Status of {db_t} changed from {og} to {new_status}")
            db_t.status = new_status

            if new_status == TorrentStatus.DOWNLOADING and db_t.time_began is None:
                db_t.time_began = datetime.datetime.now()
            elif new_status == TorrentStatus.COMPLETED:
                db_t.is_slow = False
                db_t.slow_since = None
                db_t.dead_since = None

                files = torrent.files
                assert files 

                self._init_files(session, db_t, files)


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




