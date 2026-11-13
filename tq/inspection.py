import datetime
import logging
import os
import socket
import time
from dataclasses import dataclass
from sqlalchemy import or_, select, update
import signal
import threading

from .models import QueueState, Torrent
from .results import parse

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class FileInfo:
    availability: float
    index: int
    name: str
    piece_range: list[int]
    priority: int
    progress: float
    size: int


@dataclass(frozen=True)
class TorrentInfo:
    added_on: int
    amount_left: int
    auto_tmm: bool
    availability: float
    category: str
    comment: str
    completed: int
    completion_on: int
    connections_count: int
    connections_limit: int
    content_path: str
    created_by: str
    creation_date: int
    dl_limit: int
    dlspeed: int
    download_path: str
    downloaded: int
    downloaded_session: int
    eta: int
    f_l_piece_prio: bool
    force_start: bool
    has_metadata: bool
    hash: str
    inactive_seeding_time_limit: int
    infohash_v1: str
    infohash_v2: str
    last_activity: int
    magnet_uri: str
    max_inactive_seeding_time: int
    max_ratio: float
    max_seeding_time: int
    name: str
    num_complete: int
    num_incomplete: int
    num_leechs: int
    num_seeds: int
    piece_size: int
    pieces_have: int
    pieces_num: int
    popularity: float
    priority: int
    private: bool # Can be None if metadata hasn't been downloaded yet, but currently we only inspect torrents with metadata
    progress: float
    ratio: float
    ratio_limit: float
    root_path: str
    save_path: str
    seeding_time: int
    seeding_time_limit: int
    seen_complete: int
    seq_dl: bool
    share_limit_action: str
    share_limits_mode: str
    size: int
    state: str
    super_seeding: bool
    tags: str
    time_active: int
    total_size: int
    total_wasted: int
    tracker: str
    trackers_count: int
    up_limit: int
    uploaded: int
    uploaded_session: int
    upspeed: int
    reannounce_in: int


@dataclass(frozen=True)
class Snapshot:
    torrent: TorrentInfo
    files: list[FileInfo]
    meta: dict[str, object]
    is_final: bool




class InspectorError(RuntimeError):
    pass



def build_info(hash, meta, snapshot):
    return Snapshot(
        torrent=TorrentInfo(**{**snapshot['torrent'], 'hash': hash}),
        files=[FileInfo(**file) for file in snapshot['files']],
        is_final=snapshot['is_final'],
        meta=meta or {},
    )


class InspectWorker:
    def __init__(self, config, session, fn):
        self.config = config
        self.session = session
        self.fn = fn
        insp = self.config.get('inspection', {})
        self.lease = datetime.timedelta(seconds=insp['lease'])
        self.poll = insp.get('poll', 1.0)
        self.id = f"{socket.gethostname()}:{os.getpid()}"

        self._stop = threading.Event()
        self._held = None


    def run(self):
        def handle_signal(signum, frame):
            if self._stop.is_set():
                raise KeyboardInterrupt
            self._stop.set()
            logger.info("Finishing current inspect. Press Ctrl+C again to stop now.")

        self._stop.clear()
        old = {}
        if threading.current_thread() is not threading.main_thread():
            for sig in (signal.SIGINT, signal.SIGTERM):
                old[sig] = signal.signal(sig, handle_signal)

        logger.info(f"Inspect worker {self.id} started")

        try:
            while not self._stop.is_set():
                if not self.run_once():
                    self._stop.wait(self.poll)
        except KeyboardInterrupt:
            logger.warning(f"Stopping inspect worker {self.id}. Interrupted check will run again")
        finally:
            for sig, handler in old.items():
                signal.signal(sig, handler)
            if self._held is not None:
                try:
                    self._release(self._held)
                except Exception as e:
                    logger.warning(f"Failed to release claim on {self._held}. It will be available again when its lease expires")
                self._held = None
            logger.info(f"Inspect worker {self.id} stopped")



    def run_once(self):
        claimed = self._claim()
        if claimed is None:
            return False
        self._inspect(*claimed)
        return True

    def _claim(self):
        now = datetime.datetime.now()
        due = (
            select(Torrent.hash)
            .where(
                Torrent.queue_state == QueueState.ACTIVE,
                Torrent.snapshot_at.is_not(None),
                or_(Torrent.verdict_snapshot_at.is_(None), Torrent.verdict_snapshot_at < Torrent.snapshot_at),
                or_(Torrent.inspect_lease_until.is_(None), Torrent.inspect_lease_until < now),
            )
            .order_by(Torrent.snapshot_at)
            .limit(1)
            .scalar_subquery()
        )
        stmt = (
            update(Torrent)
            .where(Torrent.hash == due)
            .values(inspect_claimed_by=self.id, inspect_lease_until=now + self.lease)
            .returning(Torrent.hash, Torrent.snapshot, Torrent.snapshot_at, Torrent.meta)
            .execution_options(synchronize_session=False)
        )
        with self.session() as s:
            row = s.execute(stmt).one_or_none()
            s.commit()

        if row is None:
            return None
        self._held = row[0]
        return tuple(row)

    def _inspect(self, hash, snapshot, snapshot_at, meta):
        try:
            outcome = parse(self.fn(build_info(hash, meta, snapshot)))
        except Exception as e:
            self._release(hash)
            self._held = None
            name = snapshot['torrent'].get('name')
            raise InspectorError(f"Inspector failed on {hash} ({name}): {type(e).__name__}: {e}") from e

        written = self._write(hash, snapshot_at, outcome)
        self._held = None
        if not written:
            logger.warning(f"Lost the claim on {hash}; discarding {outcome.kind} verdict")

    def _write(self, hash, snapshot_at, outcome):
        now = datetime.datetime.now()
        values = dict(
            verdict=outcome.kind.value,
            reason=outcome.reason,
            verdict_snapshot_at=snapshot_at,
            next_check_at=now + datetime.timedelta(seconds=outcome.wait) if outcome.wait else None,
            inspect_claimed_by=None,
            inspect_lease_until=None,
        )
        if outcome.select is not None:
            values['select_files'] = list(outcome.select)
        stmt = (
            update(Torrent)
            .where(Torrent.hash == hash, Torrent.inspect_claimed_by == self.id, Torrent.queue_state == QueueState.ACTIVE)
            .values(**values)
            .execution_options(synchronize_session=False)
        )
        with self.session() as s:
            written = s.execute(stmt).rowcount == 1
            s.commit()
        return written

    def _release(self, hash):
        stmt = (
            update(Torrent)
            .where(Torrent.hash == hash, Torrent.inspect_claimed_by == self.id)
            .values(inspect_claimed_by=None, inspect_lease_until=None)
            .execution_options(synchronize_session=False)
        )
        with self.session() as s:
            s.execute(stmt)
            s.commit()