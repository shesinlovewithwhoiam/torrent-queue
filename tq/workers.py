import datetime
import logging
import os
import signal
import threading
import traceback
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional
from sqlalchemy import or_, select, update

from .models import File, QueueState, Torrent
from .results import parse, Kind

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class FileInfo:
    index: int
    name: str
    path: Path
    size: int
    progress: float


@dataclass(frozen=True)
class Snapshot:
    hash: str
    name: str
    progress: float
    is_final: bool
    files: list[FileInfo]
    meta: dict[str, object]


def build_info(hash, meta, snapshot, save_dir):
    torrent = snapshot['torrent']
    return Snapshot(
        hash=hash,
        name=torrent['name'],
        progress=torrent['progress'],
        is_final=snapshot['is_final'],
        files=[FileInfo(f['index'], f['name'], save_dir / f['name'], f['size'], f['progress']) for f in snapshot['files']],
        meta=meta or {},
    )



@dataclass(frozen=True)
class ProcessFile:
    file_id: int
    filename: str
    path: Path
    size: int


@dataclass(frozen=True)
class ProcessInfo:
    hash: str
    name: Optional[str]
    meta: Optional[dict]
    magnet: str
    files: list[ProcessFile] = field(default_factory=list)


class InspectorError(RuntimeError):
    pass




class Worker:
    section: str = 'worker'

    def __init__(self, config, session, fn):
        self.session = session
        self.fn = fn
        self.poll = config.get(self.section, {}).get('poll', 0.5)
        self.save_dir = Path(os.path.abspath(os.path.expanduser(config['qbit']['save_path'])))
        self._stop = threading.Event()

    def run(self):
        def handle_signal(signum, frame):
            if self._stop.is_set():
                raise KeyboardInterrupt
            self._stop.set()
            logger.info("Finishing current work. Press Ctrl+C again to stop now.")

        self._stop.clear()
        old = {}
        if threading.current_thread() is threading.main_thread():
            for sig in (signal.SIGINT, signal.SIGTERM):
                old[sig] = signal.signal(sig, handle_signal)

        logger.info(f"{type(self).__name__} started")

        try:
            self._start()
            while not self._stop.is_set():
                if not self.run_once():
                    self._stop.wait(self.poll)
        except KeyboardInterrupt:
            logger.warning("Stopping now. Interrupted work will run again")
        finally:
            for sig, handler in old.items():
                signal.signal(sig, handler)
            self._cleanup()
            logger.info(f"{type(self).__name__} stopped")

    def run_once(self):
        claimed = self._next()
        if claimed is None:
            return False
        self._process(claimed)
        return True

    def _start(self):
        pass

    def _cleanup(self):
        pass

    def _next(self):
        raise NotImplementedError

    def _process(self, item):
        raise NotImplementedError


class InspectWorker(Worker):
    section = 'inspection'

    def _start(self):
        stmt = (
            update(Torrent)
            .where(Torrent.queue_state == QueueState.ACTIVE)
            .values(next_check_at=datetime.datetime.now())
            .execution_options(synchronize_session=False)
        )
        with self.session() as s:
            s.execute(stmt)
            s.commit()

    def _next(self):
        stmt = (
            select(Torrent.hash, Torrent.snapshot, Torrent.snapshot_at, Torrent.meta)
            .where(
                Torrent.queue_state == QueueState.ACTIVE,
                Torrent.snapshot_at.is_not(None),
                or_(Torrent.action_snapshot_at.is_(None), Torrent.action_snapshot_at < Torrent.snapshot_at),
            )
            .order_by(Torrent.snapshot_at)
            .limit(1)
        )
        with self.session() as s:
            row = s.execute(stmt).one_or_none()
        return tuple(row) if row else None

    def _process(self, claimed):
        hash, snapshot, snapshot_at, meta = claimed
        try:
            outcome = parse(self.fn(build_info(hash, meta, snapshot, self.save_dir)))
        except Exception as e:
            name = snapshot['torrent'].get('name')
            raise InspectorError(f"Inspector failed on {hash} ({name}): {e}") from e

        if not self._write(hash, snapshot_at, outcome):
            logger.warning(f"{hash} changed while it was being checked. Discarding {outcome.kind} action")

    def _write(self, hash, snapshot_at, outcome):
        now = datetime.datetime.now()
        values = dict(
            action=outcome.kind.value,
            reason=outcome.reason,
            action_snapshot_at=snapshot_at,
            next_check_at=None if outcome.kind == Kind.WAIT_FOR_COMPLETION else now + datetime.timedelta(seconds=outcome.wait),
        )
        if outcome.select is not None:
            values['select_files'] = list(outcome.select)
        stmt = (
            update(Torrent)
            .where(Torrent.hash == hash, Torrent.queue_state == QueueState.ACTIVE, Torrent.snapshot_at == snapshot_at)
            .values(**values)
            .execution_options(synchronize_session=False)
        )
        with self.session() as s:
            written = s.execute(stmt).rowcount == 1
            s.commit()
        return written


class ProcessWorker(Worker):
    section = 'processing'

    def __init__(self, config, session, fn):
        super().__init__(config, session, fn)
        self._held = None

    def _start(self):
        stmt = (
            update(Torrent)
            .where(Torrent.queue_state == QueueState.PROCESSING)
            .values(queue_state=QueueState.FAILED, reason="Processor died while processing this torrent")
            .execution_options(synchronize_session=False)
        )
        with self.session() as s:
            died = s.execute(stmt).rowcount
            s.commit()
        if died:
            logger.warning(f"{died} torrent(s) were left in PROCESSING by an earlier run and are now FAILED")

    def _next(self):
        due = (
            select(Torrent.hash)
            .where(Torrent.queue_state == QueueState.READY)
            .order_by(Torrent.queue_position)
            .limit(1)
            .scalar_subquery()
        )
        stmt = (
            update(Torrent)
            .where(Torrent.hash == due, Torrent.queue_state == QueueState.READY)
            .values(queue_state=QueueState.PROCESSING)
            .returning(Torrent.hash, Torrent.name, Torrent.meta, Torrent.magnet)
            .execution_options(synchronize_session=False)
        )
        with self.session() as s:
            row = s.execute(stmt).one_or_none()
            s.commit()
            if row is None:
                return None
            self._held = row.hash
            files = s.scalars(select(File).where(File.hash == row.hash).order_by(File.file_id)).all()

        return ProcessInfo(
            hash=row.hash, name=row.name, meta=row.meta, magnet=row.magnet,
            files=[ProcessFile(f.file_id, f.filename, self.save_dir / f.filename, f.size) for f in files],
        )

    def _process(self, info):
        try:
            self.fn(info)
        except Exception as e:
            logger.exception(f"Processing failed: {info.name} ({info.hash})")
            reason = f"{type(e).__name__}: {e}".splitlines()[0]
            self._finish(info.hash, QueueState.FAILED, reason, traceback.format_exc())
        else:
            self._finish(info.hash, QueueState.DONE)

    def _finish(self, hash, state, reason=None, tb=None):
        stmt = (
            update(Torrent)
            .where(Torrent.hash == hash, Torrent.queue_state == QueueState.PROCESSING)
            .values(queue_state=state, reason=reason, traceback=tb)
            .execution_options(synchronize_session=False)
        )
        with self.session() as s:
            s.execute(stmt)
            s.commit()
        self._held = None

    def _cleanup(self):
        if self._held is None:
            return
        try:
            stmt = (
                update(Torrent)
                .where(Torrent.hash == self._held, Torrent.queue_state == QueueState.PROCESSING)
                .values(queue_state=QueueState.READY)
                .execution_options(synchronize_session=False)
            )
            with self.session() as s:
                s.execute(stmt)
                s.commit()
        except Exception:
            logger.warning(f"Failed to release {self._held}. It stays PROCESSING and will be marked FAILED on the next start")
        self._held = None