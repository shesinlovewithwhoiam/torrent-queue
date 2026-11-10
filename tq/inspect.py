import datetime
import logging
import os
import socket
import time
from sqlalchemy import or_, select, update
from .models import QueueState, Torrent
from .results import parse

logger = logging.getLogger(__name__)



_inspector = None


def inspector(fn):
    global _inspector
    _inspector = fn
    return fn


def get_inspector():
    return _inspector


class InspectorError(RuntimeError):
    pass


def build_info(hash, meta, snapshot):
    return {
        **snapshot['torrent'],
        'hash': hash,
        'files': snapshot['files'],
        'is_final': snapshot['is_final'],
        'meta': meta or {},
    }


class InspectWorker:
    def __init__(self, config, session, fn=None):
        self.config = config
        self.session = session
        self.fn = fn or get_inspector()
        if self.fn is None:
            raise RuntimeError("No inspector registered: decorate a function with @inspector")
        insp = self.config.get('inspection', {})
        self.lease = datetime.timedelta(seconds=insp.get('lease', 600))
        self.idle = insp.get('idle', 1.0)
        self.id = f"{socket.gethostname()}:{os.getpid()}"

    def run(self):
        logger.info(f"Inspect worker {self.id} started")
        while True:
            if not self.run_once():
                time.sleep(self.idle)

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
        return None if row is None else tuple(row)

    def _inspect(self, hash, snapshot, snapshot_at, meta):
        try:
            outcome = parse(self.fn(build_info(hash, meta, snapshot)))
        except Exception as e:
            self._release(hash)
            name = snapshot['torrent'].get('name')
            raise InspectorError(f"Inspector failed on {hash} ({name}): {type(e).__name__}: {e}") from e

        if not self._write(hash, snapshot_at, outcome):
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