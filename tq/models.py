import datetime
import enum
import logging
from typing import List, Optional

from sqlalchemy import ForeignKey, JSON, String
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship

logger = logging.getLogger(__name__)


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
    PROCESSING = 'PROCESSING'
    DONE = 'DONE' # processed and ready to delete
    STALLED = 'STALLED'
    INVALID = 'INVALID'
    FAILED = 'FAILED' # failed to process, but not invalid



class Torrent(Base):
    __tablename__ = "torrents"

    hash: Mapped[str] = mapped_column(String(40), primary_key=True)
    name: Mapped[Optional[str]]
    magnet: Mapped[str] = mapped_column(unique=True)
    queue_position: Mapped[int] = mapped_column(unique=True)

    client_state: Mapped[ClientState] = mapped_column(default=ClientState.NONE)
    queue_state: Mapped[QueueState] = mapped_column(default=QueueState.PENDING)
    time_began: Mapped[Optional[datetime.datetime]]
    reason: Mapped[Optional[str]]

    is_slow: Mapped[bool] = mapped_column(default=False)
    slow_since: Mapped[Optional[datetime.datetime]]
    dead_since: Mapped[Optional[datetime.datetime]]

    claimed_at: Mapped[Optional[datetime.datetime]]

    # inspector
    next_check_at: Mapped[Optional[datetime.datetime]]
    snapshot: Mapped[Optional[dict]] = mapped_column(JSON)
    snapshot_at: Mapped[Optional[datetime.datetime]]

    verdict: Mapped[Optional[str]] # TODO: rich return
    verdict_snapshot_at: Mapped[Optional[datetime.datetime]] # matches snapshot_at value function ran on

    inspect_claimed_by: Mapped[Optional[str]] # worker
    inspect_lease_until: Mapped[Optional[datetime.datetime]]


    # TODO: processor


    meta: Mapped[Optional[dict]] = mapped_column(JSON)
    files: Mapped[List["File"]] = relationship(back_populates="torrent", cascade="all, delete-orphan")



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


STATE_CHANGE_VALID = {
    QueueState.PENDING: {QueueState.ACTIVE},
    QueueState.ACTIVE: {QueueState.READY, QueueState.STALLED, QueueState.INVALID, QueueState.PENDING},
    QueueState.READY: {QueueState.PROCESSING, QueueState.PENDING},
    QueueState.PROCESSING: {QueueState.DONE, QueueState.FAILED, QueueState.READY},
    QueueState.DONE: set(),
    QueueState.STALLED: set(),
    QueueState.INVALID: set(),
    QueueState.FAILED: set(),
}

SHOULD_DELETE = {QueueState.DONE, QueueState.STALLED, QueueState.INVALID, QueueState.FAILED}


def set_queue_state(t, new):
    old = t.queue_state
    if new == old:
        return
    if new not in STATE_CHANGE_VALID[old]:
        raise ValueError(f"Illegal transition {old} -> {new} for {t}")
    logger.info(f"Queue state of {t}: {old} -> {new}")
    t.queue_state = new

