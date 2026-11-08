import logging
from sqlalchemy import select
from .models import QueueState, Torrent, set_queue_state
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor
from dataclasses import dataclass, field
from typing import Optional
import time
import datetime
from .utils import AttrDict

logger = logging.getLogger(__name__)

processing_function = None

def processor(func):
    global processing_function
    processing_function = func
    return func



@dataclass(frozen=True)
class FileInfo:
    file_id: int
    filename: str
    path: Path
    size: int
    
@dataclass(frozen=True)
class TorrentInfo:
    hash: str
    name: Optional[str]
    meta: Optional[dict]
    magnet: str
    files: list[FileInfo] = field(default_factory=list)



def watch(session, config):
    if processing_function is None:
        raise ValueError("No processing function registered")

    config = AttrDict(config)

    while True:
        try:
            info = _get_next(session, config)
        except Exception as e:
            raise RuntimeError(f"Error getting next torrent to process. Make sure to run the manager first to populate the database") from e
        
        if info is None:
            time.sleep(0.5)
            continue
        
        processing_function(info)
        _set_done(session, info.hash)




def _get_next(session, config):
    with session() as s:
        stmt = (
            select(Torrent)
            .where(
                Torrent.queue_state == QueueState.READY,
            )
            .order_by(Torrent.time_began.asc())
            .limit(1)
        )
        t = s.scalar(stmt)
        
        if t is None:
            return None

        logger.info(f"Processing torrent: {t}")

        files = [
            FileInfo(
                file_id=f.file_id,
                filename=f.filename,
                path=Path(config.qbit.save_path) / f.filename,
                size=f.size
            )
            for f in t.files
        ]

        info = TorrentInfo(
            hash=t.hash,
            name=t.name,
            meta=t.meta,
            magnet=t.magnet,
            files=files
        )
        return info



def _set_done(session, hash):
    with session() as s:
        t = s.get(Torrent, hash)
        if t is None:
            logger.warning(f"{hash} not found in database?")
            return

        if t.queue_state != QueueState.READY:
            logger.warning(f"{t} is no longer READY ({t.queue_state}), not marking done")
            return

        set_queue_state(t, QueueState.DONE)
        s.commit()


