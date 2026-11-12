from sqlalchemy import select, insert, func, and_, or_
from dataclasses import dataclass, field
import datetime
import polars as pl
import os
import logging
import qbittorrentapi
import time

from .utils import get_hash
from .models import (
    Base,
    ClientState,
    File,
    QueueState,
    SHOULD_DELETE,
    Torrent,
    set_queue_state,
)
from .client import TorrentClient, ClientUpdate
from .results import Kind




logger = logging.getLogger(__name__)



class TorrentManager:
    def __init__(self, config, session, inspect):
        self.config = config
        self.session = session

        self.client = TorrentClient(self.config['qbit'])
        self.running = True

        self.inspect_enabled = inspect
        logger.info(f"Inspection {'on' if inspect else 'off (no inspector registered)'}")


    def build_db(self, df, engine):
        logger.info(f"Rebuilding database...")

        if 'magnet' not in df.columns:
            raise ValueError("Data must contain a 'magnet' column")
        assert 'queue_position' not in df.columns

        hashes = []
        for row, magnet in enumerate(df['magnet'], start=1):
            try:
                hashes.append(get_hash(magnet))
            except Exception as e:
                raise ValueError(f"Failed to compute hash for row {row}: {e}")

        df = df.with_columns(pl.Series('hash', hashes, dtype=pl.String))

        dupes = df.with_row_index('row', offset=1).filter(pl.col('hash').is_duplicated())
        if len(dupes) > 0:
            rows = dupes['row'].to_list()
            raise ValueError(f"{len(rows)} duplicate hashes found in CSV data.")

        df = df.with_row_index('queue_position', offset=1)

        Base.metadata.drop_all(engine)
        Base.metadata.create_all(engine)
        self._insert_tasks(df)
        logger.info(f"Database built with {len(df)} torrents.")

    def _insert_tasks(self, df):
        meta_cols = [c for c in df.columns if c not in ('hash', 'magnet', 'queue_position')]
        rows = df.to_dicts()
        for row in rows:
            row['meta'] = {c: row.pop(c) for c in meta_cols}
        with self.session() as session:
            session.execute(insert(Torrent), rows)
            session.commit()


    def update(self):
        while self.running:
            self._tick()
            time.sleep(self.client.refresh_interval())

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
        verdict, db_t.verdict = db_t.verdict, None
        wanted = self._apply_select(db_t, t, updates)
        if db_t.queue_state != QueueState.ACTIVE:
            return


        now = datetime.datetime.now()

        if verdict == Kind.REJECT:
            self._terminate(db_t, QueueState.INVALID, db_t.reason or "Inspector returned Reject()")
        elif verdict == Kind.READY:
            self._mark_ready(session, db_t, t, updates, wanted)
        elif db_t.client_state == ClientState.COMPLETED:
            if not self._final_check_pending(db_t, t, now):
                self._mark_ready(session, db_t, t, updates, wanted)
        elif reason := self._check_dead(db_t, t):
            self._terminate(db_t, QueueState.STALLED, reason)
        else:
            self._set_if_slow(db_t, t)
            self._schedule_inspection(db_t, t, now)

    @staticmethod
    def _has_metadata(t):
        return t.total_size > 0 

    def _apply_select(self, db_t, t, updates):
        if not db_t.select_files:
            return None
        wanted = set(db_t.select_files)
        if not self._has_metadata(t):
            return wanted
        files = t['files']
        if not files:
            return wanted

        have = {f['index'] for f in files}
        if wanted - have:
            self._terminate(db_t, QueueState.INVALID, f"Select referenced files not in torrent: {sorted(wanted - have)}")
            return None

        enable = [f['index'] for f in files if f['index'] in wanted and f['priority'] == 0]
        disable = [f['index'] for f in files if f['index'] not in wanted and f['priority'] != 0]
        if enable or disable:
            updates.select.append((db_t.hash, enable, disable))
        else:
            db_t.select_files = None
        return wanted

    def _terminate(self, db_t, state, reason):
        db_t.reason = reason
        logger.info(f"{state}: {reason}: {db_t}")
        set_queue_state(db_t, state)

    def _schedule_inspection(self, db_t, t, now):
        if not self.inspect_enabled or not self._has_metadata(t):
            return
        due = db_t.next_check_at
        if due is None or due > now:
            return
        if db_t.snapshot_at is not None and db_t.snapshot_at >= due:
            return
        if db_t.client_state == ClientState.PAUSED and db_t.snapshot_at is not None:
            return
        self._take_snapshot(db_t, t, now)


    def _take_snapshot(self, db_t, t, now):
        db_t.snapshot = {
            'torrent': {k: v for k, v in dict(t).items() if k != 'files'},
            'files': t['files'],
            'is_final': db_t.client_state == ClientState.COMPLETED,
        }
        db_t.snapshot_at = now

    def _final_check_pending(self, db_t, t, now):
        if not self.inspect_enabled:
            return False
        if (db_t.snapshot or {}).get('is_final'):
            has_checked = db_t.verdict_snapshot_at is not None and db_t.verdict_snapshot_at >= db_t.snapshot_at
            return not has_checked
        db_t.next_check_at = now
        self._take_snapshot(db_t, t, now)
        return True

    def _mark_ready(self, session, db_t, t, updates, wanted=None):
        files = t['files']
        keep = wanted or {f['index'] for f in files if f['priority'] > 0}
        self._init_files(session, db_t, [f for f in files if f['index'] in keep])
        set_queue_state(db_t, QueueState.READY)
        db_t.next_check_at = None
        db_t.is_slow, db_t.slow_since, db_t.dead_since = False, None, None
        if db_t.client_state != ClientState.COMPLETED: # early ready, stop downloading
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
            level = logging.WARNING if new == ClientState.ERRORED else logging.INFO
            logger.log(level, f"Client state of {db_t}: {db_t.client_state} -> {new} ({torrent.state})")
            db_t.client_state = new

        # torrent was just added to client
        if db_t.queue_state == QueueState.PENDING:
            set_queue_state(db_t, QueueState.ACTIVE)
            db_t.time_began = datetime.datetime.now()
            db_t.next_check_at = db_t.time_began

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

        slow_cfg = self.config['management']['slow_cls']
        if speed < slow_cfg['slow_threshold']:
            if db_t.slow_since is None:
                db_t.slow_since = now
            elif not db_t.is_slow and (now - db_t.slow_since).total_seconds() >= slow_cfg['min_time']:
                db_t.is_slow = True
                # self.client.torrents_add_tags(tags='slow', torrent_hashes=db_t.hash)
                logger.info(f"Slowed: {db_t}")
        elif speed > slow_cfg['unslow_threshold']:
            if db_t.is_slow:
                logger.info(f"Recovered: {db_t}")
            db_t.slow_since = None
            db_t.is_slow = False
            # self.client.torrents_remove_tags(tags='slow', torrent_hashes=db_t.hash)



    def _check_dead(self, db_t, t):
        if db_t.client_state != ClientState.DOWNLOADING:
            db_t.dead_since = None
            return None

        cfg = self.config['management']['dead_cls']
        now = datetime.datetime.now()

        if not self._has_metadata(t):
            # Note on edge case: torrent limited before metadata is downloaded, then resumed after timeout. Marked as dead since reference is time_began. 
            db_t.dead_since = None
            timeout = cfg.get('metadata_timeout', cfg['timeout'])
            if db_t.time_began and (now - db_t.time_began).total_seconds() >= timeout:
                return f"No metadata after {timeout} seconds"
            return None

        if t.dlspeed == 0:
            if db_t.dead_since is None:
                db_t.dead_since = now
            else:
                waited = (now - db_t.dead_since).total_seconds()
                if waited >= cfg['timeout']:
                    db_t.dead_since = None
                    db_t.is_slow = False    
                    db_t.slow_since = None
                    return f"No download activity for {waited:.0f}s"
        else:
            db_t.dead_since = None

        return None



    # MARK: Limiter
    def _limit(self, session, updates):
        active = self._count_active(session)
        maxa = self.config['management']['max_active']

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
                    or_(
                        Torrent.client_state.in_([ClientState.COMPLETED, ClientState.ERRORED]),
                        and_(Torrent.client_state == ClientState.DOWNLOADING, ~Torrent.is_slow),
                    ),
                ),
                Torrent.queue_state.in_([QueueState.READY, QueueState.PROCESSING])
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

                    t.reason = None
                    t.next_check_at = t.snapshot = t.snapshot_at = None
                    t.verdict = t.verdict_snapshot_at = t.select_files = None
                    t.inspect_claimed_by = t.inspect_lease_until = None

                    t.files.clear()
                    logger.info(f"Deprioritizing removed torrent: {t}. Queue position: {t.queue_position} -> {max_pos + i}")
                    t.queue_position = max_pos + i


    def _init_files(self, session, torrent, files):
        to_insert = [
            {
                'hash': torrent.hash,
                'file_id': f['index'],
                'filename': f['name'],
                'size': f['size'],
            } 
            for f in files
        ]


        logger.info(f"Initializing {len(files)} files for torrent {torrent}")
        session.execute(insert(File), to_insert)