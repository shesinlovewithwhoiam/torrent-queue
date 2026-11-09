import qbittorrentapi
import os
from dataclasses import dataclass, field
import logging

logger = logging.getLogger(__name__)


@dataclass
class ClientUpdate:
    add: list[tuple[str, str]] = field(default_factory=list)
    pause: list[str] = field(default_factory=list)
    resume: list[str] = field(default_factory=list)
    delete: list[str] = field(default_factory=list)
    select: list[tuple[str, list[int], list[int]]] = field(default_factory=list)


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

        if updates.select:
            logger.info(f"Setting file priority for {len(updates.select)} selected torrents in client")
            for torrent_hash, get, skip in updates.select:
                if skip:
                    self._client.torrents_file_priority(torrent_hash=torrent_hash, file_ids=skip, priority=0)
                if get:
                    self._client.torrents_file_priority(torrent_hash=torrent_hash, file_ids=get, priority=1)

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


