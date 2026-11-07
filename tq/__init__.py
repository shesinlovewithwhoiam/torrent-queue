from .processing import TorrentInfo, FileInfo
from .manage import TorrentManager, Torrent, File
from qbittorrentapi import TorrentDictionary
from .manage import watcher
from .processing import processor


__all__ = [
    'TorrentInfo', 
    'FileInfo',
    'Torrent',
    'File',
    'TorrentManager',
    'TorrentDictionary', # re-export
    'processor',
    'watcher'
]