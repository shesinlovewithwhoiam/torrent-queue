from .processing import TorrentInfo, FileInfo
from .manage import TorrentManager, Torrent, File, TorrentStatus
from qbittorrentapi import TorrentDictionary
from .manage import validator
from .processing import processor


__all__ = [
    'TorrentInfo', 
    'FileInfo',
    'Torrent',
    'File',
    'TorrentStatus',
    'TorrentManager',
    'TorrentDictionary', # re-export
    'processor',
    'validator'
]