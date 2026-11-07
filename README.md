This is a torrent download queue tool with worker logic

Use
```bash
pip install .
tq --help
```


Handler signatures:

```python
from tq import TorrentDictionary, TorrentInfo, processor, validator

@processor
def process_download(info: TorrentInfo) -> None:
    pass


@validator
def validate_torrent(candidate: TorrentDictionary) -> bool:
    pass


```

Example validator that ensures a max size and certain file types:


```python
MAX_SIZE_GB = 100
ALLOWED_EXTENSIONS = ['.mp4', '.mkv']



@validator
def validate_torrent(candidate: TorrentDictionary) -> bool:
    size_gb = candidate.size / 1000**3

    if size_gb > MAX_SIZE_GB:
        return False

    if not any(candidate.name.lower().endswith(ext) for ext in ALLOWED_EXTENSIONS):
        return False

    return True
```
