
Use
```bash
pip install .
tq --help
```



## Configuration

Initialize a config in your directory using:

```
tq init path/to/tq.toml
```

It is recommended you read through and consider each key, as the values you'd want to pick depend highly on your use case and downloads. Pass the config using the `--config` flag, or give it to your `App` like:

```
app = App("path/to/config.toml")
```


### [qbit]
qBittorrent settings. Make sure the Web UI is reachable before running `tq manager`. 

| Key | Required | Description | 
| --- | --- | --- |
| host | yes | Web UI host |
| port | yes | Web UI port |
| username | yes | Web UI username |
| password | yes | Web UI password | 
| category | yes | Category that all of `tq`'s torrents will be grouped under. Automatically created. |
| sequential | no | Whether to download torrents in sequential order. |
| save_path | yes | Directory torrents will be downloaded to. |
| container_save_path | no | You may want to use this if running qBittorrent in Docker container, or any setup where qBittorrent sees the folder at a different path. This is the path torrents will be saved to by the client. You can then mount the local `save_path` to this path in your `docker-compose.yml`. Defaults to `save_path` |



### [data]
| Key | Required | Description | 
| --- | --- | --- |
| db_path | yes | Path to SQLite queue database. |
| log_dir | yes | Directory in which log files will be written to. |



### [management]
| Key | Required | Description | 
| --- | --- | --- |
| max_active | yes | Maximum number of active torrents occupying a slot at once. Counts downloading (but not slow) torrents, completed torrents waiting to be processed, currently processing torrents and errored torrents. More than this many clients *can* show up in your client, as slow torrents are intentionally uncounted to make room for new downloads. |
| inspect | no | Enable inspection. Defaults to `false`. When `true`, the manager will take snapshots of downloading torrents and hold every finished torrent until your inspector has judged it, so a `tq inspector` must be running. With none running, finished torrents wait forever. |

#### [management.slow_cls]
When a torrent should be set to slow. 
| Key | Required | Description | 
| --- | --- | --- |
| slow_threshold | yes | Download speed in MB/s below which a torrent can be classified as slow. |
| min_time | yes | Seconds it must remain below `slow_threshold` to be marked as slow. |
| unslow_threshold | yes | Speed in MB/s above which a torrent is unmarked as slow. | 

#### [management.dead_cls]
When a torrent should be given up on, marked as `STALLED`, and removed from the client.
| Key | Required | Description | 
| --- | --- | --- |
| timeout | yes | Seconds a torrent downloads nothing, after metadata is downloaded, before it is marked dead. |
| metadata_timeout | no | Seconds a magnet may go without metadata before it is marked dead. Defaults to `timeout` |


### [inspection] (optional)
Only used by `tq inspector`. 
| Key | Description | 
| --- | --- |
| poll | Seconds an idle inspector waits before checking again for new things to inspect. Not very important. |
| lease | Seconds an inspector can hold a torrent before another can take it over. Set above the time your slowest inspector could take. |