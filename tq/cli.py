import click
from .utils import load_toml
from .db import make_sessionmaker
from .processing import watch
from .manage import TorrentManager
import importlib
from importlib import resources
import sys
from pathlib import Path
import shutil
import logging
import os

CONFIG_DEFAULT_PATH = "./configs/tq.toml"



def setup_logging(name, dir_path, mode):
    logging.basicConfig(
        level=logging.INFO,
        format="[%(asctime)s] %(message)s",
        handlers=[
            logging.FileHandler(os.path.join(dir_path, f"{name}.log"), mode=mode, encoding='utf-8'),
            logging.StreamHandler()
        ]
    )

def initialize(path):
    try:
        config = load_toml(path)
    except FileNotFoundError as e:
        click.secho(f"[ERROR] Failed to load config from {path}: {e}", fg='red')
        sys.exit(1)
    return config

def get_module(name):
    cwd = os.getcwd()
    if cwd not in sys.path:
        sys.path.insert(0, cwd)

    importlib.import_module(name)


@click.group()
@click.option('--config', default=CONFIG_DEFAULT_PATH, type=click.Path(path_type=Path), help='Path to TOML config file')
@click.pass_context
def cli(ctx, config):
    ctx.ensure_object(dict)
    ctx.obj['config'] = config


@cli.command()
@click.pass_context
def worker(ctx):
    config = initialize(ctx.obj['config'])
    try:
        get_module(config['handlers']['module'])
    except ModuleNotFoundError as e:
        click.secho(f'[ERROR] Running worker but handler module could not be found.', fg='red')
        sys.exit(1)

    setup_logging('worker', config['data']['log_dir'], mode='w')
    s, _ = make_sessionmaker(config['data']['db_path'])

    watch(s, config)


@cli.command()
@click.option('-d', '--data', type=str, help="Path to CSV data file to rebuild database from. Must contain 'magnet' column")
@click.pass_context
def manager(ctx, data):
    config = initialize(ctx.obj['config'])
    try:
        get_module(config['handlers']['module'])
    except ModuleNotFoundError as e:
        click.secho(f'[WARN] Running manager without handlers module loaded, as module could not be found. This is fine if you do not need validation.', fg='yellow')

    db_path = Path(config['data']['db_path'])
    if db_path.exists() and data is not None:
        click.confirm("Database already exists. Overwrite?", abort=True)
    elif not db_path.exists() and data is None:
        click.secho("[ERROR] Queue database does not exist. Please provide a CSV data file to build the database from using the -d option.", fg='red')
        sys.exit(1)
    else:
        db_path.parent.mkdir(parents=True, exist_ok=True)

    log_path = Path(config['data']['log_dir'])
    log_path.mkdir(parents=True, exist_ok=True)


    setup_logging('manager', config['data']['log_dir'], mode='w' if data is not None else 'a')
    s, e = make_sessionmaker(config['data']['db_path'])

    tm = TorrentManager(config=config, session=s)
    if data is not None:
        import polars as pl
        df = pl.read_csv(data)
        tm.build_db(df=df, engine=e)

    tm.update()


@cli.command()
@click.argument('to', default=CONFIG_DEFAULT_PATH, type=click.Path(path_type=Path))
def init(to):
    if to.exists():
        click.confirm("Config already exists. Overwrite?", abort=True)
    else:
        to.parent.mkdir(parents=True, exist_ok=True)


    with resources.as_file(resources.files('tq').joinpath('data', 'config.toml')) as conf:
        shutil.copy(conf, to)

    click.echo(f"Config file written to {to}")
    
