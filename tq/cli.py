import click
import importlib
from importlib import resources
import sys
from pathlib import Path
import shutil
import logging
import os


from .manage import TorrentManager
from .queue import queue
from .workers import InspectorError


def setup_logging(name, dir_path, mode):
    logging.basicConfig(
        level=logging.INFO,
        format="[%(asctime)s] %(message)s",
        handlers=[
            logging.FileHandler(os.path.join(dir_path, f"{name}.log"), mode=mode, encoding='utf-8'),
            logging.StreamHandler()
        ]
    )


def get_queue(ctx, module):
    cwd = os.getcwd()
    if cwd not in sys.path:
        sys.path.insert(0, cwd)

    try:
        importlib.import_module(module)
    except ModuleNotFoundError as e:
        click.secho(f"[ERROR] Failed to import module '{module}': {e}", fg='red')
        ctx.exit(1)

    if ctx.obj['config'] is not None:
        queue.use_config(ctx.obj['config'])

    try:
        queue.config
    except (RuntimeError, FileNotFoundError) as e:
        click.secho(f"[ERROR] {e}", fg='red')
        ctx.exit(1)

    return queue


@click.group()
@click.option('--config', default=None, type=click.Path(path_type=Path), help='Path to TOML config file. Overrides tq.configure() in your module.')
@click.pass_context
def cli(ctx, config):
    ctx.ensure_object(dict)
    ctx.obj['config'] = config


def prepare_worker(ctx, module, name):
    q = get_queue(ctx, module)
    config = q.config

    if not Path(config['data']['db_path']).exists():
        click.secho("[ERROR] Queue database does not exist. Run the manager first: tq manager -d data.csv", fg='red')
        ctx.exit(1)

    Path(config['data']['log_dir']).mkdir(parents=True, exist_ok=True)
    setup_logging(name, config['data']['log_dir'], mode='a')
    return q



@cli.command()
@click.argument('module')
@click.pass_context
def inspector(ctx, module):
    q = prepare_worker(ctx, module, 'inspector')
    try:
        worker = q.inspect_worker()
    except RuntimeError as e:
        click.secho(f"[ERROR] {e}", fg='red')
        ctx.exit(1)

    try:
        worker.run()
    except InspectorError as e:
        click.secho(f"[ERROR] {e}", fg='red')
        ctx.exit(1)



@cli.command()
@click.argument('module')
@click.pass_context
def processor(ctx, module):
    q = prepare_worker(ctx, module, 'processor')
    try:
        worker = q.process_worker()
    except RuntimeError as e:
        click.secho(f"[ERROR] {e}", fg='red')
        ctx.exit(1)

    worker.run()


@cli.command()
@click.argument('module')
@click.option('-d', '--data', type=str, help="Path to CSV data file to rebuild database from. Must contain 'magnet' column")
@click.pass_context
def manager(ctx, module, data):
    q = get_queue(ctx, module)
    config = q.config

    db_path = Path(config['data']['db_path'])
    if db_path.exists() and data is not None:
        if not click.confirm(f"{db_path} already exists. Rebuilding it from {data} deletes queue and all progress. Overwrite?"):
            click.echo("Cancelled. Existing database remains unchanged. Run without -d to resume from there.")
            ctx.exit(0)
    elif not db_path.exists() and data is None:
        click.secho("[ERROR] Queue database does not exist. Please provide a CSV data file to build the database from using the -d option.", fg='red')
        ctx.exit(1)
    else:
        db_path.parent.mkdir(parents=True, exist_ok=True)

    log_path = Path(config['data']['log_dir'])
    log_path.mkdir(parents=True, exist_ok=True)

    setup_logging('manager', config['data']['log_dir'], mode='w' if data is not None else 'a')

    tm = TorrentManager(config=config, session=q.sessionmaker, inspect=q.has_inspector)
    if data is not None:
        import polars as pl
        df = pl.read_csv(data)
        try:
            tm.build_db(df, q.engine)
        except ValueError as e:
            click.secho(f"[ERROR] {e}", fg='red')
            ctx.exit(1)

    tm.update()


@cli.command()
@click.argument('to', type=click.Path(path_type=Path))
def init(to):
    if to.exists():
        click.confirm("Config already exists. Overwrite?", abort=True)
    else:
        to.parent.mkdir(parents=True, exist_ok=True)


    with resources.as_file(resources.files('tq').joinpath('data', 'config.toml')) as conf:
        shutil.copy(conf, to)

    click.echo(f"Config file written to {to}")
    
