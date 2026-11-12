import click
import importlib
from importlib import resources
import sys
from pathlib import Path
import shutil
import logging
import os

from .utils import load_toml
from .manage import TorrentManager
from .app import App
from .inspect import InspectorError


def setup_logging(name, dir_path, mode):
    logging.basicConfig(
        level=logging.INFO,
        format="[%(asctime)s] %(message)s",
        handlers=[
            logging.FileHandler(os.path.join(dir_path, f"{name}.log"), mode=mode, encoding='utf-8'),
            logging.StreamHandler()
        ]
    )


def find_app(ctx):
    path, config = ctx.obj['app_path'], None

    if ctx.obj['config'] is not None:
        try:
            config = load_toml(ctx.obj['config'])
        except FileNotFoundError as e:
            click.secho(f"[ERROR] Failed to load config: {e}", fg='red')
            ctx.exit(1)
        path = path or config.get('handlers', {}).get('app')

    if path is None:
        if config is None:
            click.secho("[ERROR] Need config, pass --config or include in App(config).", fg='red')
            ctx.exit(1)
        return App(config)

    mod, _, var = path.partition(':')
    var = var or 'app'

    cwd = os.getcwd()
    if cwd not in sys.path:
        sys.path.insert(0, cwd)

    try:
        module = importlib.import_module(mod)
    except ModuleNotFoundError as e:
        click.secho(f"[ERROR] Failed to import module '{mod}': {e}", fg='red')
        ctx.exit(1)

    app = getattr(module, var, None)
    if not isinstance(app, App):
        click.secho(f"[ERROR] '{var}' in module '{mod}' is not an instance of tq.App.", fg='red')
        ctx.exit(1)

    if config is not None:
        app.use_config(config)

    try:
        app.config
    except (RuntimeError, FileNotFoundError) as e:
        click.secho(f"[ERROR]: {e}", fg='red')
        ctx.exit(1)

    return app





@click.group()
@click.option('-A', '--app', default=None, help='App module path, in the form of "module:app". Overrides the app module specified in the config file.')
@click.option('--config', default=None, type=click.Path(path_type=Path), help='Path to TOML config file')
@click.pass_context
def cli(ctx, app, config):
    ctx.ensure_object(dict)
    ctx.obj['config'] = config
    ctx.obj['app_path'] = app

def prepare_worker(ctx, name):
    app = find_app(ctx)
    config = app.config

    if not Path(config['data']['db_path']).exists():
        click.secho("[ERROR] Queue database does not exist. Run the manager first: tq manager -d data.csv", fg='red')
        ctx.exit(1)

    Path(config['data']['log_dir']).mkdir(parents=True, exist_ok=True)
    setup_logging(name, config['data']['log_dir'], mode='a')
    return app, config



@cli.command()
@click.pass_context
def inspector(ctx):
    app, config = prepare_worker(ctx, 'inspector')
    try:
        worker = app.inspect_worker()
    except RuntimeError as e:
        click.secho(f"[ERROR] {e}", fg='red')
        ctx.exit(1)

    try:
        worker.run()
    except InspectorError as e:
        click.secho(f"[ERROR] {e}", fg='red')
        ctx.exit(1)



@cli.command()
@click.pass_context
def worker(ctx):
    app, config = prepare_worker(ctx, 'worker')

    raise NotImplementedError()


@cli.command()
@click.option('-d', '--data', type=str, help="Path to CSV data file to rebuild database from. Must contain 'magnet' column")
@click.pass_context
def manager(ctx, data):
    app = find_app(ctx)
    config = app.config

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

    tm = TorrentManager(config=config, session=app.sessionmaker, inspect=app.has_inspector)
    if data is not None:
        import polars as pl
        df = pl.read_csv(data)
        try:
            tm.build_db(df, app.engine)
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
    
