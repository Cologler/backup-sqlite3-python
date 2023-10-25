# -*- coding: utf-8 -*-
# 
# Copyright (c) 2023~2999 - Cologler <skyoflw@gmail.com>
# ----------
# 
# ----------

import datetime
import os
import sqlite3
import sys
from contextlib import closing
from pathlib import Path
from typing import Annotated, Optional, TypedDict, NotRequired
from dataclasses import dataclass
from functools import cached_property

import typer
import rich
from rich.console import Console
from rich.progress import Progress
from yaml import safe_load

DATETIME_FORMAT = r'%Y%m%d%H%M%S'


class BackupConfig(TypedDict):
    db_path: str
    dest_dir: str
    retention: NotRequired[int]
    interval: NotRequired[int]


@dataclass
class BackupRecord:
    path: Path
    created_in_str: str

    @cached_property
    def created(self) -> datetime:
        return datetime.datetime.strptime(self.created_in_str, DATETIME_FORMAT)


def list_exists_backups(name: str, backups_location: Path) -> list[BackupRecord]:
    prefix = name + '.'
    suffix = '.sqlite3'

    backup_records = [
        BackupRecord(p, p.stem[len(prefix):]) for p in backups_location.iterdir()
        if p.stem.startswith(prefix) and p.suffix == suffix
    ]

    return sorted(backup_records, key=lambda r: r.created_in_str)


def _filter_not_retention_files(records: list[BackupRecord], retention: int) -> list[BackupRecord]:
    if retention < 1:
        raise ValueError('retention must be greater than 0')
    if len(records) > retention - 1:
        return records[:len(records) - retention + 1]
    return []


def backup_sqlite3(
        name: str, config: BackupConfig, *,
        enable_progress_bar: bool = True
    ) -> None:

    typer.echo(f'Backup task: {name}')

    dest_dir = config['dest_dir']
    dest_dir_path = Path(config['dest_dir'])
    dest_dir_path.mkdir(parents=True, exist_ok=True)

    now = datetime.datetime.now() # use local timezone for human readable

    backup_records = list_exists_backups(name, dest_dir_path)

    if isinstance(interval := config.get('interval'), int):
        if backup_records and now - backup_records[-1].created < datetime.timedelta(seconds=interval):
            rich.print('  Skipped by [green]interval[/green] options.')
            return

    old_records = _filter_not_retention_files(backup_records, config.get('retention', 1))

    backup_time = now.strftime(DATETIME_FORMAT)
    new_prefix = os.path.join(dest_dir, f'{name}.{backup_time}')

    db_name_tmp = new_prefix + '.tmp'
    db_name_fin = new_prefix + '.sqlite3'
    if os.path.exists(db_name_tmp):
        raise FileExistsError(f'{db_name_tmp} already exists')
    if os.path.exists(db_name_fin):
        raise FileExistsError(f'{db_name_fin} already exists')

    try:
        with closing(sqlite3.connect(config['db_path'])) as src_db:
            with closing(sqlite3.connect(db_name_tmp)) as dest_db:
                if enable_progress_bar:
                    with Progress(console=Console(file=sys.stderr)) as progress:
                        progress_task = progress.add_task("  [cyan]Backuping...", total=1000)
                        def update_progress(status, remaining, total):
                            progress.update(progress_task, total=total, completed=(total - remaining))
                        src_db.backup(dest_db, pages=8192, progress=update_progress)
                else:
                    src_db.backup(dest_db)
    except:
        if os.path.exists(db_name_tmp):
            os.remove(db_name_tmp)
        raise

    os.rename(db_name_tmp, db_name_fin)
    for old in old_records:
        old.path.unlink()


def get_absolute_path(working_dir: str, relative_path: str):
    if os.path.isabs(relative_path):
        return relative_path
    return os.path.abspath(os.path.join(working_dir, relative_path))

def preprocess_config(config: BackupConfig, profile_path: str) -> BackupConfig:

    config['db_path'] = get_absolute_path(
        os.path.dirname(profile_path),
        os.path.expandvars(config['db_path'])
    )

    assert os.path.isabs(profile_path)
    config['dest_dir'] = get_absolute_path(
        os.path.dirname(profile_path),
        os.path.expandvars(config['dest_dir'])
    )

    return config

app = typer.Typer()

@app.command()
def backup(
        profile: Annotated[Optional[Path], typer.Option(
            exists=True,
            dir_okay=False,
            readable=True
        )],
        config_name: Annotated[str, typer.Argument()] = None,
        quite: bool = False,
    ):

    if config_name:
        typer.echo(f'Trying to backup config: {config_name}')

    profile_path = profile.absolute()
    profile_path_str = str(profile_path)
    with profile_path.open() as fp:
        profile_content: dict = safe_load(fp)

        if config_name:
            if config_name not in profile_content:
                typer.echo(f'{config_name} is not in {profile_path}', err=True)
                raise typer.Exit(code=1)
            configs = [(config_name, profile_content[config_name])]
        else:
            configs = profile_content.items()

        for key, config in configs:
            preprocess_config(config, profile_path_str)
            backup_sqlite3(key, config, enable_progress_bar=not quite)

@app.command()
def restore(
        profile: Annotated[Optional[Path], typer.Option(
            exists=True,
            dir_okay=False,
            readable=True
        )],
        config_name: Annotated[str, typer.Argument()] = None
    ):
    raise NotImplementedError
