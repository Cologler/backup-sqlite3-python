# -*- coding: utf-8 -*-
# 
# Copyright (c) 2023~2999 - Cologler <skyoflw@gmail.com>
# ----------
# 
# ----------

import datetime
import os
import shutil
import sqlite3
import sys
from contextlib import closing, nullcontext, contextmanager
from dataclasses import dataclass
from functools import cached_property
from pathlib import Path
from typing import Annotated, NotRequired, TypedDict
import io

import rich
import typer
import portalocker
from rich.console import Console
from rich.progress import Progress
from yaml import safe_load

from .compression import compress_zstd, decompress_zstd

DATETIME_FORMAT = r'%Y%m%d%H%M%S'


class BackupConfig(TypedDict):
    db_path: str
    dest_dir: str
    retention: NotRequired[int]
    interval: NotRequired[int]
    compression: NotRequired[bool]


@dataclass
class BackupRecord:
    path: Path
    created_in_str: str
    is_compressed: bool

    @cached_property
    def created(self) -> datetime.datetime:
        return datetime.datetime.strptime(self.created_in_str, DATETIME_FORMAT)


def list_exists_backups(name: str, backups_location: Path) -> list[BackupRecord]:
    prefix = name + '.'

    def iter_backup_records():
        for p in [x for x in backups_location.iterdir() if x.stem.startswith(prefix)]:
            if p.name.endswith('.sqlite3.zst'):
                yield BackupRecord(p, p.name[len(prefix):-len('.sqlite3.zst')], True)
            elif p.name.endswith('.sqlite3'):
                yield BackupRecord(p, p.name[len(prefix):-len('.sqlite3')], False)

    return sorted(list(iter_backup_records()), key=lambda r: r.created_in_str)


def _filter_not_retention_files(records: list[BackupRecord], retention: int) -> list[BackupRecord]:
    if retention < 1:
        raise ValueError('retention must be greater than 0')
    if len(records) > retention - 1:
        return records[:len(records) - retention + 1]
    return []


@contextmanager
def _tempfile_to_write(path: Path):
    '''
    Execute as a context manager, returns a temporary file path to write.

    If an exception is raised, the temporary file will be deleted.
    If execution without exception, the temporary file will be renamed to the original path.
    '''
    tmp_path = path.with_suffix(path.suffix + '.tmp')
    tmp_path.unlink(missing_ok=True)
    try:
        yield tmp_path
    except Exception:
        raise
    else:
        if tmp_path.exists():
            tmp_path.rename(path)
    finally:
        tmp_path.unlink(missing_ok=True)


def _backup_with_sqlite_backup(src_db_path: Path, dest_db_path: Path, *, enable_progress_bar):
    with closing(sqlite3.connect(str(src_db_path))) as src_db, closing(sqlite3.connect(str(dest_db_path))) as dest_db:
        if enable_progress_bar:
            backup_pages = 1024 * 8
            progress = Progress(console=Console(file=sys.stderr))
            progress_task = progress.add_task("  [cyan]SQLite.backuping...", total=1000)
            def progress_callback(status, remaining, total):
                progress.update(progress_task, total=total, completed=(total - remaining))
        else:
            backup_pages = -1
            progress = nullcontext()
            progress_callback = None

        with progress:
            src_db.backup(dest_db, pages=backup_pages, progress=progress_callback)


def _compress_with_zstd(src_fp: io.BufferedReader, src_size: int, dest_path: Path, *, enable_progress_bar):
    if enable_progress_bar:
        progress = Progress(console=Console(file=sys.stderr))
        progress_task = progress.add_task("  [cyan]Compress...", total=src_size)

        def progress_callback(read_size: int):
            progress.update(progress_task, advance=read_size)
    else:
        progress = nullcontext()
        progress_callback = None

    with progress, dest_path.open('xb') as dest:
        compress_zstd(src_fp, dest, progress_callback)


def backup_sqlite3(
        name: str, config: BackupConfig, *,
        enable_progress_bar: bool = True,
        dry_run: bool = False,
    ) -> None:

    dryrun_prefix: str = '[yellow](dryrun)[/] ' if dry_run else ''

    rich.print(f'Backup task: [green]{name}[/]')

    # load config
    dest_dir = config['dest_dir']
    dest_dir_path = Path(config['dest_dir'])
    dest_dir_path.mkdir(parents=True, exist_ok=True)
    do_compress = config.get('compression', True)

    now = datetime.datetime.now() # use local timezone for human readable

    backup_records = list_exists_backups(name, dest_dir_path)

    if isinstance(interval := config.get('interval'), int):
        if backup_records and now - backup_records[-1].created < datetime.timedelta(seconds=interval):
            rich.print('  Skipped by [blue]interval[/] options.')
            return

    old_records = _filter_not_retention_files(backup_records, config.get('retention', 1))

    backup_time = now.strftime(DATETIME_FORMAT)
    new_prefix = os.path.join(dest_dir, f'{name}.{backup_time}')
    backup_path_final = Path(new_prefix + '.sqlite3')
    if do_compress:
        backup_path_final = backup_path_final.with_name(backup_path_final.name + '.zst')

    if backup_path_final.exists():
        raise FileExistsError(f'The finally backup file already exists: {backup_path_final}')

    if dry_run:
        action = 'Backup'
        if do_compress:
            action += ' and Compress'
        rich.print(f'  {dryrun_prefix}{action} to [blue]{backup_path_final}[/]')

    else:
        with _tempfile_to_write(backup_path_final) as backup_path:
            src_db_path = Path(config['db_path'])

            if not do_compress:
                # backup
                _backup_with_sqlite_backup(src_db_path, backup_path, enable_progress_bar=enable_progress_bar)

            else:
                # backup and compress
                # try lock origin file:
                try:
                    with src_db_path.open('rb') as src_fp:
                        portalocker.lock(src_fp, portalocker.LOCK_NB)
                        rich.print('  Locked original database.')
                        # ensure sqlite -wal file does not exist
                        if src_db_path.with_suffix(src_db_path.suffix + '-wal').exists():
                            rich.print('  WAL file exists, fallback to SQLite.backup.')
                        else:
                            rich.print('  Compress database from original.')
                            _compress_with_zstd(src_fp, src_db_path.stat().st_size, backup_path, enable_progress_bar=enable_progress_bar)
                except portalocker.LockException:
                    rich.print('  Failed to lock the original database, fallback to SQLite.backup.')

                if not backup_path.is_file():
                    # fallback to backup
                    mid_db_path = backup_path.with_suffix(backup_path.suffix + '.mid.tmp')
                    try:
                        _backup_with_sqlite_backup(src_db_path, mid_db_path, enable_progress_bar=enable_progress_bar)
                        with mid_db_path.open('rb') as mid_db_fp:
                            _compress_with_zstd(mid_db_fp, mid_db_path.stat().st_size, backup_path, enable_progress_bar=enable_progress_bar)
                    finally:
                        mid_db_path.unlink(missing_ok=True)

        if backup_path_final.is_file():
            rich.print(f'  Backup is created: [blue]{backup_path_final}[/]')

    # finally, remove old records
    for old in old_records:
        rich.print(f'  {dryrun_prefix}Remove old backup: [blue]{old.path}[/]')
        if not dry_run:
            old.path.unlink()


def restore_sqlite3(
        name: str, config: BackupConfig,
    ) -> None:

    rich.print(f'Restore task: [green]{name}[/green]')

    dest_dir_path = Path(config['dest_dir'])
    backup_records = list_exists_backups(name, dest_dir_path)
    if not backup_records:
        raise FileNotFoundError(f'No backups found for {name}')

    src_db_path = config['db_path']
    db_name_tmp = Path(src_db_path + '-restoring.tmp')
    try:
        latest_backup_record = backup_records[-1]
        if latest_backup_record.is_compressed:
            with latest_backup_record.path.open('rb') as src, db_name_tmp.open('xb') as dest:
                decompress_zstd(src, dest, None)
        else:
            shutil.copyfile(latest_backup_record.path, db_name_tmp)
        # delete order should not change
        Path(src_db_path + '-shm').unlink(True)
        Path(src_db_path + '-wal').unlink(True)
        Path(src_db_path).unlink(True)
        os.rename(db_name_tmp, src_db_path)
    except:
        db_name_tmp.unlink(True)
        raise


def get_absolute_path(working_dir: str, relative_path: str):
    if os.path.isabs(relative_path):
        return relative_path
    return os.path.abspath(os.path.join(working_dir, relative_path))


def preprocess_config(config: BackupConfig, profile_path: str) -> BackupConfig:
    assert os.path.isabs(profile_path)
    working_dir = os.path.dirname(profile_path)
    config['db_path'] = get_absolute_path(working_dir, os.path.expandvars(config['db_path']))
    config['dest_dir'] = get_absolute_path(working_dir, os.path.expandvars(config['dest_dir']))
    return config


app = typer.Typer()


@app.command()
def backup(
        profile: Annotated[Path, typer.Option(
            exists=True,
            dir_okay=False,
            readable=True
        )],
        config_name: Annotated[str, typer.Argument()] = None,
        quite: Annotated[bool, typer.Option('--quiet')] = False,
        dry_run: Annotated[bool, typer.Option('--dry-run')] = False,
    ):

    if config_name:
        typer.echo(f'Trying to backup config: {config_name}')

    profile_abs = profile.absolute()
    with profile_abs.open() as fp:
        profile_content: dict = safe_load(fp)

    if config_name:
        if config_name not in profile_content:
            typer.echo(f'{config_name} is not in {profile_abs}', err=True)
            raise typer.Exit(code=1)
        configs = [(config_name, profile_content[config_name])]
    else:
        configs = profile_content.items()

    for key, config in configs:
        preprocess_config(config, str(profile_abs))
        backup_sqlite3(key, config, enable_progress_bar=not quite, dry_run=dry_run)


@app.command()
def restore(
        profile: Annotated[Path, typer.Option(
            exists=True,
            dir_okay=False,
            readable=True
        )],
        config_name: Annotated[str, typer.Argument()] = None
    ):

    profile_abs = profile.absolute()
    with profile_abs.open() as fp:
        profile_content: dict = safe_load(fp)

    if config_name:
        if config_name not in profile_content:
            typer.echo(f'{config_name} is not in {profile_abs}', err=True)
            raise typer.Exit(code=1)
        configs = [(config_name, profile_content[config_name])]
    else:
        configs = profile_content.items()

    for key, config in configs:
        preprocess_config(config, str(profile_abs))
        restore_sqlite3(key, config)
