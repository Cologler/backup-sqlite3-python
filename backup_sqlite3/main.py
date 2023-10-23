# -*- coding: utf-8 -*-
# 
# Copyright (c) 2023~2999 - Cologler <skyoflw@gmail.com>
# ----------
# 
# ----------

import datetime
import os
import sqlite3
from contextlib import closing
from pathlib import Path
from typing import Annotated, Optional, TypedDict

import typer
from yaml import safe_load


class BackupConfig(TypedDict):
    db_path: str
    retention: int
    dest_dir: str

def list_exists_backups(name: str, config: BackupConfig) -> list[str]:
    backups = [
        n for n in os.listdir(config['dest_dir'])
        if n.startswith(name + '.') and n.endswith('.sqlite3')
    ]
    return sorted(backups)

def _find_old_files(name: str, config: BackupConfig) -> list[str]:
    backups = list_exists_backups(name, config)
    if len(backups) > config['retention'] - 1:
        return backups[:len(backups) - config['retention'] + 1]
    return []

def backup_sqlite3(name: str, config: BackupConfig) -> None:
    dest_dir = config['dest_dir']
    os.makedirs(dest_dir, exist_ok=True)

    old_file = _find_old_files(name, config)
    backup_time = datetime.datetime.now().strftime(r'%Y%m%d%H%M%S')
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
                src_db.backup(dest_db)
    except:
        if os.path.exists(db_name_tmp):
            os.remove(db_name_tmp)
        raise

    os.rename(db_name_tmp, db_name_fin)
    for old in old_file:
        os.remove(os.path.join(config['dest_dir'], old))

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
        config_name: Annotated[str, typer.Argument()] = None
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
            config = profile_content[config_name]
            preprocess_config(config, profile_path_str)
            backup_sqlite3(config_name, config)
        else:
            for key, config in profile_content.items():
                preprocess_config(config, profile_path_str)
                backup_sqlite3(key, config)

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
