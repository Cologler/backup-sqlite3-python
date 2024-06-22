# backup-sqlite3

backup and restore sqlite3 database with profile.

## Usage

```shell
# backup all jobs:
backup-sqlite3 backup --profile ./profile.yaml

# backup one job:
backup-sqlite3 backup --profile ./profile.yaml [${jobname}]

# also support restore:
backup-sqlite3 restore ...
```

Use `--help` option for see more.

### Profile format

A profile can includes many jobs:

```yaml
${jobname}:
  db_path:  "..." # source database path
  dest_dir: "..." # dest location
  retention: 1 # optional value for how many backups to keep
  interval: 1800 # optional seconds for backup only if the previous backup is outdated
```

The job will create `${jobname}.${time}.sqlite3` on dest location.

## Continuous Backup

If you want to continuous backup with a background service, try [litestream](https://litestream.io/).
