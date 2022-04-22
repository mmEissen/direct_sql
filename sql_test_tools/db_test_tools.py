import dataclasses
import contextlib
import subprocess
import time
from typing import Optional
import sqlalchemy
import sqlalchemy.pool
import click

import sys

class BrokenDatabaseCommand(Exception):
    pass


SHORT_TIME = 0.05  # seconds


@dataclasses.dataclass
class DatabaseProcess:
    name: str
    process: subprocess.Popen


@dataclasses.dataclass
class TestDBPool:
    connection_string: str
    pool_size: int = 3

    def __post_init__(self) -> None:
        db_names = [f"__test_db_{i}" for i in range(self.pool_size)]
        self._database_processes = [
            DatabaseProcess(db_name, subprocess.Popen(
                [sys.executable, __file__, "reset", self.connection_string, db_name]
            ))
            for db_name in db_names
        ]

    def _wait_for_db(self, timeout: float=30) -> Optional[DatabaseProcess]:
        start = time.time()
        while True:
            for db_process in self._database_processes:
                if db_process.process.poll() is not None:
                    if db_process.process.returncode != 0:
                        raise BrokenDatabaseCommand()
                    return db_process
            if time.time() - start > timeout:
                raise BrokenDatabaseCommand()
            time.sleep(SHORT_TIME)

    @contextlib.contextmanager
    def isolated_db(self) -> str:
        ready_db = self._wait_for_db()
        yield ready_db.name
        ready_db.process = subprocess.Popen(
            [sys.executable, __file__, "reset", self.connection_string, ready_db.name]
        )
    
    def cleanup(self) -> None:
        for db in self._database_processes:
            db.process.kill()
            subprocess.Popen(
                [sys.executable, __file__, "delete", self.connection_string, db.name]
            )


@contextlib.contextmanager
def db_test_pool(connection_string: str, pool_size: int=3):
    db_pool = TestDBPool(connection_string, pool_size)
    yield db_pool
    db_pool.cleanup()
    


def create_engine(db_url: str) -> sqlalchemy.engine.Engine:
    return sqlalchemy.create_engine(db_url + "/postgres", poolclass=sqlalchemy.pool.NullPool)


@click.group()
def cli() -> None:
    pass


def _drop_db(connection: sqlalchemy.engine.Connection, db_name: str) -> None:
    connection.execute(sqlalchemy.text(f"""
        SELECT pg_terminate_backend(pg_stat_activity.pid)
        FROM pg_stat_activity
        WHERE pg_stat_activity.datname = '{db_name}'
        AND pid <> pg_backend_pid()
    """))
    connection.execute(sqlalchemy.text(f"""
        DROP DATABASE IF EXISTS {db_name}
    """))


@cli.command()
@click.argument("db-url", type=str)
@click.argument("db-name", type=str)
def reset(db_url: str, db_name: str) -> None:
    db = create_engine(db_url)
    with db.begin() as connection:
        connection.execute(sqlalchemy.text("COMMIT"))
        _drop_db(connection, db_name)
        connection.execute(sqlalchemy.text(f"""
            CREATE DATABASE {db_name}
        """))
    db.dispose()


@cli.command()
@click.argument("db-url", type=str)
@click.argument("db-name", type=str)
def delete(db_url: str, db_name: str) -> None:
    db = create_engine(db_url)
    with db.begin() as connection:
        connection.execute(sqlalchemy.text("COMMIT"))
        _drop_db(connection, db_name)
    db.dispose()


if __name__ == "__main__":
    cli()
