import pytest

import db_test_tools
from os import path
import sqlalchemy


@pytest.fixture(scope="session")
def database_server_str():
    return "postgresql://postgres:postgres@localhost:5432"


@pytest.fixture(autouse=True, scope="session")
def db_test_pool(database_server_str):
    with db_test_tools.db_test_pool(database_server_str, pool_size=20) as pool:
        yield pool


@pytest.fixture()
def db(db_test_pool: db_test_tools.TestDBPool, database_server_str):
    with db_test_pool.isolated_db() as db_name:
        yield sqlalchemy.create_engine(f"{database_server_str}/{db_name}")


@pytest.fixture()
def data_dir() -> str:
    return path.join(path.dirname(__file__), "data")
