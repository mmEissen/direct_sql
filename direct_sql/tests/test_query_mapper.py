import pytest
import os.path as path
from query_function_mapper import query_mapper


@pytest.fixture
def setup_name():
    return None


@pytest.fixture
def setup_dir(data_dir, setup_name):
    return path.join(data_dir, setup_name)


@pytest.fixture
def queries_dir(setup_dir):
    return path.join(setup_dir, "queries")


@pytest.fixture
def schema_loaded(db, setup_dir):
    filename = path.join(setup_dir, "schema.sql")
    with open(filename) as schema:
        content = schema.read()
    queries = content.strip().split(";")
    for query in queries:
        if not query.strip():
            continue
        db.execute(query)


@pytest.mark.parametrize(
    "setup_name",
    [
        pytest.param("simple_setup")
    ]
)
def test_dynamic_load(queries_dir):
    queries = query_mapper.dynamic_load(queries_dir)

    assert hasattr(queries, "simple_query")


@pytest.mark.parametrize(
    "setup_name",
    [
        pytest.param("simple_setup")
    ]
)
def test_simple_query(queries_dir, schema_loaded, db):
    queries = query_mapper.dynamic_load(queries_dir)

    result = queries.simple_query(db, "sam")

    assert len(result) == 1
    sam = result[0]
    assert sam.name == "sam"
    assert sam.age == 30
