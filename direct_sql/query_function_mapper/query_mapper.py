import dataclasses
from typing import Any
import sqlparse
import jinja2
import os.path as path
import os
import importlib.util
import tempfile

TEMPLATES_DIR = path.join(path.dirname(__file__), "_templates")

query1 = "WITH sq AS (SELECT * FROM x) SELECT name, age FROM user WHERE user_id = :user_id"
query2 = "INSERT INTO foo (x, y) VALUES (1, 2) RETURNING x"

parsed = sqlparse.parse(query1)[0]

class ParseError(Exception): pass


@dataclasses.dataclass()
class ReturnColumn:
    name: str


@dataclasses.dataclass()
class QueryArg:
    name: str


@dataclasses.dataclass()
class Query:
    name: str
    query_string: str
    return_columns: list[ReturnColumn]
    args: list[QueryArg]


def render_module(queries: list[Query]) -> str:
    env = jinja2.Environment(
        loader=jinja2.FileSystemLoader(TEMPLATES_DIR),
        autoescape=jinja2.select_autoescape(),
        trim_blocks=True,
        lstrip_blocks=True,
    )
    template = env.get_template("queries.template.py")
    return template.render(queries=queries)


def get_first_identifiers(token_list):
    for token in token_list:
        if isinstance(token, sqlparse.sql.Identifier):
            return [token]
        if isinstance(token, sqlparse.sql.IdentifierList):
            return list(token.get_identifiers())
    raise ParseError()


def from_dml(name: str, statement: sqlparse.sql.Statement) -> Query:
    tokens = statement.tokens
    for dml_index, token in enumerate(tokens):
        if token.ttype == sqlparse.tokens.DML:
            break
    else:
        raise ParseError()

    identifiers = get_first_identifiers(tokens[dml_index:])
    return_columns = [
        ReturnColumn(identifier.get_name())
        for identifier in identifiers
    ]

    placeholders = [
        token for token in statement.flatten() 
        if token.ttype == sqlparse.tokens.Name.Placeholder
    ]
    arg_names = {placeholder.value[1:] for placeholder in placeholders}
    args = [QueryArg(name) for name in sorted(arg_names)]

    return Query(
        name=name,
        query_string=str(statement).replace("\"", "\\\"").replace("\n", " "),
        return_columns=return_columns,
        args=args,
    )


def load_file(filename: str) -> Query:
    with open(filename) as file_:
        content = file_.read()
    statements = sqlparse.parse(content)
    if not statements:
        raise ParseError(f"No SQL statements in {filename}")
    if len(statements) > 1:
        raise ParseError(f"More than one SQL statement in {filename}")
    statement = statements[0]
    if any(token.ttype == sqlparse.tokens.DML for token in statement.tokens):
        return from_dml(path.basename(filename)[:-len(SQL_FILE_EXTENTION)], statement)
    raise NotImplemented()


SQL_FILE_EXTENTION = ".sql"

def from_queries(directory: str) -> str:
    queries = []
    for filename in os.listdir(directory):
        if (
            not filename.endswith(SQL_FILE_EXTENTION)
            or not path.isfile(path.join(directory, filename))
        ):
            continue
        queries.append(load_file(path.join(directory, filename)))
    return render_module(queries)


def dynamic_load(directory: str) -> None:
    with tempfile.TemporaryDirectory() as temp_dir:
        filename = path.join(temp_dir, "queries.py")
        with open(filename, "w") as module_file:
            module_file.write(from_queries(directory))
        spec = importlib.util.spec_from_file_location("queries", filename)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
    return module



def main() -> None:
    print(
        from_queries(path.join(path.dirname(__file__), "queries"))
    )


if __name__ == "__main__":
    main()
