import dataclasses
from typing import Any, Iterable
import sqlalchemy


class _simple_query:
    @dataclasses.dataclass
    class Row:
        name: Any
    
    _QUERY = sqlalchemy.text("SELECT name  FROM user  WHERE user_id = :user_id (int);")
    
    def __call__(
        self,
        db: sqlalchemy.engine.Engine,
        user_id: Any,
    ) -> Iterable[Row]:
        query_result = db.execute(
            self._QUERY, 
            {
                "user_id": user_id,
            },
        )
        return [self.Row(*row) for row in query_result]


simple_query = _simple_query()

