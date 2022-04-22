import dataclasses
from typing import Any, Iterable
import sqlalchemy
{% for query in queries %}


class _{{query.name}}:
    @dataclasses.dataclass
    class Row:
        {% if query.return_columns %}
        {% for return_column in query.return_columns %}
        {{ return_column.name }}: Any
        {% endfor %}
        {% else %}
        pass
        {% endif %}
    
    _QUERY = sqlalchemy.text("{{query.query_string}}")
    
    def __call__(
        self,
        db: sqlalchemy.engine.Engine,
        {% for arg in query.args %}
        {{ arg.name }}: Any,
        {% endfor %}
    ) -> Iterable[Row]:
        query_result = db.execute(
            self._QUERY, 
            {
                {% for arg in query.args %}
                "{{ arg.name }}": {{ arg.name }},
                {% endfor %}
            },
        )
        return [self.Row(*row) for row in query_result]


{{query.name}} = _{{query.name}}()
{% endfor %}
