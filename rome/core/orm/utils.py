"""Utils module.

This module contains functions that are used by Rome's queries to understand SQLAlchemy's queries.

All the code of this module is inspired from the following stackoverflow question:
  http://stackoverflow.com/questions/5631078/sqlalchemy-print-the-actual-query

"""

from sqlalchemy.engine.default import DefaultDialect
from sqlalchemy.sql.sqltypes import String, DateTime, NullType


class StringLiteral(String):
    """Teach SA how to literalize various things."""

    def literal_processor(self, dialect):
        # python2/3 compatible.
        is_python3 = str is not bytes
        text_type = str if is_python3 else unicode
        int_type = int if is_python3 else (int, long)
        str_type = str if is_python3 else (str, unicode)

        super_processor = super(StringLiteral, self).literal_processor(dialect)

        def process(value):
            if isinstance(value, int_type):
                return text_type(value)
            if not isinstance(value, str_type):
                value = text_type(value)
            result = super_processor(value)
            if isinstance(result, bytes):
                result = result.decode(dialect.encoding)
            return result

        return process


class LiteralDialect(DefaultDialect):
    """Dialect that will enable to literalize datetime and NullType values."""

    colspecs = {
        # prevent various encoding explosions
        String: StringLiteral,
        # teach SA about how to literalize a datetime
        DateTime: StringLiteral,
        # don't format py2 long integers to NULL
        NullType: StringLiteral,
    }


def get_literal_query(statement):
    """
    Extracts the SQL query corresponding to a SQLAlchemy query.

    NOTE: This is entirely insecure. DO NOT execute the resulting strings.
    :param statement: a query object
    :return: a string which corresponds to the corresponding SQL query
    """
    import sqlalchemy.orm
    if isinstance(statement, sqlalchemy.orm.Query):
        statement = statement.selectable
    return statement.compile(dialect=LiteralDialect(),
                             compile_kwargs={'literal_binds': True},).string
