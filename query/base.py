from __future__ import annotations

import datetime
import decimal
from abc import ABC, abstractmethod
from inspect import Attribute
from typing import Any, ClassVar

from exception.base import CheckOptionError, NotFieldsFoundError
from models.events import EventConsumer
from psycopg2.extensions import JSONB
from pydantic import Field
from settings.db import get_sync_session
from sqlalchemy import (
    ARRAY,
    BigInteger,
    Boolean,
    Date,
    DateTime,
    Float,
    Interval,
    LargeBinary,
    Numeric,
    Select,
    String,
    Time,
    cast,
)
from sqlalchemy.dialects import postgresql

_type_py2sql_dict = {
    int: BigInteger,
    str: String,
    float: Float,
    decimal.Decimal: Numeric,
    datetime.datetime: DateTime,
    bytes: LargeBinary,
    bool: Boolean,
    datetime.date: Date,
    datetime.time: Time,
    datetime.timedelta: Interval,
    list: ARRAY,
    dict: JSONB
}


class AbstractBase(ABC):
    """"lala.

    :param
    """

    options: ClassVar

    @abstractmethod
    def __check_options(self, value: list) -> str:
        """Check types in options."""
        raise NotImplementedError

    @abstractmethod
    def __get_model(self, query: Select) -> list[dict]:
        """Get model."""
        raise NotImplementedError

    @abstractmethod
    def __get_field(self, model: dict, param: str) -> Attribute:
        pass

    @abstractmethod
    def __get_json_field(self, models: list[dict], path: str) -> dict | None:
        pass

    @abstractmethod
    def make(self, query: Select, value: Any, annotation: Any) -> Any:  # noqa: ANN401
        """Base class for all queries types.

        :param value:
        :param annotation:
        :param query: select query.
        """


class BaseQuery(AbstractBase, ABC):
    """Base class for all queries types."""

    options: ClassVar = ['operator', 'alchemy_type', 'path', 'inner_op', 'outer_op', 'query_alias']

    def __init__(self, *args: Any, **kwargs: Any):  # noqa: ANN401
        """pass.

        :param.
        """
        fields: dict = {}
        self.meta: dict = {}
        for key in kwargs:
            if key in self.options:
                self.meta[key] = kwargs[key]
            else:
                fields[key] = kwargs[key]
        self.field = Field(*args, **fields)

    def __check_options(self, value: list) -> None:
        if self.meta.get('inner_op') and isinstance(value, list):
            return
        raise CheckOptionError

    def __get_model(self, query: Select) -> dict[Any, dict[str, list[Any] | Any]]:
        models = {}
        for column in query.column_descriptions:
            models[column.get('name')] = {
                'fields': [name for name, value in column.get('entity').__dict__.items() if
                           not name.startswith('__') and name != '_sa_class_manager'],
                'model': column.get('type'),
            }
        return models

    def __get_field(self, model: dict, param: str) -> Attribute:
        if param in model.get('fields'):
            return getattr(model.get('model'), param)
        raise NotFieldsFoundError

    def __get_json_field(self, models: list[dict], path: str) -> dict | None:
        parents = path
        attr = None
        for values in models.values():
            if parents[0] in values.get('fields'):
                for parent in parents:
                    attr = getattr(values.get('model'), parent) if attr is None else attr[parent]
        return attr

    def make(self, query: Select, value: Any, annotation: Any) -> Any:
        # if isinstance(value, list):
        #     conditions.append(Detail.sellers.ilike('%{}%'.format(name)))
        # generic = get_args(annotation)
        m = self.__get_model(query)
        op = self.meta.get('operator')
        alchemy_type = self.meta.get('alchemy_type')
        inner_op = self.meta.get('inner_op')
        outer_op = self.meta.get('outer_op')
        path = self.meta.get('path').split('.')
        exp = []
        if len(path) > 1:
            # json_field = self.__get_json_field(m, path, generic[0] if generic else annotation)
            json_field = self.__get_json_field(m, path, int)
            if isinstance(value, list):
                for i, v in enumerate(value):
                    if i >= 1:
                        exp = inner_op(exp, op(cast(json_field, alchemy_type), v))
                    else:
                        exp = op(cast(json_field, alchemy_type), v)
                session = get_sync_session()
                return exp, outer_op
                q = session.query(EventConsumer).filter(exp)
                bb = q.statement.compile(dialect=postgresql.dialect(),
                                         compile_kwargs={'literal_binds': True})
                print()
                ll = self.meta.get('inner_op')(*exp)
            print()
        if self.meta.get('inner_op') and isinstance(value, list):
            pass
        l = value
        c = annotation
        # BooleanClauseList.or_(*clauses)
        print()
        return 'select * from public.consumer_event_predict'
