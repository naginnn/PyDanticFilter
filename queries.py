import datetime
import decimal
from abc import ABC
from typing import Any, Iterable, get_args

from psycopg2.extensions import JSONB
from pydantic import Field
from sqlalchemy import BooleanClauseList, BinaryExpression, cast, String, BigInteger, Unicode, Numeric, Float, DateTime, \
    LargeBinary, Boolean, Date, Time, Interval, ARRAY
from sqlalchemy.dialects import postgresql
from sqlalchemy.sql import expression
from sqlalchemy.sql.operators import custom_op, OperatorType, eq

from models.events import EventConsumer
from settings.db import get_sync_session

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


class BaseQuery(ABC):
    """Base class for all queries types"""
    options: list[str] = ['operator', 'alchemy_type', 'path', 'inner_op', 'outer_op', 'query_alias']

    def __init__(self, *args, **kwargs):
        ## реализовать проверку операторов __check_options
        fields = {}
        self.meta = {}
        for key in kwargs.keys():
            if key in self.options:
                self.meta[key] = kwargs[key]
            else:
                fields[key] = kwargs[key]
        self.field = Field(*args, **fields)

    def __check_options(self, value):
        if self.meta.get('inner_op') and isinstance(value, list):
            return
        raise Exception

    def __get_model(self, query: 'Select'):
        models = {}
        for column in query.column_descriptions:
            models[column.get('name')] = {
                'fields': [name for name, value in column.get('entity').__dict__.items() if
                           not name.startswith('__') and name != '_sa_class_manager'],
                'model': column.get('type'),
            }
        return models

    def __get_field(self, model, param):
        if param in model.get('fields'):
            attr = getattr(model.get('model'), param)
            return attr

    def __get_json_field(self, models, path, annotation):
        parents = path
        attr = None
        for name, values in models.items():
            if parents[0] in values.get('fields'):
                for parent in parents:
                    if attr is None:
                        attr = getattr(values.get('model'), parent)
                    else:
                        attr = attr[parent]
                return attr

    def __create_exp(self, param, values, option):
        if not values:
            return
        values = values.split(',')
        options = option.replace(' ', '').split(',')
        params = []
        param = cast(param, String)
        buf = None
        for option in options:
            for value in values:
                if not value:
                    continue
                if option == 'like':
                    buf = param.like('{}'.format(value))
                elif option == 'ilike':
                    buf = param.ilike('%{}%'.format(value))
                elif option == 'in':
                    buf = param.in_(**values)
                elif option == '==':
                    buf = param == value
                elif option == '!=':
                    buf = param != value
                elif option == '>=':
                    buf = param >= value
                elif option == '<=':
                    buf = param <= value
                if buf is not None:
                    params.append(buf)
        return params

    def make(self, query: Any, value: Any, annotation: Any):
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
        return "select * from public.consumer_event_predict"


class SlugQuery(BaseQuery):
    # options = ['path', 'inner_op']
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
