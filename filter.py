from abc import ABC
from typing import Any
from pydantic import Field, BaseModel
from sqlalchemy import or_
from sqlalchemy.dialects import postgresql

from models.events import EventConsumer
from query.base import BaseQuery
from settings.db import get_sync_session


class FilterMeta(type):
    def __new__(cls, name, bases, dct):
        if name == 'Filter':
            instance = super().__new__(cls, name, bases, dct)
        else:
            __filter_meta__ = {}
            for k, v in dct.items():
                if isinstance(v, BaseQuery):
                    __filter_meta__[k] = dct[k]
                    if dct[k].field:
                        dct[k] = dct[k].field
                    else:
                        __filter_meta__[k] = dct.pop(k)
            dct['filter'] = cls.__filter__
            dct['__filter_meta__'] = __filter_meta__
            instance = type(name, (BaseModel,), dct)
        return instance

    def __filter__(self, query: 'Select'):
        q = []
        i = 0
        next_op = or_
        for name, obj in self.__filter_meta__.items():
            if i == 0:
                exp, next_op = obj.make(query, getattr(self, name), self.model_fields[name].annotation)
            else:
                l, last_exp = obj.make(query, getattr(self, name), self.model_fields[name].annotation)
                exp = next_op(exp, l)
                if last_exp:
                    next_op = last_exp
            i += 1
        session = get_sync_session()
        q = session.query(EventConsumer).filter(exp)
        bb = q.statement.compile(dialect=postgresql.dialect(),
                                 compile_kwargs={'literal_binds': True})
        print()
        s_obj = query.filter(*self.__make_query(query))
        return s_obj


class Filter(metaclass=FilterMeta):
    def __new__(cls, *args, **kwargs):
        return super().__new__(cls)
