from sqlalchemy.event import Events

from filter import Filter
from pydantic import Field, model_validator

from models.events import EventConsumer
from queries import SlugQuery
from sqlalchemy import String, select
from sqlalchemy.sql.operators import and_, le, or_


class Byk(Filter):  # noqa: D101
    test: list[str] = SlugQuery(
        path='source.info.data',
        alchemy_type=String,
        operator=le,
        inner_op=or_,
        outer_op=and_,
        required=True)
    baba: list[str] | None = SlugQuery(
        path='source.info.baba',
        alchemy_type=String,
        operator=le,
        inner_op=or_,
        required=True, default=None)
    name: str | None = Field(default=None)
    value: str | None = Field(default=None)

    @model_validator(mode='after')
    def root_validator(cls, values):  # noqa: ANN201, ANN001
        return values


if __name__ == '__main__':
    byk = Byk(test=['slug.query'])
    byk.filter(select(EventConsumer))
