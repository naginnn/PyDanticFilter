from query.base import BaseQuery


class SlugQuery(BaseQuery):
    # options = ['path', 'inner_op']
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
