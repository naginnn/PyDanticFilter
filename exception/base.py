from abc import ABC


class BaseError(Exception, ABC):
    """BaseQueryError."""

    def __init__(self, m: str):
        """Init exception.

        :param m: error information.
        """
        self.m = m


class ValidateOptionError(BaseError):
    """Check options fields error."""


class CheckOptionError(BaseError):
    """Check options fields error."""


class NotFieldsFoundError(BaseError):
    """Check options fields error."""
