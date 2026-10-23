from typing import Optional

__all__ = ["AuthError", "BadResponse"]


class AuthError(Exception):
    """
    Empty or invalid API key
    """

    pass


class BadResponse(Exception):
    """
    Non-200 response from API

    The exception message (``str(exc)`` and ``exc.args``) is the response body,
    as before. The HTTP status code is also available as an attribute, so
    callers can tell, for example, a 403 from a 429 without parsing the message.

    :param args: The exception arguments, passed to ``Exception`` unchanged.
        The client passes one argument, the decoded response body.
    :param status: The HTTP status code of the response, if known.
    :ivar status: The HTTP status code of the response, or ``None`` if the
        exception was raised without one.
    """

    def __init__(self, *args: object, status: Optional[int] = None):
        super().__init__(*args)
        self.status = status
