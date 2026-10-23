from typing import Optional


class AuthError(Exception):
    """
    Empty or invalid API key
    """

    pass


class BadResponse(Exception):
    """
    Non-200 response from API

    The exception message (``str(exc)`` and ``exc.args[0]``) is the response
    body, as before. The HTTP status code and the decoded body are also
    available as attributes, so callers can tell, for example, a 403 from a
    429 without parsing the message.

    :param message: The exception message, normally the response body.
    :param status: The HTTP status code of the response, if known.
    :param body: The decoded response body. Defaults to ``message``.
    :ivar status: The HTTP status code of the response, or ``None`` if the
        exception was raised without one.
    :ivar body: The decoded response body.
    """

    def __init__(
        self,
        message: str,
        *,
        status: Optional[int] = None,
        body: Optional[str] = None,
    ):
        super().__init__(message)
        self.status = status
        self.body = message if body is None else body
