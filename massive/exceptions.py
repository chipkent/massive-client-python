from typing import Optional

# the package star-imports this module; keep Optional out of its namespace
__all__ = ["AuthError", "BadResponse"]


class AuthError(Exception):
    """
    Empty or invalid API key
    """

    pass


class BadResponse(Exception):
    """
    Non-200 response from API, or a 200 response whose body is not valid JSON

    The message is the response body; for invalid JSON it is "Invalid JSON in
    response: " followed by the first 200 bytes of the body. ``status`` is the
    HTTP status code, or None if not given.
    """

    def __init__(self, *args: object, status: Optional[int] = None):
        super().__init__(*args)
        self.status = status
