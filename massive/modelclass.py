import inspect
import sys
import typing
from dataclasses import dataclass

if typing.TYPE_CHECKING:
    if sys.version_info >= (3, 11):
        from typing import dataclass_transform
    else:
        from typing_extensions import dataclass_transform
else:

    def dataclass_transform(**kwargs):
        return lambda f: f


_T = typing.TypeVar("_T")


@dataclass_transform()
def modelclass(cls: typing.Type[_T]) -> typing.Type[_T]:
    cls = dataclass(cls)
    type_hints = typing.get_type_hints(cls)
    attributes = [
        a
        for a in type_hints.keys()
        if not a.startswith("__") and not inspect.isroutine(getattr(cls, a, None))
    ]

    def init(self, *args, **kwargs):
        for i, a in enumerate(args):
            if i < len(attributes):
                self.__dict__[attributes[i]] = a
        for k, v in kwargs.items():
            if k in attributes:
                self.__dict__[k] = v

    cls.__init__ = init  # type: ignore[assignment]

    return cls
