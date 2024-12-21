import typing as t
from collections import deque

from ksuid import Ksuid

from fastactor.settings import settings


def id_generator(prefix: str = "") -> t.Callable[[], str]:
    def gen_id():
        return f"{prefix}:{Ksuid()}"

    return gen_id


def deque_factory(maxlen: int = settings.mailbox_size):
    def factory():
        return deque(maxlen=maxlen)

    return factory
