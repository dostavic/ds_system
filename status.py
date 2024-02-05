from enum import Enum


class Status(Enum):
    NOT_DEFINED = 1
    TRUE = 2
    FALSE = 3


class State(Enum):
    PRE_PREPARE = 1
    PREPARE = 2
    COMMITTED = 3
    INSERTED = 4
    NOT_DEFINED = 5
