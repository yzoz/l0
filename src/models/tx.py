"""Модели транзакций.

Конвенция credit/debit: ``credit`` — плательщик (отправитель средств),
``debit`` — получатель. Имена сохранены ради совместимости с существующими
данными; при чтении кода помните об этой особенности.
"""

from typing import Annotated, List, Optional

import base58
from nacl.signing import VerifyKey
from pydantic import BaseModel, PositiveInt
from pydantic.functional_validators import AfterValidator
from pydantic.types import MaxLen, MinLen


# Функция валидации непустой строки
def check_non_empty(value: str) -> str:
    if not value.strip():
        raise ValueError("Not empty!")
    return value


# Функция валидации уникальности
def unique_elements(value: List) -> List:
    if len(value) != len(set(value)):
        raise ValueError("All elements in the list must be unique!")
    return value


# Не самому себе
def not_equal_to_credit(value, info):
    if "credit" in info.data and value == info.data["credit"]:
        raise ValueError("Not for yourself!")
    return value


# Ключ подходит для Ed25519
def check_valid_key(value: str):
    b = base58.b58decode(value)
    if len(b) != 32:
        raise ValueError("Public key must be 32 bytes")
    try:
        VerifyKey(b)
    except ValueError as e:
        raise ValueError("Invalid Ed25519 public key") from e
    return value


# Определяем тип NonEmptyStr
NonEmptyStr = Annotated[str, AfterValidator(check_non_empty)]
ValidKey = Annotated[str, AfterValidator(check_valid_key)]


class Tx(BaseModel):
    parents: Annotated[List[NonEmptyStr], MinLen(2), MaxLen(10), AfterValidator(unique_elements)]
    credit: str
    debit: Annotated[ValidKey, AfterValidator(not_equal_to_credit)]
    amount: PositiveInt
    uniq: str
    msg: Optional[Annotated[str, MaxLen(10)]] = ""


class TxRead(BaseModel):
    content: Tx
    sign: str


class TxStore(Tx):
    sign: str


class TxWrite(TxStore):
    hash: str
    # v=2: формат сериализации ordered_sum с разделителями (см. src/utils/data.py)
    v: Optional[int] = 2
    time: int


class TxSend(TxWrite):
    amount: float
    time: float
