from typing import Optional, Annotated, List
from pydantic import BaseModel, PositiveInt
from pydantic.types import MinLen, MaxLen
from pydantic.functional_validators import AfterValidator
from nacl.signing import VerifyKey
import base58

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
    value = base58.b58decode(value)
    if len(value) != 32:
        raise ValueError("Public key must be 32 bytes")
    try:
        VerifyKey(value)
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
    v: Optional[int] = 1
    time: int
    

class TxSend(TxWrite):
    amount: float
    time: float