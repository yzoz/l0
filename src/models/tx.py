from typing import Optional, Annotated, List
from pydantic import BaseModel, PositiveInt
from pydantic.types import MinLen, MaxLen
from pydantic.functional_validators import AfterValidator

# Функция валидации непустой строки
def check_non_empty(value: str) -> str:
    if not value.strip():
        raise ValueError("Строка не может быть пустой")
    return value

# Определяем тип NonEmptyStr
NonEmptyStr = Annotated[str, AfterValidator(check_non_empty)]
class Tx(BaseModel):
    parents: Annotated[List[NonEmptyStr], MinLen(2), MaxLen(10)]
    credit: str
    debit: str
    amount: PositiveInt
    uniq: str
    msg: Optional[str] = ""

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