from typing import Optional
from pydantic import BaseModel

class Tx(BaseModel):
    credit: str
    debit: str
    asset: str
    amount: int
    uniq: str
    msg: Optional[str] = ""

class TxRead(BaseModel):
    content: Tx
    sign: str

class TxWrite(BaseModel):
    credit: str
    debit: str
    asset: str
    amount: int
    uniq: str
    msg: Optional[str] = ""
    sign: Optional[str] = ""
    v: Optional[int] = 1
    time: Optional[int] = 0
    hash: Optional[str] = ""

class TxSend(BaseModel):
    credit: str
    debit: str
    asset: str
    amount: float
    uniq: str
    sign: str
    msg: str
    v: int
    time: float
    hash: str