from typing import Optional
from pydantic import BaseModel, PositiveInt

class Tx(BaseModel):
    mom: str
    dad: str
    credit: str
    debit: str
    amount: PositiveInt
    uniq: str
    msg: Optional[str] = ""

class TxRead(BaseModel):
    content: Tx
    sign: str

class TxStore(BaseModel):
    mom: str
    dad: str
    credit: str
    debit: str
    amount: int
    uniq: str
    msg: Optional[str] = ""
    sign: str

class TxWrite(BaseModel):
    mom: str
    dad: str
    credit: str
    debit: str
    amount: int
    uniq: str
    msg: Optional[str] = ""
    sign: str
    hash: str
    v: Optional[int] = 1
    time: int
    

class TxSend(BaseModel):
    credit: str
    debit: str
    amount: float
    uniq: str
    sign: str
    msg: str
    v: int
    time: float
    hash: str