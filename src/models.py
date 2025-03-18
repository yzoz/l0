from typing import Optional
from pydantic import BaseModel

class Tx(BaseModel):
    credit: str
    debit: str
    asset: str
    amount: float
    uniq: str
    sign: str
    hash: Optional[str]
    msg: Optional[str]

class Token(BaseModel):
    creater: str
    amount: int
    tiker: str
    img: str
    title: str
    desc: str
    uniq: str
    sign: str
    hash: Optional[str]
    time: Optional[int]

class NFT(BaseModel):
    creater: str
    img: str
    title: str
    desc: str
    uniq: str
    sign: str
    hash: Optional[str]
    time: Optional[int]