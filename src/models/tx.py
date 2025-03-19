from typing import Optional
from pydantic import BaseModel

class Tx(BaseModel):
    credit: str
    debit: str
    asset: str = "0"
    amount: float
    uniq: str
    sign: str
    msg: Optional[str] = ""
    v: Optional[int] = 1
    time: Optional[int] = 0
    hash: Optional[str] = "0"
