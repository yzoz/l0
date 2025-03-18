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
