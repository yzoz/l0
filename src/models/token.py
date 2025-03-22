from typing import Optional
from pydantic import BaseModel

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

