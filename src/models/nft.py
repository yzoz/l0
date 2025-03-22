from typing import Optional
from pydantic import BaseModel

class NFT(BaseModel):
    creater: str
    img: str
    title: str
    desc: str
    uniq: str
    sign: str
    hash: Optional[str]
    time: Optional[int]