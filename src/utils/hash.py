# src/utils/hash.py
import hashlib
import base58

def tob2b(t: str) -> str:
    h = hashlib.sha512()
    h.update(bytes(t.encode("utf-8")))
    return base58.b58encode(h.digest()).decode("utf-8")