# src/utils/hash.py
from hashlib import sha512

def hash_hex(t: str) -> str:
    return sha512(t.encode('utf-8')).hexdigest()

def ordered_sum(t: dict) -> str:
    s = dict(sorted(t.items()))
    m = ''
    for t in s.values():
        if isinstance(t, list): t = "".join(sorted(t))
        m += str(t)
    print(m)
    return m