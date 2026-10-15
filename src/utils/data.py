# src/utils/hash.py
from hashlib import sha512

# Разделители сериализации: значения полей разделяются US (0x1f),
# элементы списков — RS (0x1e). Без разделителей склейка допускала
# теоретические коллизии — границы строк «скользили».
FIELD_SEP = "\x1f"
ITEM_SEP = "\x1e"


def hash_hex(t: str) -> str:
    return sha512(t.encode("utf-8")).hexdigest()


def ordered_sum(t: dict) -> str:
    s = dict(sorted(t.items()))
    parts = []
    for v in s.values():
        if isinstance(v, list):
            v = ITEM_SEP.join(str(x) for x in v)
        parts.append(str(v))
    return FIELD_SEP.join(parts)
