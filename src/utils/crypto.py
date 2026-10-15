# src/utils/crypto.py
from nacl.signing import SigningKey, VerifyKey
from nacl.encoding import HexEncoder
import base58
from hashlib import sha512

def sign_message(signing_key_txt: str, message: str) -> str:
    priv = base58.b58decode(signing_key_txt)
    signing_key = SigningKey(priv)
    message_bytes = sha512(message.encode("utf-8")).digest()
    signed = signing_key.sign(message_bytes, encoder=HexEncoder).signature
    return signed.decode("utf-8")

def verify_message(verify_key_txt: str, signed_txt: str , msg: bytes) -> bool:
    verify_key = VerifyKey(base58.b58decode(verify_key_txt))
    return verify_key.verify(msg, HexEncoder.decode(signed_txt))