# src/utils/crypto.py
from nacl.signing import SigningKey, VerifyKey
from nacl.encoding import HexEncoder
import base58
from hashlib import sha512

def generate_keys():
    signing_key = SigningKey.generate()
    signing_key_txt = base58.b58encode(signing_key.encode()).decode("utf-8")
    verify_key = signing_key.verify_key
    verify_key_txt = base58.b58encode(verify_key.encode()).decode("utf-8")
    return signing_key_txt, verify_key_txt

def sign_message(signing_key_txt: str, message: str) -> str:
    priv = base58.b58decode(signing_key_txt)
    signing_key = SigningKey(priv)
    message_bytes = sha512(message.encode("utf-8")).digest()
    signed = signing_key.sign(message_bytes, encoder=HexEncoder)
    return signed.decode("utf-8")

def verify_message(verify_key_txt: str, signed_txt: str) -> str:
    verify_key = VerifyKey(base58.b58decode(verify_key_txt))
    return verify_key.verify(HexEncoder.decode(signed_txt))