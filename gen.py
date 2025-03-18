from nacl.signing import SigningKey
from nacl.signing import VerifyKey
import base58

message = '!!!!!!!'
message_bytes = bytes(message.encode("utf-8"))

for _ in range(10):
    signing_key = SigningKey.generate()
    signing_key_txt = base58.b58encode(signing_key.encode()).decode("utf-8")
    print('PRIV:', signing_key_txt)

    verify_key = signing_key.verify_key
    verify_key_txt = base58.b58encode(verify_key.encode()).decode("utf-8")
    print('PUB:', verify_key_txt)


signed = signing_key.sign(message_bytes)
signed_txt = base58.b58encode(signed).decode("utf-8")
print('SIGN:', signed_txt)

verify_key = VerifyKey(base58.b58decode(verify_key_txt))
v = verify_key.verify(base58.b58decode(signed_txt)).decode("utf-8")
print(v)
