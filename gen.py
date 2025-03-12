from nacl.encoding import HexEncoder
from nacl.signing import SigningKey
from nacl.signing import VerifyKey
from binascii import unhexlify
from nacl.hash import blake2b
import hashlib

message = '3e2eb8467505b6bd56033841ff8aeedfc3d9da0f64f16b84910a68a7b1855b8649700da9fa6bb1ba5177e072b54e83372b5af36a8812cb85948abacbc2e1bcb421000000000000living in believing1647751749519'
message_bytes = bytes(message.encode("utf-8"))

h = hashlib.blake2b()
h.update(message_bytes)
h2 = h.digest().hex()
h2 = bytes(h2.encode("utf-8"))
h = hashlib.blake2b()
h.update(h2)
print(h.digest().hex())

print(message_bytes)

for _ in range(10):
    signing_key = SigningKey.generate()
    signing_key_hex = signing_key.encode(encoder=HexEncoder)
    signing_key_hex_txt = signing_key_hex.decode("utf-8")
    print('PRIV:', signing_key_hex_txt)
    #sk = unhexlify('aea5ca02b340b13611c52208c16369a2cb7a6ead06ff29ee56464961e2b52de5')
    #signing_key = SigningKey(sk)
    #print('PRIV:', signing_key)

    verify_key = signing_key.verify_key
    verify_key_hex = verify_key.encode(encoder=HexEncoder)
    verify_key_hex_txt = verify_key_hex.decode("utf-8")
    print('PUB:', verify_key_hex_txt)

signed_hex = signing_key.sign(message_bytes, encoder=HexEncoder)
signed_hex_txt = signed_hex.decode("utf-8")
print('SIGN:', signed_hex_txt)

verify_key_hex_bytes = bytes(verify_key_hex_txt.encode("utf-8"))
verify_key = VerifyKey(verify_key_hex_bytes, encoder=HexEncoder)
signed_hex_bytes = bytes(signed_hex_txt.encode("utf-8"))
v = verify_key.verify(signed_hex_bytes, encoder=HexEncoder)
print(v.decode("utf-8"))