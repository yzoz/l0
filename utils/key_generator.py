from nacl.signing import SigningKey
import base58

def generate_keys():
    signing_key = SigningKey.generate()
    signing_key_txt = base58.b58encode(signing_key.encode()).decode("utf-8")
    verify_key = signing_key.verify_key
    verify_key_txt = base58.b58encode(verify_key.encode()).decode("utf-8")
    return signing_key_txt, verify_key_txt

for _ in range(10):
    priv_key, pub_key = generate_keys()
    print('PRIV:', priv_key)
    print('PUB:', pub_key)

print(base58.b58encode(bytes([0] * 32)).decode("utf-8"))