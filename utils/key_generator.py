from nacl.signing import SigningKey
import base58

def generate_keys():
    signing_key = SigningKey.generate()
    signing_key_txt = base58.b58encode(signing_key.encode()).decode("utf-8")
    verify_key = signing_key.verify_key
    verify_key_txt = base58.b58encode(verify_key.encode()).decode("utf-8")
    return signing_key_txt, verify_key_txt

priv_keys = []
pub_keys = []
for _ in range(25):
    priv_key, pub_key = generate_keys()
    priv_keys.append(priv_key)
    pub_keys.append(pub_key)

print(priv_keys)
print(pub_keys)

#print(base58.b58encode(bytes([0] * 32)).decode("utf-8"))