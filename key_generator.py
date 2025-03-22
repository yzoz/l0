# src/scripts/key_generator.py
from src.utils.crypto import generate_keys, sign_message, verify_message

message = '!!!!!!!'

for _ in range(10):
    priv_key, pub_key = generate_keys()
    print('PRIV:', priv_key)
    print('PUB:', pub_key)

signed_txt = sign_message(priv_key, message)
print('SIGN:', signed_txt)

verified = verify_message(pub_key, signed_txt)
print(verified)