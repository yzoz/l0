from nacl.signing import VerifyKey
import base58

m = "ZAjPpYqRToeE6dkXFNdA8XMEZHC8R3Wo12255ZFDWDAiqkWZrNVt2Y3FoNqBhv2VYHDc98J94yyVTuedFw9X2i8SDpju2hbxb"
snd = "EK8mNnU4aFe5AdhrrbE3A1nveU7uc4Ttn2NkmaXZgWzb"

verify_key = VerifyKey(base58.b58decode(snd))
v = verify_key.verify(base58.b58decode(m))

print(v.decode("utf-8"))