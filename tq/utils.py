import urllib.parse
import tomllib
import base64

def load_toml(path):
    with open(path, 'rb') as f:
        config = tomllib.load(f)
    return config


def get_hash(magnet):
    parsed = urllib.parse.urlparse(magnet)
    params = urllib.parse.parse_qs(parsed.query)
    xt = params['xt'][0]
    hash = xt.removeprefix('urn:btih:')
    if len(hash) == 32:
        return base64.b16encode(base64.b32decode(hash)).decode().lower()
    elif len(hash) == 40:
        return hash.lower()
    else:
        raise ValueError(f"Hash length is {len(hash)}")

