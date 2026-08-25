import urllib.parse
import tomllib
import base64

class AttrDict(dict):
    def __init__(self, *args, **kwargs):
        super(AttrDict, self).__init__(*args, **kwargs)
        self.__dict__ = self
        for key, value in self.__dict__.items():
            if isinstance(value, dict):
                self.__dict__[key] = AttrDict(value)
            elif isinstance(value, (list, tuple)):
                if value and isinstance(value[0], dict):
                    self.__dict__[key] = [AttrDict(item) for item in value]
                else:
                    self.__dict__[key] = value


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

