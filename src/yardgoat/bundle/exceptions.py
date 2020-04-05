class MissingKeyError(KeyError):
    pass


class InvalidKeyError(KeyError):
    pass


__all__ = ["InvalidKeyError", "MissingKeyError"]
