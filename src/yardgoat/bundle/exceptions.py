class MissingKeyError(KeyError):
    """Raised when a required key is missing from a Bundle config file."""

    pass


class InvalidKeyError(KeyError):
    """Raised when an unexpected key is present in a Bundle config file."""

    pass


class BundleConfigNotPresent(FileNotFoundError):
    """Raised when a Bundle candidate directory does not have a yardgoat.toml file."""

    pass


__all__ = ["BundleConfigNotPresent", "InvalidKeyError", "MissingKeyError"]
