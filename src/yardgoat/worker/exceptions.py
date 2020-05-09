class AlreadyCompletedError(RuntimeError):
    """Raised when the one attempts to complete an already completed job."""

    pass


__all__ = ["AlreadyCompletedError"]
