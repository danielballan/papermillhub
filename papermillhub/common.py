import enum


class _IntEnum(enum.IntEnum):
    @classmethod
    def from_name(cls, name):
        """Create an enum value from a name"""
        try:
            return cls[name.upper()]
        except KeyError:
            pass
        raise ValueError("%r is not a valid %s" % (name, cls.__name__))


class JobStatus(_IntEnum):
    # Job is pending
    PENDING = 1
    # Job is currently running
    RUNNING = 2
    # Job completed successfully
    SUCCEEDED = 3
    # Job failed with cell errors
    FAILED = 4
    # Job failed with internal errors (not due to notebook code)
    ERRORED = 5
    # Job was cancelled
    CANCELLED = 6
