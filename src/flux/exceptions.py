class FluxMigrationException(Exception):
    """
    Base exception for Flux Migrations
    """


class MigrationLoadingError(FluxMigrationException):
    """
    Base exception for migration loading errors
    """


class InvalidMigrationModule(MigrationLoadingError):
    """
    Raised when a migration cannot be loaded from a module
    """


class BackendLoadingError(FluxMigrationException):
    """
    Base exception for backend loading errors
    """


class InvalidBackendError(BackendLoadingError):
    """
    Raised when a backend is not a subclass of ``MigrationBackend``
    """
