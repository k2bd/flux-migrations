class FluxMigrationException(Exception):
    """
    Base exception for Flux Migrations
    """


class MigrationLoadingError(FluxMigrationException):
    """
    Base exception for migration loading errors
    """


class InvalidMigrationModuleError(MigrationLoadingError):
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


class BackendNotInstalledError(BackendLoadingError):
    """
    A backend of the specified name is not installed
    """


class InvalidConfigurationError(FluxMigrationException):
    """
    Raised when the configuration file is invalid
    """


class MigrationDirectoryCorruptedError(FluxMigrationException):
    """
    Raised when the migration directory is corrupted
    """
