class ConfigError(ValueError):
    """Handle Error in config control"""
    def __init__(self, message, errors=None):
        super(ConfigError, self).__init__(message)
        self.message = message
        self.errors = errors


class ValidateSchemaError(ConfigError):
    """Raise for validate schema error"""


class ValidateTypeError(ConfigError):
    """Raise for validate type error"""


class ConfigNotFound(ConfigError):
    """Raise for configuration data does not exists"""
