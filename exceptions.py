
class MissSpeakException(Exception):
    """
    Exception class for when Client or Server fails part of the Sync.
    """
    pass

class SkipResponseException(Exception):
    """
    Exception class for when Client requests to skip a command to do a temporary
    error
    """
    pass