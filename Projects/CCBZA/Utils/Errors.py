class DataError(Exception):
    """
    Exception for cases where IOT device does not have a connected shelf in the database
    """

    def __init__(self, message):
        self._message = message

    @property
    def message(self):
        return self._message