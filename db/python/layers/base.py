from db.python.connect import Connection


class BaseLayer:
    """Base of all DB layers"""

    def __init__(self, connection: Connection):
        self.connection = connection

    @property
    def author(self):
        """Get author from connection"""
        return self.connection.author
