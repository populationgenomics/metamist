from db.python.gcp_connect import BqConnection


class BqBaseLayer:
    """Base of all Big Query DB layers"""

    def __init__(self, connection: BqConnection):
        self.connection = connection

    @property
    def author(self):
        """Get author from connection"""
        return self.connection.author

    @property
    def cost(self):
        """Get author from connection"""
        return self.connection.cost
