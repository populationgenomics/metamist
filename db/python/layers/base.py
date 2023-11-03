from db.python.tables.project import Connection, ProjectPermissionsTable


class BaseLayer:
    """Base of all DB layers"""

    def __init__(self, connection: Connection):
        self.connection = connection
        self.ptable = ProjectPermissionsTable(connection)

    @property
    def author(self):
        """Get author from connection"""
        return self.connection.author
