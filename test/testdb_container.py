import socket
from testcontainers.mysql import MySqlContainer


class TestDatabaseContainer:
    """
    Logic to create a singleton MYSQL MariaDB test database container
    """

    _instance = None
    _db_container = None
    _db_port = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(TestDatabaseContainer, cls).__new__(cls)
        return cls._instance

    def _find_free_port(self):
        """Find free port to run tests on"""
        s = socket.socket()
        s.bind(('localhost', 0))  # Bind to a free port provided by the host.
        free_port_number = s.getsockname()[1]  # Return the port number assigned.
        s.close()  # free the port so we can immediately use
        return free_port_number

    def start(self):
        """Set up the test database container"""
        if self._db_container is None:
            self._db_container = MySqlContainer('mariadb:11.2.2', password='test')
            self._db_port = self._find_free_port()
            self._db_container.with_bind_ports(self._db_container.port, self._db_port)
            self._db_container.start()

    def teardown(self):
        """Stop the test database container"""
        if self._db_container is not None:
            self._db_container.stop()
            self._db_container = None
            self._db_port = None

    def get_container(self):
        """Return the test database container"""
        return self._db_container

    def get_port(self):
        """Return the free port number"""
        return self._db_port
