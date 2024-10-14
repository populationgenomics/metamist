"""
Code for connecting to Big Query database
"""

import logging
import os

import google.cloud.bigquery as bq
from google.cloud import pubsub_v1

from db.python.utils import InternalError

logging.basicConfig(level=logging.WARNING)
logger = logging.getLogger(__name__)


class BqConnection:
    """Stores a Big Query DB connection, project and author"""

    def __init__(
        self,
        author: str,
    ):
        self.gcp_project = os.getenv('METAMIST_GCP_PROJECT')
        self.connection: bq.Client = bq.Client(project=self.gcp_project)
        self.author: str = author
        # initialise cost of the query
        self._cost: float = 0

    @staticmethod
    async def get_connection_no_project(author: str):
        """Get a db connection from a project and user"""
        # maybe it makes sense to perform permission checks here too
        logger.debug(f'Authenticate no-project connection with {author!r}')

        # we don't authenticate project-less connection, but rely on the
        # the endpoint to validate the resources

        return BqConnection(author=author)

    @property
    def cost(self) -> float:
        """Get the cost of the query"""
        return self._cost

    @cost.setter
    def cost(self, value: float):
        """Set the cost of the query"""
        self._cost = value


class BqDbBase:
    """Base class for big query database subclasses"""

    def __init__(self, connection: BqConnection):
        if connection is None:
            raise InternalError(
                f'No connection was provided to the table {self.__class__.__name__!r}'
            )
        if not isinstance(connection, BqConnection):
            raise InternalError(
                f'Expected connection type Connection, received {type(connection)}, '
                f'did you mean to call self._connection?'
            )

        self._connection = connection


class PubSubConnection:
    """Stores a PubSub connection, project and author"""

    def __init__(
        self,
        author: str,
        topic: str,
    ):
        self.client: pubsub_v1.PublisherClient = pubsub_v1.PublisherClient()
        self.author: str = author
        self.topic: str = os.getenv('METAMIST_GCP_PROJECT') + topic

    @staticmethod
    async def get_connection_no_project(author: str, topic: str):
        """Get a pubsub connection from a project and user"""
        # maybe it makes sense to perform permission checks here too
        logger.debug(f'Authenticate no-project connection with {author!r}')

        # we don't authenticate project-less connection, but rely on the
        # the endpoint to validate the resources

        return PubSubConnection(author=author, topic=topic)
