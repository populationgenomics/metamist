import unittest

from testcontainers.mysql import MariaDbContainer

from db.python.connect import (
    ConnectionStringDatabaseConfiguration,
    Connection,
    SMConnections,
)
from db.python.layers.sample import SampleLayer
from db.python.tables.project import ProjectPermissionsTable
from models.enums import SampleType
from sample_metadata.parser.generic_parser import run_as_sync


class DbTest(unittest.IsolatedAsyncioTestCase):
    @classmethod
    def setUpClass(cls) -> None:
        @run_as_sync
        async def setup():
            db = MariaDbContainer('mariadb:latest')
            # db.port_to_expose = 3307
            # db.with_exposed_ports(db.port_to_expose)
            db.start()
            DbTest._db = db

            import subprocess
            try:
                command = [
                        'liquibase',
                        *('--changeLogFile', '../db/project.xml'),
                        *(f'--url', f'jdbc:mariadb://127.0.0.1:{db.port_to_expose}/{db.MYSQL_DATABASE}'),
                        *('--driver', 'org.mariadb.jdbc.Driver'),
                        *('--classpath', '../db/mariadb-java-client-2.7.2.jar'),
                        *('--username', db.MYSQL_USER),
                        'update',
                    ]
                rc = subprocess.check_output(command)
            except subprocess.CalledProcessError as e:
                print(e)
                print(e.stderr)
                print(e.stdout)
                cls.tearDownClass()
                raise e

            con_string = db.get_connection_url()
            con_string = 'mysql://' + con_string.split('://', maxsplit=1)[1]
            print('connecting with', con_string)
            sm_db = SMConnections.make_connection(
                ConnectionStringDatabaseConfiguration(con_string)
            )
            await sm_db.connect()
            DbTest.connection = Connection(
                connection=sm_db,
                project=0,
                author='testuser',
            )

            p = await ProjectPermissionsTable(
                DbTest.connection.connection
            ).create_project(
                project_name='test',
                dataset_name='test',
                create_test_project=False,
                gcp_project_id='None',
                author='testuser',
                read_secret_name='None',
                write_secret_name='None',
                check_permissions=False,
            )
            print(f'Project created with ID: {p}')

        setup()

    @classmethod
    def tearDownClass(cls) -> None:
        DbTest._db.exec(f'DROP DATABASE {DbTest._db.MYSQL_DATABASE};')
        DbTest._db.stop()

    async def test_number_one(self):
        sl = SampleLayer(DbTest.connection)
        s = await sl.insert_sample(
            'Test01',
            SampleType.BLOOD,
            active=True,
            meta={'meta': 'meta ;)'},
            check_project_id=False,
        )

        print(self._db.exec('SELECT * FROM samples'))
