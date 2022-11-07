import json

from gql import Client
from gql.transport.requests import RequestsHTTPTransport
from gql.dsl import DSLSchema, DSLQuery, dsl_gql

transport = RequestsHTTPTransport(
    url='http://localhost:8000/graphql',
    verify=True,
    retries=3,
)

client = Client(transport=transport, fetch_schema_from_transport=True)
with client as session:
    ds = DSLSchema(client.schema)

    query = dsl_gql(
        DSLQuery(
            ds.Query.sample(id='CPGLCL10').select(
                ds.Sample.id,
                ds.Sample.sequences.select(
                    ds.SampleSequencing.id, ds.SampleSequencing.meta
                ),
            )
        )
    )

    samples = session.execute(query)

print(json.dumps(samples, indent=2))
