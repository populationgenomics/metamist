# pylint: disable=unused-variable,unused-argument

from flask import g

from api.utils.request import JsonBlueprint
from api.schema.sample import SampleSchema
from db.python.tables.sample import SampleTable


def get_sample_blueprint(prefix):
    """Build blueprint / routes for sample API"""
    sample_api = JsonBlueprint('sample_api', __name__)
    project_prefix = prefix + '<project>/sample'

    @sample_api.route(project_prefix + '/<id_>', methods=['GET'])
    def get_by_external_id(project, id_):
        """
        Get a sample by its external ID
        ---
        get:
          operationId: getSampleByExternalId
          tags: [samples]
          parameters:
          - in: path
            name: project
            required: true
            schema:
              type: string
          - in: path
            name: id_
            required: true
            schema:
              type: string
          responses:
            '200':
              content:
                application/json:
                  schema: SampleSchema
              description: ''
          summary: Get a sample by its external ID
        """

        st = SampleTable(g.connection, g.author)
        result = st.get_single_by_external_id(id_)
        return SampleSchema().dump(result), 200

    return sample_api
