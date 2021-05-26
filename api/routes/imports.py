# pylint: disable=unused-variable,unused-argument

import csv
import codecs

from flask import request, g, jsonify

from api.utils.request import JsonBlueprint
from db.python.layers.imports import ImportLayer


def get_import_blueprint(prefix):
    """Build blueprint / routes for imports API"""
    import_api = JsonBlueprint('import_api', __name__)
    # just a convenience helper
    project_prefix = prefix + '<project>/imports'

    @import_api.route(project_prefix + '/airtable-manifest', methods=['POST'])
    def import_airtable_manifest(project):
        """
        Import the manifest from Airtable
        ---
        post:
          operationId: importAirtableManifest
          tags: [imports]
          parameters:
          - name: project
            schema:
              type: string
            required: true
            in: path
          requestBody:
            required: true
            content:
              'multipart/form-data':
                schema:
                  type: object
                  properties:
                    file:
                      type: string
                      format: binary
                  required:
                    - file
          responses:
            '200':
              description: The manifest was imported successfully
              content:
                application/json:
                  schema:
                    type: object
                    properties:
                      success:
                        type: boolean
                      sample_ids:
                        type: array
                        items:
                          type: integer
        """

        if 'file' not in request.files:
            raise ValueError('No file was provided')
        file = request.files['file']
        if file.filename == '':
            raise ValueError('No file was provided (the filename was empty)')

        if not file.filename.endswith('.csv'):
            raise ValueError('Expected a csv, but didn\'t have ".csv" extension')

        csvreader = csv.reader(codecs.iterdecode(file.stream, 'utf-8-sig'))
        headers = next(csvreader)

        import_layer = ImportLayer(g.connection, g.author)
        sample_ids = import_layer.import_airtable_manifest_csv(headers, csvreader)

        return jsonify({'success': True, 'sample_ids': sample_ids}), 200

    return import_api
