# pylint: disable=unused-variable

import csv
import codecs
from http import HTTPStatus

from flask import request, g, jsonify
from flasgger import swag_from

from api.utils.request import JsonBlueprint
from db.python.layers.imports import ImportLayer


def get_import_blueprint(prefix):
    """Build blueprint / routes for imports API"""
    import_api = JsonBlueprint('import_api', __name__)
    project_prefix = prefix + '/<project>/imports'

    @import_api.route(project_prefix + '/airtable-manifest', methods=['POST'])
    @swag_from(
        {
            'responses': {
                HTTPStatus.OK.value: {
                    'description': '{success: true, sample_ids: []}',
                    'schema': None,
                }
            },
            'consumes': ['multipart/form-data'],
            'parameters': [
                {'name': 'project', 'type': 'string', 'required': True, 'in': 'path'},
                {
                    'name': 'file',
                    'type': 'file',
                    'required': True,
                    'in': 'formData',
                    'description': 'airtable CSV',
                },
            ],
        }
    )
    def import_airtable_manifest(_):
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
