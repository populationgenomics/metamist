# pylint: disable=unused-variable,unused-argument

# from flask import request, g, jsonify

from api.utils.request import JsonBlueprint

# from models.enums import AnalysisType, AnalysisStatus


def get_analysis_blueprint(prefix):
    """Build blueprint / routes for analysis API"""
    analysis_api = JsonBlueprint('analysis_api', __name__)
    # just a convenience helper
    project_prefix = prefix + '<project>/analysis'

    @analysis_api.json_route(project_prefix, methods=['POST'], decode_json=True)
    def create_new_analysis(
        _,
        sample_ids,
    ):
        """
        Create a new analysis
        ---
        post:
          operationId: createNewAnalysis
          tags: [analysis]
          parameters:
          - name: project
            schema:
              type: string
            required: true
            in: path
          requestBody:
            required: true
            content:
              application/json:
                schema:
                  type: object
                  properties:
                    sample_ids:
                      type: array
                      items:
                        type: integer
                    type_: AnalysisType
          responses:
            '200':
              description: The analysis was created successfully
              content:
                application/json:
                  schema:
                    type: object
                    properties:
                      success:
                        type: boolean
                      analysisId:
                        type: integer
        """

        raise NotImplementedError(f'Not implemented for sampleIds={sample_ids}')

    return analysis_api
