from json import loads
import requests

from flask import Blueprint, request, jsonify


class JsonBlueprint(Blueprint):
    """
    Subclass of Blueprint that adds 'json_route' for automatic
    serialisation and deserialisation of request data.
    """

    def json_route(self, rule, decode_json: bool, include_headers=False, **options):
        """Add automatic json serialisation and deserialisation"""

        def decorator(f):
            # similar to
            endpoint = options.pop('endpoint', f.__name__)

            def jsonified_f(*args, **kwargs):
                if decode_json:
                    if not request.data:
                        raise ValueError('No data was provided')
                    kwargs = {**loads(request.data), **kwargs}
                if include_headers and 'headers' not in kwargs:
                    kwargs['headers'] = request.headers

                response = f(*args, **kwargs)
                if isinstance(response, tuple) and len(response) == 2:
                    response_data = response[0]
                    code = response[1]
                else:
                    response_data = response
                    # pylint: disable=no-member
                    code = requests.codes.ok

                return jsonify({'data': response_data}), code

            self.add_url_rule(rule, endpoint, jsonified_f, **options)

            return jsonified_f

        return decorator
