"""
    Sample metadata API

    No description provided (generated by Openapi Generator https://github.com/openapitools/openapi-generator)  # noqa: E501

    The version of the OpenAPI document: 6.8.0
    Generated by: https://openapi-generator.tech
"""


import re  # noqa: F401
import sys  # noqa: F401

from metamist.api_client import ApiClient, Endpoint as _Endpoint
from metamist.model_utils import (  # noqa: F401
    check_allowed_values,
    check_validations,
    date,
    datetime,
    file_type,
    none_type,
    validate_and_convert_types,
    async_wrap
)
from metamist.model.family_update_model import FamilyUpdateModel
from metamist.model.http_validation_error import HTTPValidationError


class FamilyApi(object):
    """NOTE: This class is auto generated by OpenAPI Generator
    Ref: https://openapi-generator.tech

    Do not edit the class manually.
    """

    def __init__(self, api_client=None):
        if api_client is None:
            api_client = ApiClient()
        self.api_client = api_client

        self.get_families_async = async_wrap(self.get_families)
        self.get_families_endpoint = _Endpoint(
            settings={
                'response_type': (bool, date, datetime, dict, float, int, list, str, none_type,),
                'auth': [
                    'HTTPBearer'
                ],
                'endpoint_path': '/api/v1/family/{project}/',
                'operation_id': 'get_families',
                'http_method': 'GET',
                'servers': None,
            },
            params_map={
                'all': [
                    'project',
                    'participant_ids',
                    'sample_ids',
                ],
                'required': [
                    'project',
                ],
                'nullable': [
                ],
                'enum': [
                ],
                'validation': [
                ]
            },
            root_map={
                'validations': {
                },
                'allowed_values': {
                },
                'openapi_types': {
                    'project':
                        (str,),
                    'participant_ids':
                        ([int],),
                    'sample_ids':
                        ([str],),
                },
                'attribute_map': {
                    'project': 'project',
                    'participant_ids': 'participant_ids',
                    'sample_ids': 'sample_ids',
                },
                'location_map': {
                    'project': 'path',
                    'participant_ids': 'query',
                    'sample_ids': 'query',
                },
                'collection_format_map': {
                    'participant_ids': 'multi',
                    'sample_ids': 'multi',
                }
            },
            headers_map={
                'accept': [
                    'application/json'
                ],
                'content_type': [],
            },
            api_client=api_client
        )

        self.get_pedigree_async = async_wrap(self.get_pedigree)
        self.get_pedigree_endpoint = _Endpoint(
            settings={
                'response_type': (bool, date, datetime, dict, float, int, list, str, none_type,),
                'auth': [
                    'HTTPBearer'
                ],
                'endpoint_path': '/api/v1/family/{project}/pedigree',
                'operation_id': 'get_pedigree',
                'http_method': 'GET',
                'servers': None,
            },
            params_map={
                'all': [
                    'project',
                    'internal_family_ids',
                    'export_type',
                    'replace_with_participant_external_ids',
                    'replace_with_family_external_ids',
                    'include_header',
                    'empty_participant_value',
                    'include_participants_not_in_families',
                ],
                'required': [
                    'project',
                ],
                'nullable': [
                ],
                'enum': [
                ],
                'validation': [
                ]
            },
            root_map={
                'validations': {
                },
                'allowed_values': {
                },
                'openapi_types': {
                    'project':
                        (str,),
                    'internal_family_ids':
                        ([int],),
                    'export_type':
                        (bool, date, datetime, dict, float, int, list, str, none_type,),
                    'replace_with_participant_external_ids':
                        (bool,),
                    'replace_with_family_external_ids':
                        (bool,),
                    'include_header':
                        (bool,),
                    'empty_participant_value':
                        (str,),
                    'include_participants_not_in_families':
                        (bool,),
                },
                'attribute_map': {
                    'project': 'project',
                    'internal_family_ids': 'internal_family_ids',
                    'export_type': 'export_type',
                    'replace_with_participant_external_ids': 'replace_with_participant_external_ids',
                    'replace_with_family_external_ids': 'replace_with_family_external_ids',
                    'include_header': 'include_header',
                    'empty_participant_value': 'empty_participant_value',
                    'include_participants_not_in_families': 'include_participants_not_in_families',
                },
                'location_map': {
                    'project': 'path',
                    'internal_family_ids': 'query',
                    'export_type': 'query',
                    'replace_with_participant_external_ids': 'query',
                    'replace_with_family_external_ids': 'query',
                    'include_header': 'query',
                    'empty_participant_value': 'query',
                    'include_participants_not_in_families': 'query',
                },
                'collection_format_map': {
                    'internal_family_ids': 'multi',
                }
            },
            headers_map={
                'accept': [
                    'application/json'
                ],
                'content_type': [],
            },
            api_client=api_client
        )

        self.import_families_async = async_wrap(self.import_families)
        self.import_families_endpoint = _Endpoint(
            settings={
                'response_type': (bool, date, datetime, dict, float, int, list, str, none_type,),
                'auth': [
                    'HTTPBearer'
                ],
                'endpoint_path': '/api/v1/family/{project}/family-template',
                'operation_id': 'import_families',
                'http_method': 'POST',
                'servers': None,
            },
            params_map={
                'all': [
                    'project',
                    'file',
                    'has_header',
                    'delimiter',
                ],
                'required': [
                    'project',
                    'file',
                ],
                'nullable': [
                ],
                'enum': [
                ],
                'validation': [
                ]
            },
            root_map={
                'validations': {
                },
                'allowed_values': {
                },
                'openapi_types': {
                    'project':
                        (str,),
                    'file':
                        (file_type,),
                    'has_header':
                        (bool,),
                    'delimiter':
                        (str,),
                },
                'attribute_map': {
                    'project': 'project',
                    'file': 'file',
                    'has_header': 'has_header',
                    'delimiter': 'delimiter',
                },
                'location_map': {
                    'project': 'path',
                    'file': 'form',
                    'has_header': 'query',
                    'delimiter': 'query',
                },
                'collection_format_map': {
                }
            },
            headers_map={
                'accept': [
                    'application/json'
                ],
                'content_type': [
                    'multipart/form-data'
                ]
            },
            api_client=api_client
        )

        self.import_pedigree_async = async_wrap(self.import_pedigree)
        self.import_pedigree_endpoint = _Endpoint(
            settings={
                'response_type': (bool, date, datetime, dict, float, int, list, str, none_type,),
                'auth': [
                    'HTTPBearer'
                ],
                'endpoint_path': '/api/v1/family/{project}/pedigree',
                'operation_id': 'import_pedigree',
                'http_method': 'POST',
                'servers': None,
            },
            params_map={
                'all': [
                    'project',
                    'file',
                    'has_header',
                    'create_missing_participants',
                    'perform_sex_check',
                ],
                'required': [
                    'project',
                    'file',
                ],
                'nullable': [
                ],
                'enum': [
                ],
                'validation': [
                ]
            },
            root_map={
                'validations': {
                },
                'allowed_values': {
                },
                'openapi_types': {
                    'project':
                        (str,),
                    'file':
                        (file_type,),
                    'has_header':
                        (bool,),
                    'create_missing_participants':
                        (bool,),
                    'perform_sex_check':
                        (bool,),
                },
                'attribute_map': {
                    'project': 'project',
                    'file': 'file',
                    'has_header': 'has_header',
                    'create_missing_participants': 'create_missing_participants',
                    'perform_sex_check': 'perform_sex_check',
                },
                'location_map': {
                    'project': 'path',
                    'file': 'form',
                    'has_header': 'query',
                    'create_missing_participants': 'query',
                    'perform_sex_check': 'query',
                },
                'collection_format_map': {
                }
            },
            headers_map={
                'accept': [
                    'application/json'
                ],
                'content_type': [
                    'multipart/form-data'
                ]
            },
            api_client=api_client
        )

        self.update_family_async = async_wrap(self.update_family)
        self.update_family_endpoint = _Endpoint(
            settings={
                'response_type': (bool, date, datetime, dict, float, int, list, str, none_type,),
                'auth': [
                    'HTTPBearer'
                ],
                'endpoint_path': '/api/v1/family/',
                'operation_id': 'update_family',
                'http_method': 'POST',
                'servers': None,
            },
            params_map={
                'all': [
                    'family_update_model',
                ],
                'required': [
                    'family_update_model',
                ],
                'nullable': [
                ],
                'enum': [
                ],
                'validation': [
                ]
            },
            root_map={
                'validations': {
                },
                'allowed_values': {
                },
                'openapi_types': {
                    'family_update_model':
                        (FamilyUpdateModel,),
                },
                'attribute_map': {
                },
                'location_map': {
                    'family_update_model': 'body',
                },
                'collection_format_map': {
                }
            },
            headers_map={
                'accept': [
                    'application/json'
                ],
                'content_type': [
                    'application/json'
                ]
            },
            api_client=api_client
        )

    def get_families(
        self,
        project,
        **kwargs
    ):
        """Get Families  # noqa: E501

        Get families for some project  # noqa: E501
        This method makes a synchronous HTTP request by default. To make an
        asynchronous HTTP request, please pass async_req=True

        >>> thread = api.get_families(project, async_req=True)
        >>> result = thread.get()

        Args:
            project (str):

        Keyword Args:
            participant_ids ([int]): [optional]
            sample_ids ([str]): [optional]
            _return_http_data_only (bool): response data without head status
                code and headers. Default is True.
            _preload_content (bool): if False, the urllib3.HTTPResponse object
                will be returned without reading/decoding response data.
                Default is True.
            _request_timeout (int/float/tuple): timeout setting for this request. If
                one number provided, it will be total request timeout. It can also
                be a pair (tuple) of (connection, read) timeouts.
                Default is None.
            _check_input_type (bool): specifies if type checking
                should be done one the data sent to the server.
                Default is True.
            _check_return_type (bool): specifies if type checking
                should be done one the data received from the server.
                Default is True.
            _content_type (str/None): force body content-type.
                Default is None and content-type will be predicted by allowed
                content-types and body.
            _host_index (int/None): specifies the index of the server
                that we want to use.
                Default is read from the configuration.
            async_req (bool): execute request asynchronously

        Returns:
            bool, date, datetime, dict, float, int, list, str, none_type
                If the method is called asynchronously, returns the request
                thread.
        """
        kwargs['async_req'] = kwargs.get(
            'async_req', False
        )
        kwargs['_return_http_data_only'] = kwargs.get(
            '_return_http_data_only', True
        )
        kwargs['_preload_content'] = kwargs.get(
            '_preload_content', True
        )
        kwargs['_request_timeout'] = kwargs.get(
            '_request_timeout', None
        )
        kwargs['_check_input_type'] = kwargs.get(
            '_check_input_type', True
        )
        kwargs['_check_return_type'] = kwargs.get(
            '_check_return_type', True
        )
        kwargs['_content_type'] = kwargs.get(
            '_content_type')
        kwargs['_host_index'] = kwargs.get('_host_index')
        kwargs['project'] = \
            project
        return self.get_families_endpoint.call_with_http_info(**kwargs)

    def get_pedigree(
        self,
        project,
        **kwargs
    ):
        """Get Pedigree  # noqa: E501

        Generate tab-separated Pedigree file for ALL families unless internal_family_ids is specified.  Allow replacement of internal participant and family IDs with their external counterparts.  # noqa: E501
        This method makes a synchronous HTTP request by default. To make an
        asynchronous HTTP request, please pass async_req=True

        >>> thread = api.get_pedigree(project, async_req=True)
        >>> result = thread.get()

        Args:
            project (str):

        Keyword Args:
            internal_family_ids ([int]): [optional]
            export_type (bool, date, datetime, dict, float, int, list, str, none_type): [optional]
            replace_with_participant_external_ids (bool): [optional] if omitted the server will use the default value of True
            replace_with_family_external_ids (bool): [optional] if omitted the server will use the default value of True
            include_header (bool): [optional] if omitted the server will use the default value of True
            empty_participant_value (str): [optional]
            include_participants_not_in_families (bool): [optional] if omitted the server will use the default value of False
            _return_http_data_only (bool): response data without head status
                code and headers. Default is True.
            _preload_content (bool): if False, the urllib3.HTTPResponse object
                will be returned without reading/decoding response data.
                Default is True.
            _request_timeout (int/float/tuple): timeout setting for this request. If
                one number provided, it will be total request timeout. It can also
                be a pair (tuple) of (connection, read) timeouts.
                Default is None.
            _check_input_type (bool): specifies if type checking
                should be done one the data sent to the server.
                Default is True.
            _check_return_type (bool): specifies if type checking
                should be done one the data received from the server.
                Default is True.
            _content_type (str/None): force body content-type.
                Default is None and content-type will be predicted by allowed
                content-types and body.
            _host_index (int/None): specifies the index of the server
                that we want to use.
                Default is read from the configuration.
            async_req (bool): execute request asynchronously

        Returns:
            bool, date, datetime, dict, float, int, list, str, none_type
                If the method is called asynchronously, returns the request
                thread.
        """
        kwargs['async_req'] = kwargs.get(
            'async_req', False
        )
        kwargs['_return_http_data_only'] = kwargs.get(
            '_return_http_data_only', True
        )
        kwargs['_preload_content'] = kwargs.get(
            '_preload_content', True
        )
        kwargs['_request_timeout'] = kwargs.get(
            '_request_timeout', None
        )
        kwargs['_check_input_type'] = kwargs.get(
            '_check_input_type', True
        )
        kwargs['_check_return_type'] = kwargs.get(
            '_check_return_type', True
        )
        kwargs['_content_type'] = kwargs.get(
            '_content_type')
        kwargs['_host_index'] = kwargs.get('_host_index')
        kwargs['project'] = \
            project
        return self.get_pedigree_endpoint.call_with_http_info(**kwargs)

    def import_families(
        self,
        project,
        file,
        **kwargs
    ):
        """Import Families  # noqa: E501

        Import a family csv  # noqa: E501
        This method makes a synchronous HTTP request by default. To make an
        asynchronous HTTP request, please pass async_req=True

        >>> thread = api.import_families(project, file, async_req=True)
        >>> result = thread.get()

        Args:
            project (str):
            file (file_type):

        Keyword Args:
            has_header (bool): [optional] if omitted the server will use the default value of True
            delimiter (str): [optional]
            _return_http_data_only (bool): response data without head status
                code and headers. Default is True.
            _preload_content (bool): if False, the urllib3.HTTPResponse object
                will be returned without reading/decoding response data.
                Default is True.
            _request_timeout (int/float/tuple): timeout setting for this request. If
                one number provided, it will be total request timeout. It can also
                be a pair (tuple) of (connection, read) timeouts.
                Default is None.
            _check_input_type (bool): specifies if type checking
                should be done one the data sent to the server.
                Default is True.
            _check_return_type (bool): specifies if type checking
                should be done one the data received from the server.
                Default is True.
            _content_type (str/None): force body content-type.
                Default is None and content-type will be predicted by allowed
                content-types and body.
            _host_index (int/None): specifies the index of the server
                that we want to use.
                Default is read from the configuration.
            async_req (bool): execute request asynchronously

        Returns:
            bool, date, datetime, dict, float, int, list, str, none_type
                If the method is called asynchronously, returns the request
                thread.
        """
        kwargs['async_req'] = kwargs.get(
            'async_req', False
        )
        kwargs['_return_http_data_only'] = kwargs.get(
            '_return_http_data_only', True
        )
        kwargs['_preload_content'] = kwargs.get(
            '_preload_content', True
        )
        kwargs['_request_timeout'] = kwargs.get(
            '_request_timeout', None
        )
        kwargs['_check_input_type'] = kwargs.get(
            '_check_input_type', True
        )
        kwargs['_check_return_type'] = kwargs.get(
            '_check_return_type', True
        )
        kwargs['_content_type'] = kwargs.get(
            '_content_type')
        kwargs['_host_index'] = kwargs.get('_host_index')
        kwargs['project'] = \
            project
        kwargs['file'] = \
            file
        return self.import_families_endpoint.call_with_http_info(**kwargs)

    def import_pedigree(
        self,
        project,
        file,
        **kwargs
    ):
        """Import Pedigree  # noqa: E501

        Import a pedigree  # noqa: E501
        This method makes a synchronous HTTP request by default. To make an
        asynchronous HTTP request, please pass async_req=True

        >>> thread = api.import_pedigree(project, file, async_req=True)
        >>> result = thread.get()

        Args:
            project (str):
            file (file_type):

        Keyword Args:
            has_header (bool): [optional] if omitted the server will use the default value of False
            create_missing_participants (bool): [optional] if omitted the server will use the default value of False
            perform_sex_check (bool): [optional] if omitted the server will use the default value of True
            _return_http_data_only (bool): response data without head status
                code and headers. Default is True.
            _preload_content (bool): if False, the urllib3.HTTPResponse object
                will be returned without reading/decoding response data.
                Default is True.
            _request_timeout (int/float/tuple): timeout setting for this request. If
                one number provided, it will be total request timeout. It can also
                be a pair (tuple) of (connection, read) timeouts.
                Default is None.
            _check_input_type (bool): specifies if type checking
                should be done one the data sent to the server.
                Default is True.
            _check_return_type (bool): specifies if type checking
                should be done one the data received from the server.
                Default is True.
            _content_type (str/None): force body content-type.
                Default is None and content-type will be predicted by allowed
                content-types and body.
            _host_index (int/None): specifies the index of the server
                that we want to use.
                Default is read from the configuration.
            async_req (bool): execute request asynchronously

        Returns:
            bool, date, datetime, dict, float, int, list, str, none_type
                If the method is called asynchronously, returns the request
                thread.
        """
        kwargs['async_req'] = kwargs.get(
            'async_req', False
        )
        kwargs['_return_http_data_only'] = kwargs.get(
            '_return_http_data_only', True
        )
        kwargs['_preload_content'] = kwargs.get(
            '_preload_content', True
        )
        kwargs['_request_timeout'] = kwargs.get(
            '_request_timeout', None
        )
        kwargs['_check_input_type'] = kwargs.get(
            '_check_input_type', True
        )
        kwargs['_check_return_type'] = kwargs.get(
            '_check_return_type', True
        )
        kwargs['_content_type'] = kwargs.get(
            '_content_type')
        kwargs['_host_index'] = kwargs.get('_host_index')
        kwargs['project'] = \
            project
        kwargs['file'] = \
            file
        return self.import_pedigree_endpoint.call_with_http_info(**kwargs)

    def update_family(
        self,
        family_update_model,
        **kwargs
    ):
        """Update Family  # noqa: E501

        Update information for a single family  # noqa: E501
        This method makes a synchronous HTTP request by default. To make an
        asynchronous HTTP request, please pass async_req=True

        >>> thread = api.update_family(family_update_model, async_req=True)
        >>> result = thread.get()

        Args:
            family_update_model (FamilyUpdateModel):

        Keyword Args:
            _return_http_data_only (bool): response data without head status
                code and headers. Default is True.
            _preload_content (bool): if False, the urllib3.HTTPResponse object
                will be returned without reading/decoding response data.
                Default is True.
            _request_timeout (int/float/tuple): timeout setting for this request. If
                one number provided, it will be total request timeout. It can also
                be a pair (tuple) of (connection, read) timeouts.
                Default is None.
            _check_input_type (bool): specifies if type checking
                should be done one the data sent to the server.
                Default is True.
            _check_return_type (bool): specifies if type checking
                should be done one the data received from the server.
                Default is True.
            _content_type (str/None): force body content-type.
                Default is None and content-type will be predicted by allowed
                content-types and body.
            _host_index (int/None): specifies the index of the server
                that we want to use.
                Default is read from the configuration.
            async_req (bool): execute request asynchronously

        Returns:
            bool, date, datetime, dict, float, int, list, str, none_type
                If the method is called asynchronously, returns the request
                thread.
        """
        kwargs['async_req'] = kwargs.get(
            'async_req', False
        )
        kwargs['_return_http_data_only'] = kwargs.get(
            '_return_http_data_only', True
        )
        kwargs['_preload_content'] = kwargs.get(
            '_preload_content', True
        )
        kwargs['_request_timeout'] = kwargs.get(
            '_request_timeout', None
        )
        kwargs['_check_input_type'] = kwargs.get(
            '_check_input_type', True
        )
        kwargs['_check_return_type'] = kwargs.get(
            '_check_return_type', True
        )
        kwargs['_content_type'] = kwargs.get(
            '_content_type')
        kwargs['_host_index'] = kwargs.get('_host_index')
        kwargs['family_update_model'] = \
            family_update_model
        return self.update_family_endpoint.call_with_http_info(**kwargs)

