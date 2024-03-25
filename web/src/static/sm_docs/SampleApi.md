# metamist.SampleApi

Method | HTTP request | Description
------------- | ------------- | -------------
[**create_sample**](SampleApi.md#create_sample) | **PUT** /api/v1/sample/{project}/ | Create Sample
[**get_all_sample_id_map_by_internal**](SampleApi.md#get_all_sample_id_map_by_internal) | **GET** /api/v1/sample/{project}/id-map/internal/all | Get All Sample Id Map By Internal
[**get_history_of_sample**](SampleApi.md#get_history_of_sample) | **GET** /api/v1/sample/{id_}/history | Get History Of Sample
[**get_sample_by_external_id**](SampleApi.md#get_sample_by_external_id) | **GET** /api/v1/sample/{project}/{external_id}/details | Get Sample By External Id
[**get_sample_id_map_by_external**](SampleApi.md#get_sample_id_map_by_external) | **POST** /api/v1/sample/{project}/id-map/external | Get Sample Id Map By External
[**get_sample_id_map_by_internal**](SampleApi.md#get_sample_id_map_by_internal) | **POST** /api/v1/sample/id-map/internal | Get Sample Id Map By Internal
[**get_samples**](SampleApi.md#get_samples) | **POST** /api/v1/sample/ | Get Samples
[**get_samples_create_date**](SampleApi.md#get_samples_create_date) | **POST** /api/v1/sample/samples-create-date | Get Samples Create Date
[**merge_samples**](SampleApi.md#merge_samples) | **PATCH** /api/v1/sample/{id_keep}/{id_merge} | Merge Samples
[**update_sample**](SampleApi.md#update_sample) | **PATCH** /api/v1/sample/{id_} | Update Sample
[**upsert_samples**](SampleApi.md#upsert_samples) | **PUT** /api/v1/sample/{project}/upsert-many | Upsert Samples


# **create_sample**
> str create_sample(project, sample_upsert)

Create Sample

Creates a new sample, and returns the internal sample ID

### Example

```python
import metamist

from metamist import SampleApi
api_instance = SampleApi()
project = "project_example" # str | 
sample_upsert = SampleUpsert(
        id=None,
        external_id=None,
        meta=None,
        project=None,
        type=None,
        participant_id=None,
        active=None,
        sequencing_groups=[
            SequencingGroupUpsert(
                id=None,
                type=None,
                technology=None,
                platform=None,
                meta=None,
                sample_id=None,
                external_ids=None,
                assays=None,
            ),
        ],
        non_sequencing_assays=[
            AssayUpsert(
                id=None,
                type=None,
                external_ids=None,
                sample_id=None,
                meta=None,
            ),
        ],
    ) # SampleUpsert | 

# Create Sample
api_response = api_instance.create_sample(project, sample_upsert)
print(api_response)
```


### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **project** | **str**|  |
 **sample_upsert** | [**SampleUpsert**](SampleUpsert.md)|  |

### Return type

**str**

### Authorization

[HTTPBearer](../README.md#HTTPBearer)

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: application/json


### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | Successful Response |  -  |
**422** | Validation Error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **get_all_sample_id_map_by_internal**
> bool, date, datetime, dict, float, int, list, str, none_type get_all_sample_id_map_by_internal(project)

Get All Sample Id Map By Internal

Get map of ALL sample IDs, { [internal_id]: external_sample_id }

### Example

```python
import metamist

from metamist import SampleApi
api_instance = SampleApi()
project = "project_example" # str | 

# Get All Sample Id Map By Internal
api_response = api_instance.get_all_sample_id_map_by_internal(project)
print(api_response)
```


### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **project** | **str**|  |

### Return type

**bool, date, datetime, dict, float, int, list, str, none_type**

### Authorization

[HTTPBearer](../README.md#HTTPBearer)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json


### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | Successful Response |  -  |
**422** | Validation Error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **get_history_of_sample**
> bool, date, datetime, dict, float, int, list, str, none_type get_history_of_sample(id_)

Get History Of Sample

Get full history of sample from internal ID

### Example

```python
import metamist

from metamist import SampleApi
api_instance = SampleApi()
id_ = "id__example" # str | 

# Get History Of Sample
api_response = api_instance.get_history_of_sample(id_)
print(api_response)
```


### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **id_** | **str**|  |

### Return type

**bool, date, datetime, dict, float, int, list, str, none_type**

### Authorization

[HTTPBearer](../README.md#HTTPBearer)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json


### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | Successful Response |  -  |
**422** | Validation Error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **get_sample_by_external_id**
> bool, date, datetime, dict, float, int, list, str, none_type get_sample_by_external_id(external_id, project)

Get Sample By External Id

Get sample by external ID

### Example

```python
import metamist

from metamist import SampleApi
api_instance = SampleApi()
external_id = "external_id_example" # str | 
project = "project_example" # str | 

# Get Sample By External Id
api_response = api_instance.get_sample_by_external_id(external_id, project)
print(api_response)
```


### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **external_id** | **str**|  |
 **project** | **str**|  |

### Return type

**bool, date, datetime, dict, float, int, list, str, none_type**

### Authorization

[HTTPBearer](../README.md#HTTPBearer)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json


### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | Successful Response |  -  |
**422** | Validation Error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **get_sample_id_map_by_external**
> bool, date, datetime, dict, float, int, list, str, none_type get_sample_id_map_by_external(project, request_body)

Get Sample Id Map By External

Get map of sample IDs, { [externalId]: internal_sample_id }

### Example

```python
import metamist

from metamist import SampleApi
api_instance = SampleApi()
project = "project_example" # str | 
request_body = [
        "request_body_example",
    ] # [str] | 
allow_missing = False # bool |  (optional) if omitted the server will use the default value of False

# Get Sample Id Map By External
api_response = api_instance.get_sample_id_map_by_external(project, request_body)
print(api_response)
# Get Sample Id Map By External
api_response = api_instance.get_sample_id_map_by_external(project, request_body, allow_missing=allow_missing)
print(api_response)
```


### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **project** | **str**|  |
 **request_body** | **[str]**|  |
 **allow_missing** | **bool**|  | [optional] if omitted the server will use the default value of False

### Return type

**bool, date, datetime, dict, float, int, list, str, none_type**

### Authorization

[HTTPBearer](../README.md#HTTPBearer)

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: application/json


### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | Successful Response |  -  |
**422** | Validation Error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **get_sample_id_map_by_internal**
> bool, date, datetime, dict, float, int, list, str, none_type get_sample_id_map_by_internal(request_body)

Get Sample Id Map By Internal

Get map of sample IDs, { [internal_id]: external_sample_id } Without specifying a project, you might see duplicate external identifiers

### Example

```python
import metamist

from metamist import SampleApi
api_instance = SampleApi()
request_body = [
        "request_body_example",
    ] # [str] | 

# Get Sample Id Map By Internal
api_response = api_instance.get_sample_id_map_by_internal(request_body)
print(api_response)
```


### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **request_body** | **[str]**|  |

### Return type

**bool, date, datetime, dict, float, int, list, str, none_type**

### Authorization

[HTTPBearer](../README.md#HTTPBearer)

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: application/json


### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | Successful Response |  -  |
**422** | Validation Error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **get_samples**
> bool, date, datetime, dict, float, int, list, str, none_type get_samples()

Get Samples

Get list of samples (dict) by some mixture of (AND'd) criteria

### Example

```python
import metamist

from metamist import SampleApi
api_instance = SampleApi()
body_get_samples = BodyGetSamples(
        sample_ids=[
            "sample_ids_example",
        ],
        meta={},
        participant_ids=[
            1,
        ],
        project_ids=[
            "project_ids_example",
        ],
        active=True,
    ) # BodyGetSamples |  (optional)
# Get Samples
api_response = api_instance.get_samples(body_get_samples=body_get_samples)
print(api_response)
```


### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **body_get_samples** | [**BodyGetSamples**](BodyGetSamples.md)|  | [optional]

### Return type

**bool, date, datetime, dict, float, int, list, str, none_type**

### Authorization

[HTTPBearer](../README.md#HTTPBearer)

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: application/json


### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | Successful Response |  -  |
**422** | Validation Error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **get_samples_create_date**
> bool, date, datetime, dict, float, int, list, str, none_type get_samples_create_date(request_body)

Get Samples Create Date

Get full history of sample from internal ID

### Example

```python
import metamist

from metamist import SampleApi
api_instance = SampleApi()
request_body = [
        "request_body_example",
    ] # [str] | 

# Get Samples Create Date
api_response = api_instance.get_samples_create_date(request_body)
print(api_response)
```


### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **request_body** | **[str]**|  |

### Return type

**bool, date, datetime, dict, float, int, list, str, none_type**

### Authorization

[HTTPBearer](../README.md#HTTPBearer)

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: application/json


### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | Successful Response |  -  |
**422** | Validation Error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **merge_samples**
> bool, date, datetime, dict, float, int, list, str, none_type merge_samples(id_keep, id_merge)

Merge Samples

Merge one sample into another, this function achieves the merge by rewriting all sample_ids of {id_merge} with {id_keep}. You must carefully consider if analysis objects need to be deleted, or other implications BEFORE running this method.

### Example

```python
import metamist

from metamist import SampleApi
api_instance = SampleApi()
id_keep = "id_keep_example" # str | 
id_merge = "id_merge_example" # str | 

# Merge Samples
api_response = api_instance.merge_samples(id_keep, id_merge)
print(api_response)
```


### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **id_keep** | **str**|  |
 **id_merge** | **str**|  |

### Return type

**bool, date, datetime, dict, float, int, list, str, none_type**

### Authorization

[HTTPBearer](../README.md#HTTPBearer)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json


### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | Successful Response |  -  |
**422** | Validation Error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **update_sample**
> bool, date, datetime, dict, float, int, list, str, none_type update_sample(id_, sample_upsert)

Update Sample

Update sample with id

### Example

```python
import metamist

from metamist import SampleApi
api_instance = SampleApi()
id_ = "id__example" # str | 
sample_upsert = SampleUpsert(
        id=None,
        external_id=None,
        meta=None,
        project=None,
        type=None,
        participant_id=None,
        active=None,
        sequencing_groups=[
            SequencingGroupUpsert(
                id=None,
                type=None,
                technology=None,
                platform=None,
                meta=None,
                sample_id=None,
                external_ids=None,
                assays=None,
            ),
        ],
        non_sequencing_assays=[
            AssayUpsert(
                id=None,
                type=None,
                external_ids=None,
                sample_id=None,
                meta=None,
            ),
        ],
    ) # SampleUpsert | 

# Update Sample
api_response = api_instance.update_sample(id_, sample_upsert)
print(api_response)
```


### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **id_** | **str**|  |
 **sample_upsert** | [**SampleUpsert**](SampleUpsert.md)|  |

### Return type

**bool, date, datetime, dict, float, int, list, str, none_type**

### Authorization

[HTTPBearer](../README.md#HTTPBearer)

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: application/json


### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | Successful Response |  -  |
**422** | Validation Error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **upsert_samples**
> bool, date, datetime, dict, float, int, list, str, none_type upsert_samples(project, sample_upsert)

Upsert Samples

Upserts a list of samples with sequencing-groups, and returns the list of internal sample IDs

### Example

```python
import metamist

from metamist import SampleApi
api_instance = SampleApi()
project = "project_example" # str | 
sample_upsert = [
        SampleUpsert(
            id=None,
            external_id=None,
            meta=None,
            project=None,
            type=None,
            participant_id=None,
            active=None,
            sequencing_groups=[
                SequencingGroupUpsert(
                    id=None,
                    type=None,
                    technology=None,
                    platform=None,
                    meta=None,
                    sample_id=None,
                    external_ids=None,
                    assays=None,
                ),
            ],
            non_sequencing_assays=[
                AssayUpsert(
                    id=None,
                    type=None,
                    external_ids=None,
                    sample_id=None,
                    meta=None,
                ),
            ],
        ),
    ] # [SampleUpsert] | 

# Upsert Samples
api_response = api_instance.upsert_samples(project, sample_upsert)
print(api_response)
```


### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **project** | **str**|  |
 **sample_upsert** | [**[SampleUpsert]**](SampleUpsert.md)|  |

### Return type

**bool, date, datetime, dict, float, int, list, str, none_type**

### Authorization

[HTTPBearer](../README.md#HTTPBearer)

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: application/json


### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | Successful Response |  -  |
**422** | Validation Error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

