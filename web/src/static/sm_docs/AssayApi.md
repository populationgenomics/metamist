# metamist.AssayApi

Method | HTTP request | Description
------------- | ------------- | -------------
[**create_assay**](AssayApi.md#create_assay) | **PUT** /api/v1/assay/ | Create Assay
[**get_assay_by_external_id**](AssayApi.md#get_assay_by_external_id) | **GET** /api/v1/assay/{project}/external_id/{external_id}/details | Get Assay By External Id
[**get_assay_by_id**](AssayApi.md#get_assay_by_id) | **GET** /api/v1/assay/{assay_id}/details | Get Assay By Id
[**get_assays_by_criteria**](AssayApi.md#get_assays_by_criteria) | **POST** /api/v1/assay/criteria | Get Assays By Criteria
[**update_assay**](AssayApi.md#update_assay) | **PATCH** /api/v1/assay/ | Update Assay


# **create_assay**
> bool, date, datetime, dict, float, int, list, str, none_type create_assay(assay_upsert)

Create Assay

Create new assay, attached to a sample

### Example

```python
import metamist

from metamist import AssayApi
api_instance = AssayApi()
assay_upsert = AssayUpsert(
        id=None,
        type=None,
        external_ids=None,
        sample_id=None,
        meta=None,
    ) # AssayUpsert | 

# Create Assay
api_response = api_instance.create_assay(assay_upsert)
print(api_response)
```


### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **assay_upsert** | [**AssayUpsert**](AssayUpsert.md)|  |

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

# **get_assay_by_external_id**
> bool, date, datetime, dict, float, int, list, str, none_type get_assay_by_external_id(external_id, project)

Get Assay By External Id

Get an assay by ONE of its external identifiers

### Example

```python
import metamist

from metamist import AssayApi
api_instance = AssayApi()
external_id = "external_id_example" # str | 
project = "project_example" # str | 

# Get Assay By External Id
api_response = api_instance.get_assay_by_external_id(external_id, project)
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

# **get_assay_by_id**
> bool, date, datetime, dict, float, int, list, str, none_type get_assay_by_id(assay_id)

Get Assay By Id

Get assay by ID

### Example

```python
import metamist

from metamist import AssayApi
api_instance = AssayApi()
assay_id = 1 # int | 

# Get Assay By Id
api_response = api_instance.get_assay_by_id(assay_id)
print(api_response)
```


### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **assay_id** | **int**|  |

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

# **get_assays_by_criteria**
> bool, date, datetime, dict, float, int, list, str, none_type get_assays_by_criteria()

Get Assays By Criteria

Get assays by criteria

### Example

```python
import metamist

from metamist import AssayApi
api_instance = AssayApi()
body_get_assays_by_criteria = BodyGetAssaysByCriteria(
        sample_ids=[
            "sample_ids_example",
        ],
        assay_ids=[
            1,
        ],
        external_assay_ids=[
            "external_assay_ids_example",
        ],
        assay_meta={},
        sample_meta={},
        projects=[
            "projects_example",
        ],
        assay_types=[
            "assay_types_example",
        ],
    ) # BodyGetAssaysByCriteria |  (optional)
# Get Assays By Criteria
api_response = api_instance.get_assays_by_criteria(body_get_assays_by_criteria=body_get_assays_by_criteria)
print(api_response)
```


### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **body_get_assays_by_criteria** | [**BodyGetAssaysByCriteria**](BodyGetAssaysByCriteria.md)|  | [optional]

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

# **update_assay**
> bool, date, datetime, dict, float, int, list, str, none_type update_assay(assay_upsert)

Update Assay

Update assay for ID

### Example

```python
import metamist

from metamist import AssayApi
api_instance = AssayApi()
assay_upsert = AssayUpsert(
        id=None,
        type=None,
        external_ids=None,
        sample_id=None,
        meta=None,
    ) # AssayUpsert | 

# Update Assay
api_response = api_instance.update_assay(assay_upsert)
print(api_response)
```


### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **assay_upsert** | [**AssayUpsert**](AssayUpsert.md)|  |

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

