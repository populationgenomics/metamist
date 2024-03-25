# metamist.WebApi

Method | HTTP request | Description
------------- | ------------- | -------------
[**get_project_summary**](WebApi.md#get_project_summary) | **POST** /api/v1/web/{project}/summary | Get Project Summary
[**search_by_keyword**](WebApi.md#search_by_keyword) | **GET** /api/v1/web/search | Search By Keyword
[**sync_seqr_project**](WebApi.md#sync_seqr_project) | **POST** /api/v1/web/{project}/{sequencing_type}/sync-dataset | Sync Seqr Project


# **get_project_summary**
> ProjectSummary get_project_summary(project, search_item)

Get Project Summary

Creates a new sample, and returns the internal sample ID

### Example

```python
import metamist

from metamist import WebApi
api_instance = WebApi()
project = "project_example" # str | 
search_item = [
        SearchItem(
            model_type=MetaSearchEntityPrefix("p"),
            query="query_example",
            field="field_example",
            is_meta=True,
        ),
    ] # [SearchItem] | 
limit = 20 # int |  (optional) if omitted the server will use the default value of 20
token = 0 # int |  (optional) if omitted the server will use the default value of 0

# Get Project Summary
api_response = api_instance.get_project_summary(project, search_item)
print(api_response)
# Get Project Summary
api_response = api_instance.get_project_summary(project, search_item, limit=limit, token=token)
print(api_response)
```


### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **project** | **str**|  |
 **search_item** | [**[SearchItem]**](SearchItem.md)|  |
 **limit** | **int**|  | [optional] if omitted the server will use the default value of 20
 **token** | **int**|  | [optional] if omitted the server will use the default value of 0

### Return type

[**ProjectSummary**](ProjectSummary.md)

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

# **search_by_keyword**
> SearchResponseModel search_by_keyword(keyword)

Search By Keyword

This searches the keyword, in families, participants + samples in the projects that you are a part of (automatically).

### Example

```python
import metamist

from metamist import WebApi
api_instance = WebApi()
keyword = "keyword_example" # str | 

# Search By Keyword
api_response = api_instance.search_by_keyword(keyword)
print(api_response)
```


### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **keyword** | **str**|  |

### Return type

[**SearchResponseModel**](SearchResponseModel.md)

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

# **sync_seqr_project**
> bool, date, datetime, dict, float, int, list, str, none_type sync_seqr_project(sequencing_type, project, seqr_dataset_type)

Sync Seqr Project

Sync a metamist project with its seqr project (for a specific sequence type) es_index_types: list of any of 'Haplotypecaller', 'SV_Caller', 'Mitochondria_Caller'

### Example

```python
import metamist

from metamist import WebApi
api_instance = WebApi()
sequencing_type = "sequencing_type_example" # str | 
project = "project_example" # str | 
seqr_dataset_type = [
        SeqrDatasetType("SNV_INDEL"),
    ] # [SeqrDatasetType] | 
sync_families = True # bool |  (optional) if omitted the server will use the default value of True
sync_individual_metadata = True # bool |  (optional) if omitted the server will use the default value of True
sync_individuals = True # bool |  (optional) if omitted the server will use the default value of True
sync_es_index = True # bool |  (optional) if omitted the server will use the default value of True
sync_saved_variants = True # bool |  (optional) if omitted the server will use the default value of True
sync_cram_map = True # bool |  (optional) if omitted the server will use the default value of True
post_slack_notification = True # bool |  (optional) if omitted the server will use the default value of True

# Sync Seqr Project
api_response = api_instance.sync_seqr_project(sequencing_type, project, seqr_dataset_type)
print(api_response)
# Sync Seqr Project
api_response = api_instance.sync_seqr_project(sequencing_type, project, seqr_dataset_type, sync_families=sync_families, sync_individual_metadata=sync_individual_metadata, sync_individuals=sync_individuals, sync_es_index=sync_es_index, sync_saved_variants=sync_saved_variants, sync_cram_map=sync_cram_map, post_slack_notification=post_slack_notification)
print(api_response)
```


### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **sequencing_type** | **str**|  |
 **project** | **str**|  |
 **seqr_dataset_type** | [**[SeqrDatasetType]**](SeqrDatasetType.md)|  |
 **sync_families** | **bool**|  | [optional] if omitted the server will use the default value of True
 **sync_individual_metadata** | **bool**|  | [optional] if omitted the server will use the default value of True
 **sync_individuals** | **bool**|  | [optional] if omitted the server will use the default value of True
 **sync_es_index** | **bool**|  | [optional] if omitted the server will use the default value of True
 **sync_saved_variants** | **bool**|  | [optional] if omitted the server will use the default value of True
 **sync_cram_map** | **bool**|  | [optional] if omitted the server will use the default value of True
 **post_slack_notification** | **bool**|  | [optional] if omitted the server will use the default value of True

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

