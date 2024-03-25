# metamist.SequencingGroupApi

Method | HTTP request | Description
------------- | ------------- | -------------
[**get_all_sequencing_group_ids_by_sample_by_type**](SequencingGroupApi.md#get_all_sequencing_group_ids_by_sample_by_type) | **GET** /api/v1/sequencing-group/project/{project} | Get All Sequencing Group Ids By Sample By Type
[**get_sequencing_group**](SequencingGroupApi.md#get_sequencing_group) | **GET** /api/v1/sequencing-group{sequencing_group_id} | Get Sequencing Group
[**update_sequencing_group**](SequencingGroupApi.md#update_sequencing_group) | **PATCH** /api/v1/sequencing-group/project/{sequencing_group_id} | Update Sequencing Group


# **get_all_sequencing_group_ids_by_sample_by_type**
> bool, date, datetime, dict, float, int, list, str, none_type get_all_sequencing_group_ids_by_sample_by_type(project)

Get All Sequencing Group Ids By Sample By Type

Creates a new sample, and returns the internal sample ID

### Example

```python
import metamist

from metamist import SequencingGroupApi
api_instance = SequencingGroupApi()
project = "project_example" # str | 

# Get All Sequencing Group Ids By Sample By Type
api_response = api_instance.get_all_sequencing_group_ids_by_sample_by_type(project)
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

# **get_sequencing_group**
> bool, date, datetime, dict, float, int, list, str, none_type get_sequencing_group(sequencing_group_id)

Get Sequencing Group

Creates a new sample, and returns the internal sample ID

### Example

```python
import metamist

from metamist import SequencingGroupApi
api_instance = SequencingGroupApi()
sequencing_group_id = "sequencing_group_id_example" # str | 

# Get Sequencing Group
api_response = api_instance.get_sequencing_group(sequencing_group_id)
print(api_response)
```


### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **sequencing_group_id** | **str**|  |

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

# **update_sequencing_group**
> bool, date, datetime, dict, float, int, list, str, none_type update_sequencing_group(sequencing_group_id, project, sequencing_group_meta_update_model)

Update Sequencing Group

Update the meta fields of a sequencing group

### Example

```python
import metamist

from metamist import SequencingGroupApi
api_instance = SequencingGroupApi()
sequencing_group_id = "sequencing_group_id_example" # str | 
project = "project_example" # str | 
sequencing_group_meta_update_model = SequencingGroupMetaUpdateModel(
        meta={},
    ) # SequencingGroupMetaUpdateModel | 

# Update Sequencing Group
api_response = api_instance.update_sequencing_group(sequencing_group_id, project, sequencing_group_meta_update_model)
print(api_response)
```


### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **sequencing_group_id** | **str**|  |
 **project** | **str**|  |
 **sequencing_group_meta_update_model** | [**SequencingGroupMetaUpdateModel**](SequencingGroupMetaUpdateModel.md)|  |

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

