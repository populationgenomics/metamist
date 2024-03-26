# metamist.FamilyApi

Method | HTTP request | Description
------------- | ------------- | -------------
[**get_families**](FamilyApi.md#get_families) | **GET** /api/v1/family/{project}/ | Get Families
[**get_pedigree**](FamilyApi.md#get_pedigree) | **GET** /api/v1/family/{project}/pedigree | Get Pedigree
[**import_families**](FamilyApi.md#import_families) | **POST** /api/v1/family/{project}/family-template | Import Families
[**import_pedigree**](FamilyApi.md#import_pedigree) | **POST** /api/v1/family/{project}/pedigree | Import Pedigree
[**update_family**](FamilyApi.md#update_family) | **POST** /api/v1/family/ | Update Family


# **get_families**
> bool, date, datetime, dict, float, int, list, str, none_type get_families(project)

Get Families

Get families for some project

### Example

```python
import metamist

from metamist import FamilyApi
api_instance = FamilyApi()
project = "project_example" # str | 
participant_ids = [
        1,
    ] # [int] |  (optional)
sample_ids = [
        "sample_ids_example",
    ] # [str] |  (optional)

# Get Families
api_response = api_instance.get_families(project)
print(api_response)
# Get Families
api_response = api_instance.get_families(project, participant_ids=participant_ids, sample_ids=sample_ids)
print(api_response)
```


### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **project** | **str**|  |
 **participant_ids** | **[int]**|  | [optional]
 **sample_ids** | **[str]**|  | [optional]

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

# **get_pedigree**
> bool, date, datetime, dict, float, int, list, str, none_type get_pedigree(project)

Get Pedigree

Generate tab-separated Pedigree file for ALL families unless internal_family_ids is specified.  Allow replacement of internal participant and family IDs with their external counterparts.

### Example

```python
import metamist

from metamist import FamilyApi
api_instance = FamilyApi()
project = "project_example" # str | 
internal_family_ids = [
        1,
    ] # [int] |  (optional)
export_type = None # bool, date, datetime, dict, float, int, list, str, none_type |  (optional)
replace_with_participant_external_ids = True # bool |  (optional) if omitted the server will use the default value of True
replace_with_family_external_ids = True # bool |  (optional) if omitted the server will use the default value of True
include_header = True # bool |  (optional) if omitted the server will use the default value of True
empty_participant_value = "empty_participant_value_example" # str |  (optional)
include_participants_not_in_families = False # bool |  (optional) if omitted the server will use the default value of False

# Get Pedigree
api_response = api_instance.get_pedigree(project)
print(api_response)
# Get Pedigree
api_response = api_instance.get_pedigree(project, internal_family_ids=internal_family_ids, export_type=export_type, replace_with_participant_external_ids=replace_with_participant_external_ids, replace_with_family_external_ids=replace_with_family_external_ids, include_header=include_header, empty_participant_value=empty_participant_value, include_participants_not_in_families=include_participants_not_in_families)
print(api_response)
```


### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **project** | **str**|  |
 **internal_family_ids** | **[int]**|  | [optional]
 **export_type** | **bool, date, datetime, dict, float, int, list, str, none_type**|  | [optional]
 **replace_with_participant_external_ids** | **bool**|  | [optional] if omitted the server will use the default value of True
 **replace_with_family_external_ids** | **bool**|  | [optional] if omitted the server will use the default value of True
 **include_header** | **bool**|  | [optional] if omitted the server will use the default value of True
 **empty_participant_value** | **str**|  | [optional]
 **include_participants_not_in_families** | **bool**|  | [optional] if omitted the server will use the default value of False

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

# **import_families**
> bool, date, datetime, dict, float, int, list, str, none_type import_families(project, file)

Import Families

Import a family csv

### Example

```python
import metamist

from metamist import FamilyApi
api_instance = FamilyApi()
project = "project_example" # str | 
file = open('/path/to/file', 'rb') # file_type | 
has_header = True # bool |  (optional) if omitted the server will use the default value of True
delimiter = "delimiter_example" # str |  (optional)

# Import Families
api_response = api_instance.import_families(project, file)
print(api_response)
# Import Families
api_response = api_instance.import_families(project, file, has_header=has_header, delimiter=delimiter)
print(api_response)
```


### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **project** | **str**|  |
 **file** | **file_type**|  |
 **has_header** | **bool**|  | [optional] if omitted the server will use the default value of True
 **delimiter** | **str**|  | [optional]

### Return type

**bool, date, datetime, dict, float, int, list, str, none_type**

### Authorization

[HTTPBearer](../README.md#HTTPBearer)

### HTTP request headers

 - **Content-Type**: multipart/form-data
 - **Accept**: application/json


### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | Successful Response |  -  |
**422** | Validation Error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **import_pedigree**
> bool, date, datetime, dict, float, int, list, str, none_type import_pedigree(project, file)

Import Pedigree

Import a pedigree

### Example

```python
import metamist

from metamist import FamilyApi
api_instance = FamilyApi()
project = "project_example" # str | 
file = open('/path/to/file', 'rb') # file_type | 
has_header = False # bool |  (optional) if omitted the server will use the default value of False
create_missing_participants = False # bool |  (optional) if omitted the server will use the default value of False
perform_sex_check = True # bool |  (optional) if omitted the server will use the default value of True

# Import Pedigree
api_response = api_instance.import_pedigree(project, file)
print(api_response)
# Import Pedigree
api_response = api_instance.import_pedigree(project, file, has_header=has_header, create_missing_participants=create_missing_participants, perform_sex_check=perform_sex_check)
print(api_response)
```


### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **project** | **str**|  |
 **file** | **file_type**|  |
 **has_header** | **bool**|  | [optional] if omitted the server will use the default value of False
 **create_missing_participants** | **bool**|  | [optional] if omitted the server will use the default value of False
 **perform_sex_check** | **bool**|  | [optional] if omitted the server will use the default value of True

### Return type

**bool, date, datetime, dict, float, int, list, str, none_type**

### Authorization

[HTTPBearer](../README.md#HTTPBearer)

### HTTP request headers

 - **Content-Type**: multipart/form-data
 - **Accept**: application/json


### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | Successful Response |  -  |
**422** | Validation Error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **update_family**
> bool, date, datetime, dict, float, int, list, str, none_type update_family(family_update_model)

Update Family

Update information for a single family

### Example

```python
import metamist

from metamist import FamilyApi
api_instance = FamilyApi()
family_update_model = FamilyUpdateModel(
        id=1,
        external_id="external_id_example",
        description="description_example",
        coded_phenotype="coded_phenotype_example",
    ) # FamilyUpdateModel | 

# Update Family
api_response = api_instance.update_family(family_update_model)
print(api_response)
```


### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **family_update_model** | [**FamilyUpdateModel**](FamilyUpdateModel.md)|  |

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

