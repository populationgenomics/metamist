# metamist.ImportApi

Method | HTTP request | Description
------------- | ------------- | -------------
[**import_individual_metadata_manifest**](ImportApi.md#import_individual_metadata_manifest) | **POST** /api/v1/import/{project}/individual-metadata-manifest | Import Individual Metadata Manifest


# **import_individual_metadata_manifest**
> bool, date, datetime, dict, float, int, list, str, none_type import_individual_metadata_manifest(project, file)

Import Individual Metadata Manifest

Import individual metadata manifest  :param extra_participants_method: If extra participants are in the uploaded file,     add a PARTICIPANT entry for them

### Example

```python
import metamist

from metamist import ImportApi
api_instance = ImportApi()
project = "project_example" # str | 
file = open('/path/to/file', 'rb') # file_type | 
delimiter = "delimiter_example" # str |  (optional)
extra_participants_method = None # bool, date, datetime, dict, float, int, list, str, none_type |  (optional)

# Import Individual Metadata Manifest
api_response = api_instance.import_individual_metadata_manifest(project, file)
print(api_response)
# Import Individual Metadata Manifest
api_response = api_instance.import_individual_metadata_manifest(project, file, delimiter=delimiter, extra_participants_method=extra_participants_method)
print(api_response)
```


### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **project** | **str**|  |
 **file** | **file_type**|  |
 **delimiter** | **str**|  | [optional]
 **extra_participants_method** | **bool, date, datetime, dict, float, int, list, str, none_type**|  | [optional]

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

