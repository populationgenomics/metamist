# metamist.AnalysisRunnerApi

Method | HTTP request | Description
------------- | ------------- | -------------
[**create_analysis_runner_log**](AnalysisRunnerApi.md#create_analysis_runner_log) | **PUT** /api/v1/analysis-runner/{project}/ | Create Analysis Runner Log
[**get_analysis_runner_logs**](AnalysisRunnerApi.md#get_analysis_runner_logs) | **GET** /api/v1/analysis-runner/{project}/ | Get Analysis Runner Logs


# **create_analysis_runner_log**
> bool, date, datetime, dict, float, int, list, str, none_type create_analysis_runner_log(project, ar_guid, access_level, repository, commit, script, description, driver_image, config_path, cwd, environment, hail_version, batch_url, submitting_user, output_path, request_body)

Create Analysis Runner Log

Create a new analysis runner log

### Example

```python
import metamist

from metamist import AnalysisRunnerApi
api_instance = AnalysisRunnerApi()
project = "project_example" # str | 
ar_guid = "ar_guid_example" # str | 
access_level = "access_level_example" # str | 
repository = "repository_example" # str | 
commit = "commit_example" # str | 
script = "script_example" # str | 
description = "description_example" # str | 
driver_image = "driver_image_example" # str | 
config_path = "config_path_example" # str | 
cwd = "cwd_example" # str | 
environment = "environment_example" # str | 
hail_version = "hail_version_example" # str | 
batch_url = "batch_url_example" # str | 
submitting_user = "submitting_user_example" # str | 
output_path = "output_path_example" # str | 
request_body = {
        "key": "key_example",
    } # {str: (str,)} | 

# Create Analysis Runner Log
api_response = api_instance.create_analysis_runner_log(project, ar_guid, access_level, repository, commit, script, description, driver_image, config_path, cwd, environment, hail_version, batch_url, submitting_user, output_path, request_body)
print(api_response)
```


### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **project** | **str**|  |
 **ar_guid** | **str**|  |
 **access_level** | **str**|  |
 **repository** | **str**|  |
 **commit** | **str**|  |
 **script** | **str**|  |
 **description** | **str**|  |
 **driver_image** | **str**|  |
 **config_path** | **str**|  |
 **cwd** | **str**|  |
 **environment** | **str**|  |
 **hail_version** | **str**|  |
 **batch_url** | **str**|  |
 **submitting_user** | **str**|  |
 **output_path** | **str**|  |
 **request_body** | **{str: (str,)}**|  |

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

# **get_analysis_runner_logs**
> bool, date, datetime, dict, float, int, list, str, none_type get_analysis_runner_logs(project)

Get Analysis Runner Logs

Get analysis runner logs

### Example

```python
import metamist

from metamist import AnalysisRunnerApi
api_instance = AnalysisRunnerApi()
project = "project_example" # str | 
ar_guid = "ar_guid_example" # str |  (optional)
submitting_user = "submitting_user_example" # str |  (optional)
repository = "repository_example" # str |  (optional)
access_level = "access_level_example" # str |  (optional)
environment = "environment_example" # str |  (optional)

# Get Analysis Runner Logs
api_response = api_instance.get_analysis_runner_logs(project)
print(api_response)
# Get Analysis Runner Logs
api_response = api_instance.get_analysis_runner_logs(project, ar_guid=ar_guid, submitting_user=submitting_user, repository=repository, access_level=access_level, environment=environment)
print(api_response)
```


### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **project** | **str**|  |
 **ar_guid** | **str**|  | [optional]
 **submitting_user** | **str**|  | [optional]
 **repository** | **str**|  | [optional]
 **access_level** | **str**|  | [optional]
 **environment** | **str**|  | [optional]

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

