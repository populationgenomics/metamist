# metamist.ProjectApi

Method | HTTP request | Description
------------- | ------------- | -------------
[**create_project**](ProjectApi.md#create_project) | **PUT** /api/v1/project/ | Create Project
[**delete_project_data**](ProjectApi.md#delete_project_data) | **DELETE** /api/v1/project/{project} | Delete Project Data
[**get_all_projects**](ProjectApi.md#get_all_projects) | **GET** /api/v1/project/all | Get All Projects
[**get_my_projects**](ProjectApi.md#get_my_projects) | **GET** /api/v1/project/ | Get My Projects
[**get_seqr_projects**](ProjectApi.md#get_seqr_projects) | **GET** /api/v1/project/seqr/all | Get Seqr Projects
[**update_project**](ProjectApi.md#update_project) | **POST** /api/v1/project/{project}/update | Update Project
[**update_project_members**](ProjectApi.md#update_project_members) | **PATCH** /api/v1/project/{project}/members | Update Project Members


# **create_project**
> bool, date, datetime, dict, float, int, list, str, none_type create_project(name, dataset)

Create Project

Create a new project

### Example

```python
import metamist

from metamist import ProjectApi
api_instance = ProjectApi()
name = "name_example" # str | 
dataset = "dataset_example" # str | 
create_test_project = False # bool |  (optional) if omitted the server will use the default value of False

# Create Project
api_response = api_instance.create_project(name, dataset)
print(api_response)
# Create Project
api_response = api_instance.create_project(name, dataset, create_test_project=create_test_project)
print(api_response)
```


### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **name** | **str**|  |
 **dataset** | **str**|  |
 **create_test_project** | **bool**|  | [optional] if omitted the server will use the default value of False

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

# **delete_project_data**
> bool, date, datetime, dict, float, int, list, str, none_type delete_project_data(project)

Delete Project Data

Delete all data in a project by project name. Can optionally delete the project itself. Requires READ access + project-creator permissions

### Example

```python
import metamist

from metamist import ProjectApi
api_instance = ProjectApi()
project = "project_example" # str | 
delete_project = False # bool |  (optional) if omitted the server will use the default value of False

# Delete Project Data
api_response = api_instance.delete_project_data(project)
print(api_response)
# Delete Project Data
api_response = api_instance.delete_project_data(project, delete_project=delete_project)
print(api_response)
```


### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **project** | **str**|  |
 **delete_project** | **bool**|  | [optional] if omitted the server will use the default value of False

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

# **get_all_projects**
> [Project] get_all_projects()

Get All Projects

Get list of projects

### Example

```python
import metamist

from metamist import ProjectApi
api_instance = ProjectApi()
# Get All Projects
api_response = api_instance.get_all_projects()
print(api_response)
```


### Parameters
This endpoint does not need any parameter.

### Return type

[**[Project]**](Project.md)

### Authorization

[HTTPBearer](../README.md#HTTPBearer)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json


### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | Successful Response |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **get_my_projects**
> [str] get_my_projects()

Get My Projects

Get projects I have access to

### Example

```python
import metamist

from metamist import ProjectApi
api_instance = ProjectApi()
# Get My Projects
api_response = api_instance.get_my_projects()
print(api_response)
```


### Parameters
This endpoint does not need any parameter.

### Return type

**[str]**

### Authorization

[HTTPBearer](../README.md#HTTPBearer)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json


### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | Successful Response |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **get_seqr_projects**
> bool, date, datetime, dict, float, int, list, str, none_type get_seqr_projects()

Get Seqr Projects

Get SM projects that should sync to seqr

### Example

```python
import metamist

from metamist import ProjectApi
api_instance = ProjectApi()
# Get Seqr Projects
api_response = api_instance.get_seqr_projects()
print(api_response)
```


### Parameters
This endpoint does not need any parameter.

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

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **update_project**
> bool, date, datetime, dict, float, int, list, str, none_type update_project(project, body)

Update Project

Update a project by project name

### Example

```python
import metamist

from metamist import ProjectApi
api_instance = ProjectApi()
project = "project_example" # str | 
body = {} # {str: (bool, date, datetime, dict, float, int, list, str, none_type)} | 

# Update Project
api_response = api_instance.update_project(project, body)
print(api_response)
```


### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **project** | **str**|  |
 **body** | **{str: (bool, date, datetime, dict, float, int, list, str, none_type)}**|  |

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

# **update_project_members**
> bool, date, datetime, dict, float, int, list, str, none_type update_project_members(project, readonly, request_body)

Update Project Members

Update project members for specific read / write group. Not that this is protected by access to a specific access group

### Example

```python
import metamist

from metamist import ProjectApi
api_instance = ProjectApi()
project = "project_example" # str | 
readonly = True # bool | 
request_body = [
        "request_body_example",
    ] # [str] | 

# Update Project Members
api_response = api_instance.update_project_members(project, readonly, request_body)
print(api_response)
```


### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **project** | **str**|  |
 **readonly** | **bool**|  |
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

