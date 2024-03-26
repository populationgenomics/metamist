# metamist.AnalysisApi

Method | HTTP request | Description
------------- | ------------- | -------------
[**create_analysis**](AnalysisApi.md#create_analysis) | **PUT** /api/v1/analysis/{project}/ | Create Analysis
[**get_all_sample_ids_without_analysis_type**](AnalysisApi.md#get_all_sample_ids_without_analysis_type) | **GET** /api/v1/analysis/{project}/type/{analysis_type}/not-included-sample-ids | Get All Sample Ids Without Analysis Type
[**get_analysis_by_id**](AnalysisApi.md#get_analysis_by_id) | **GET** /api/v1/analysis/{analysis_id}/details | Get Analysis By Id
[**get_analysis_runner_log**](AnalysisApi.md#get_analysis_runner_log) | **GET** /api/v1/analysis/analysis-runner | Get Analysis Runner Log
[**get_incomplete_analyses**](AnalysisApi.md#get_incomplete_analyses) | **GET** /api/v1/analysis/{project}/incomplete | Get Incomplete Analyses
[**get_latest_complete_analysis_for_type**](AnalysisApi.md#get_latest_complete_analysis_for_type) | **GET** /api/v1/analysis/{project}/{analysis_type}/latest-complete | Get Latest Complete Analysis For Type
[**get_latest_complete_analysis_for_type_post**](AnalysisApi.md#get_latest_complete_analysis_for_type_post) | **POST** /api/v1/analysis/{project}/{analysis_type}/latest-complete | Get Latest Complete Analysis For Type Post
[**get_proportionate_map**](AnalysisApi.md#get_proportionate_map) | **POST** /api/v1/analysis/cram-proportionate-map | Get Proportionate Map
[**get_samples_reads_map**](AnalysisApi.md#get_samples_reads_map) | **GET** /api/v1/analysis/{project}/sample-cram-path-map | Get Sample Reads Map
[**query_analyses**](AnalysisApi.md#query_analyses) | **POST** /api/v1/analysis/query | Query Analyses
[**update_analysis**](AnalysisApi.md#update_analysis) | **PATCH** /api/v1/analysis/{analysis_id}/ | Update Analysis


# **create_analysis**
> int create_analysis(project, analysis)

Create Analysis

Create a new analysis

### Example

```python
import metamist

from metamist import AnalysisApi
api_instance = AnalysisApi()
project = "project_example" # str | 
analysis = Analysis(
        type="type_example",
        status=AnalysisStatus("queued"),
        id=1,
        output="output_example",
        sequencing_group_ids=[],
        author="author_example",
        timestamp_completed="timestamp_completed_example",
        project=1,
        active=True,
        meta={},
    ) # Analysis | 

# Create Analysis
api_response = api_instance.create_analysis(project, analysis)
print(api_response)
```


### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **project** | **str**|  |
 **analysis** | [**Analysis**](Analysis.md)|  |

### Return type

**int**

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

# **get_all_sample_ids_without_analysis_type**
> bool, date, datetime, dict, float, int, list, str, none_type get_all_sample_ids_without_analysis_type(analysis_type, project)

Get All Sample Ids Without Analysis Type

get_all_sample_ids_without_analysis_type

### Example

```python
import metamist

from metamist import AnalysisApi
api_instance = AnalysisApi()
analysis_type = "analysis_type_example" # str | 
project = "project_example" # str | 

# Get All Sample Ids Without Analysis Type
api_response = api_instance.get_all_sample_ids_without_analysis_type(analysis_type, project)
print(api_response)
```


### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **analysis_type** | **str**|  |
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

# **get_analysis_by_id**
> bool, date, datetime, dict, float, int, list, str, none_type get_analysis_by_id(analysis_id)

Get Analysis By Id

Get analysis by ID

### Example

```python
import metamist

from metamist import AnalysisApi
api_instance = AnalysisApi()
analysis_id = 1 # int | 

# Get Analysis By Id
api_response = api_instance.get_analysis_by_id(analysis_id)
print(api_response)
```


### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **analysis_id** | **int**|  |

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

# **get_analysis_runner_log**
> bool, date, datetime, dict, float, int, list, str, none_type get_analysis_runner_log()

Get Analysis Runner Log

Get log for the analysis-runner, useful for checking this history of analysis

### Example

```python
import metamist

from metamist import AnalysisApi
api_instance = AnalysisApi()
project_names = [
        "project_names_example",
    ] # [str] |  (optional)
output_dir = "output_dir_example" # str |  (optional)
ar_guid = "ar_guid_example" # str |  (optional)
# Get Analysis Runner Log
api_response = api_instance.get_analysis_runner_log(project_names=project_names, output_dir=output_dir, ar_guid=ar_guid)
print(api_response)
```


### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **project_names** | **[str]**|  | [optional]
 **output_dir** | **str**|  | [optional]
 **ar_guid** | **str**|  | [optional]

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

# **get_incomplete_analyses**
> bool, date, datetime, dict, float, int, list, str, none_type get_incomplete_analyses(project)

Get Incomplete Analyses

Get analyses with status queued or in-progress

### Example

```python
import metamist

from metamist import AnalysisApi
api_instance = AnalysisApi()
project = "project_example" # str | 

# Get Incomplete Analyses
api_response = api_instance.get_incomplete_analyses(project)
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

# **get_latest_complete_analysis_for_type**
> bool, date, datetime, dict, float, int, list, str, none_type get_latest_complete_analysis_for_type(analysis_type, project)

Get Latest Complete Analysis For Type

Get (SINGLE) latest complete analysis for some analysis type

### Example

```python
import metamist

from metamist import AnalysisApi
api_instance = AnalysisApi()
analysis_type = "analysis_type_example" # str | 
project = "project_example" # str | 

# Get Latest Complete Analysis For Type
api_response = api_instance.get_latest_complete_analysis_for_type(analysis_type, project)
print(api_response)
```


### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **analysis_type** | **str**|  |
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

# **get_latest_complete_analysis_for_type_post**
> bool, date, datetime, dict, float, int, list, str, none_type get_latest_complete_analysis_for_type_post(analysis_type, project, body_get_latest_complete_analysis_for_type_post)

Get Latest Complete Analysis For Type Post

Get SINGLE latest complete analysis for some analysis type (you can specify meta attributes in this route)

### Example

```python
import metamist

from metamist import AnalysisApi
api_instance = AnalysisApi()
analysis_type = "analysis_type_example" # str | 
project = "project_example" # str | 
body_get_latest_complete_analysis_for_type_post = BodyGetLatestCompleteAnalysisForTypePost(
        meta={},
    ) # BodyGetLatestCompleteAnalysisForTypePost | 

# Get Latest Complete Analysis For Type Post
api_response = api_instance.get_latest_complete_analysis_for_type_post(analysis_type, project, body_get_latest_complete_analysis_for_type_post)
print(api_response)
```


### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **analysis_type** | **str**|  |
 **project** | **str**|  |
 **body_get_latest_complete_analysis_for_type_post** | [**BodyGetLatestCompleteAnalysisForTypePost**](BodyGetLatestCompleteAnalysisForTypePost.md)|  |

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

# **get_proportionate_map**
> bool, date, datetime, dict, float, int, list, str, none_type get_proportionate_map(start, body_get_proportionate_map)

Get Proportionate Map

Get proportionate map of project sizes over time, specifying the temporal methods to use. These will be given back as: {     [temporalMethod]: {         date: Date,         projects: [{ project: string, percentage: float, size: int }]     } }

### Example

```python
import metamist

from metamist import AnalysisApi
api_instance = AnalysisApi()
start = "start_example" # str | 
body_get_proportionate_map = BodyGetProportionateMap(
        projects=[
            "projects_example",
        ],
        temporal_methods=[
            ProportionalDateTemporalMethod("SAMPLE_CREATE_DATE"),
        ],
        sequencing_types=[
            "sequencing_types_example",
        ],
    ) # BodyGetProportionateMap | 
end = "end_example" # str |  (optional)

# Get Proportionate Map
api_response = api_instance.get_proportionate_map(start, body_get_proportionate_map)
print(api_response)
# Get Proportionate Map
api_response = api_instance.get_proportionate_map(start, body_get_proportionate_map, end=end)
print(api_response)
```


### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **start** | **str**|  |
 **body_get_proportionate_map** | [**BodyGetProportionateMap**](BodyGetProportionateMap.md)|  |
 **end** | **str**|  | [optional]

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

# **get_samples_reads_map**
> bool, date, datetime, dict, float, int, list, str, none_type get_samples_reads_map(project)

Get Sample Reads Map

Get map of ExternalSampleId  pathToCram  InternalSeqGroupID for seqr  Note that, in JSON the result is  Description:     Column 1: Individual ID     Column 2: gs:// Google bucket path or server filesystem path for this Individual     Column 3: SequencingGroup ID for this file, if different from the Individual ID.                 Used primarily for gCNV files to identify the sample in the batch path

### Example

```python
import metamist

from metamist import AnalysisApi
api_instance = AnalysisApi()
project = "project_example" # str | 
export_type = None # bool, date, datetime, dict, float, int, list, str, none_type |  (optional)
sequencing_types = [
        "sequencing_types_example",
    ] # [str] |  (optional)

# Get Sample Reads Map
api_response = api_instance.get_samples_reads_map(project)
print(api_response)
# Get Sample Reads Map
api_response = api_instance.get_samples_reads_map(project, export_type=export_type, sequencing_types=sequencing_types)
print(api_response)
```


### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **project** | **str**|  |
 **export_type** | **bool, date, datetime, dict, float, int, list, str, none_type**|  | [optional]
 **sequencing_types** | **[str]**|  | [optional]

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

# **query_analyses**
> bool, date, datetime, dict, float, int, list, str, none_type query_analyses(analysis_query_model)

Query Analyses

Get analyses by some criteria

### Example

```python
import metamist

from metamist import AnalysisApi
api_instance = AnalysisApi()
analysis_query_model = AnalysisQueryModel(
        sample_ids=[
            "sample_ids_example",
        ],
        sequencing_group_ids=[
            "sequencing_group_ids_example",
        ],
        projects=[
            "projects_example",
        ],
        type="type_example",
        status=AnalysisStatus("queued"),
        meta={},
        output="output_example",
        active=True,
    ) # AnalysisQueryModel | 

# Query Analyses
api_response = api_instance.query_analyses(analysis_query_model)
print(api_response)
```


### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **analysis_query_model** | [**AnalysisQueryModel**](AnalysisQueryModel.md)|  |

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

# **update_analysis**
> bool, date, datetime, dict, float, int, list, str, none_type update_analysis(analysis_id, analysis_update_model)

Update Analysis

Update status of analysis

### Example

```python
import metamist

from metamist import AnalysisApi
api_instance = AnalysisApi()
analysis_id = 1 # int | 
analysis_update_model = AnalysisUpdateModel(
        status=AnalysisStatus("queued"),
        output="output_example",
        meta={},
        active=True,
    ) # AnalysisUpdateModel | 

# Update Analysis
api_response = api_instance.update_analysis(analysis_id, analysis_update_model)
print(api_response)
```


### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **analysis_id** | **int**|  |
 **analysis_update_model** | [**AnalysisUpdateModel**](AnalysisUpdateModel.md)|  |

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

