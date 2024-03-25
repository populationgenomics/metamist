# metamist.SeqrApi

Method | HTTP request | Description
------------- | ------------- | -------------
[**get_external_participant_id_to_sequencing_group_id**](SeqrApi.md#get_external_participant_id_to_sequencing_group_id) | **GET** /api/v1/participant/{project}/external-pid-to-sg-id | Get External Participant Id To Sequencing Group Id
[**get_families**](SeqrApi.md#get_families) | **GET** /api/v1/family/{project}/ | Get Families
[**get_individual_metadata_for_seqr**](SeqrApi.md#get_individual_metadata_for_seqr) | **GET** /api/v1/participant/{project}/individual-metadata-seqr | Get Individual Metadata Template For Seqr
[**get_pedigree**](SeqrApi.md#get_pedigree) | **GET** /api/v1/family/{project}/pedigree | Get Pedigree
[**get_samples_reads_map**](SeqrApi.md#get_samples_reads_map) | **GET** /api/v1/analysis/{project}/sample-cram-path-map | Get Sample Reads Map
[**import_families**](SeqrApi.md#import_families) | **POST** /api/v1/family/{project}/family-template | Import Families
[**import_individual_metadata_manifest**](SeqrApi.md#import_individual_metadata_manifest) | **POST** /api/v1/import/{project}/individual-metadata-manifest | Import Individual Metadata Manifest
[**import_pedigree**](SeqrApi.md#import_pedigree) | **POST** /api/v1/family/{project}/pedigree | Import Pedigree


# **get_external_participant_id_to_sequencing_group_id**
> bool, date, datetime, dict, float, int, list, str, none_type get_external_participant_id_to_sequencing_group_id(project)

Get External Participant Id To Sequencing Group Id

Get csv / tsv export of external_participant_id to sequencing_group_id  Get a map of {external_participant_id} -> {sequencing_group_id} useful to matching joint-called sequencing groups in the matrix table to the participant  Return a list not dictionary, because dict could lose participants with multiple samples.  :param sequencing_type: Leave empty to get all sequencing types :param flip_columns: Set to True when exporting for seqr

### Example

```python
import metamist

from metamist import SeqrApi
api_instance = SeqrApi()
project = "project_example" # str | 
sequencing_type = "sequencing_type_example" # str |  (optional)
export_type = None # bool, date, datetime, dict, float, int, list, str, none_type |  (optional)
flip_columns = False # bool |  (optional) if omitted the server will use the default value of False

# Get External Participant Id To Sequencing Group Id
api_response = api_instance.get_external_participant_id_to_sequencing_group_id(project)
print(api_response)
# Get External Participant Id To Sequencing Group Id
api_response = api_instance.get_external_participant_id_to_sequencing_group_id(project, sequencing_type=sequencing_type, export_type=export_type, flip_columns=flip_columns)
print(api_response)
```


### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **project** | **str**|  |
 **sequencing_type** | **str**|  | [optional]
 **export_type** | **bool, date, datetime, dict, float, int, list, str, none_type**|  | [optional]
 **flip_columns** | **bool**|  | [optional] if omitted the server will use the default value of False

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

# **get_families**
> bool, date, datetime, dict, float, int, list, str, none_type get_families(project)

Get Families

Get families for some project

### Example

```python
import metamist

from metamist import SeqrApi
api_instance = SeqrApi()
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

# **get_individual_metadata_for_seqr**
> bool, date, datetime, dict, float, int, list, str, none_type get_individual_metadata_for_seqr(project)

Get Individual Metadata Template For Seqr

Get individual metadata template for SEQR as a CSV

### Example

```python
import metamist

from metamist import SeqrApi
api_instance = SeqrApi()
project = "project_example" # str | 
export_type = None # bool, date, datetime, dict, float, int, list, str, none_type |  (optional)
external_participant_ids = [
        "external_participant_ids_example",
    ] # [str] |  (optional)
replace_with_participant_external_ids = True # bool |  (optional) if omitted the server will use the default value of True

# Get Individual Metadata Template For Seqr
api_response = api_instance.get_individual_metadata_for_seqr(project)
print(api_response)
# Get Individual Metadata Template For Seqr
api_response = api_instance.get_individual_metadata_for_seqr(project, export_type=export_type, external_participant_ids=external_participant_ids, replace_with_participant_external_ids=replace_with_participant_external_ids)
print(api_response)
```


### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **project** | **str**|  |
 **export_type** | **bool, date, datetime, dict, float, int, list, str, none_type**|  | [optional]
 **external_participant_ids** | **[str]**|  | [optional]
 **replace_with_participant_external_ids** | **bool**|  | [optional] if omitted the server will use the default value of True

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

from metamist import SeqrApi
api_instance = SeqrApi()
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

# **get_samples_reads_map**
> bool, date, datetime, dict, float, int, list, str, none_type get_samples_reads_map(project)

Get Sample Reads Map

Get map of ExternalSampleId  pathToCram  InternalSeqGroupID for seqr  Note that, in JSON the result is  Description:     Column 1: Individual ID     Column 2: gs:// Google bucket path or server filesystem path for this Individual     Column 3: SequencingGroup ID for this file, if different from the Individual ID.                 Used primarily for gCNV files to identify the sample in the batch path

### Example

```python
import metamist

from metamist import SeqrApi
api_instance = SeqrApi()
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

# **import_families**
> bool, date, datetime, dict, float, int, list, str, none_type import_families(project, file)

Import Families

Import a family csv

### Example

```python
import metamist

from metamist import SeqrApi
api_instance = SeqrApi()
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

# **import_individual_metadata_manifest**
> bool, date, datetime, dict, float, int, list, str, none_type import_individual_metadata_manifest(project, file)

Import Individual Metadata Manifest

Import individual metadata manifest  :param extra_participants_method: If extra participants are in the uploaded file,     add a PARTICIPANT entry for them

### Example

```python
import metamist

from metamist import SeqrApi
api_instance = SeqrApi()
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

# **import_pedigree**
> bool, date, datetime, dict, float, int, list, str, none_type import_pedigree(project, file)

Import Pedigree

Import a pedigree

### Example

```python
import metamist

from metamist import SeqrApi
api_instance = SeqrApi()
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

