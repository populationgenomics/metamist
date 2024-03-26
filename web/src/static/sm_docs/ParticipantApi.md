# metamist.ParticipantApi

Method | HTTP request | Description
------------- | ------------- | -------------
[**fill_in_missing_participants**](ParticipantApi.md#fill_in_missing_participants) | **POST** /api/v1/participant/{project}/fill-in-missing-participants | Fill In Missing Participants
[**get_external_participant_id_to_sequencing_group_id**](ParticipantApi.md#get_external_participant_id_to_sequencing_group_id) | **GET** /api/v1/participant/{project}/external-pid-to-sg-id | Get External Participant Id To Sequencing Group Id
[**get_individual_metadata_for_seqr**](ParticipantApi.md#get_individual_metadata_for_seqr) | **GET** /api/v1/participant/{project}/individual-metadata-seqr | Get Individual Metadata Template For Seqr
[**get_participant_id_map_by_external_ids**](ParticipantApi.md#get_participant_id_map_by_external_ids) | **POST** /api/v1/participant/{project}/id-map/external | Get Id Map By External Ids
[**get_participants**](ParticipantApi.md#get_participants) | **POST** /api/v1/participant/{project} | Get Participants
[**update_many_participants**](ParticipantApi.md#update_many_participants) | **POST** /api/v1/participant/update-many | Update Many Participant External Ids
[**update_participant**](ParticipantApi.md#update_participant) | **POST** /api/v1/participant/{participant_id}/update-participant | Update Participant
[**update_participant_family**](ParticipantApi.md#update_participant_family) | **POST** /api/v1/participant/{participant_id}/update-participant-family | Update Participant Family
[**upsert_participants**](ParticipantApi.md#upsert_participants) | **PUT** /api/v1/participant/{project}/upsert-many | Upsert Participants


# **fill_in_missing_participants**
> bool, date, datetime, dict, float, int, list, str, none_type fill_in_missing_participants(project)

Fill In Missing Participants

Create a corresponding participant (if required) for each sample within a project, useful for then importing a pedigree

### Example

```python
import metamist

from metamist import ParticipantApi
api_instance = ParticipantApi()
project = "project_example" # str | 

# Fill In Missing Participants
api_response = api_instance.fill_in_missing_participants(project)
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

# **get_external_participant_id_to_sequencing_group_id**
> bool, date, datetime, dict, float, int, list, str, none_type get_external_participant_id_to_sequencing_group_id(project)

Get External Participant Id To Sequencing Group Id

Get csv / tsv export of external_participant_id to sequencing_group_id  Get a map of {external_participant_id} -> {sequencing_group_id} useful to matching joint-called sequencing groups in the matrix table to the participant  Return a list not dictionary, because dict could lose participants with multiple samples.  :param sequencing_type: Leave empty to get all sequencing types :param flip_columns: Set to True when exporting for seqr

### Example

```python
import metamist

from metamist import ParticipantApi
api_instance = ParticipantApi()
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

# **get_individual_metadata_for_seqr**
> bool, date, datetime, dict, float, int, list, str, none_type get_individual_metadata_for_seqr(project)

Get Individual Metadata Template For Seqr

Get individual metadata template for SEQR as a CSV

### Example

```python
import metamist

from metamist import ParticipantApi
api_instance = ParticipantApi()
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

# **get_participant_id_map_by_external_ids**
> bool, date, datetime, dict, float, int, list, str, none_type get_participant_id_map_by_external_ids(project, request_body)

Get Id Map By External Ids

Get ID map of participants, by external_id

### Example

```python
import metamist

from metamist import ParticipantApi
api_instance = ParticipantApi()
project = "project_example" # str | 
request_body = [
        "request_body_example",
    ] # [str] | 
allow_missing = False # bool |  (optional) if omitted the server will use the default value of False

# Get Id Map By External Ids
api_response = api_instance.get_participant_id_map_by_external_ids(project, request_body)
print(api_response)
# Get Id Map By External Ids
api_response = api_instance.get_participant_id_map_by_external_ids(project, request_body, allow_missing=allow_missing)
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

# **get_participants**
> bool, date, datetime, dict, float, int, list, str, none_type get_participants(project)

Get Participants

Get participants, default ALL participants in project

### Example

```python
import metamist

from metamist import ParticipantApi
api_instance = ParticipantApi()
project = "project_example" # str | 
body_get_participants = BodyGetParticipants(
        external_participant_ids=[
            "external_participant_ids_example",
        ],
        internal_participant_ids=[
            1,
        ],
    ) # BodyGetParticipants |  (optional)

# Get Participants
api_response = api_instance.get_participants(project)
print(api_response)
# Get Participants
api_response = api_instance.get_participants(project, body_get_participants=body_get_participants)
print(api_response)
```


### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **project** | **str**|  |
 **body_get_participants** | [**BodyGetParticipants**](BodyGetParticipants.md)|  | [optional]

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

# **update_many_participants**
> bool, date, datetime, dict, float, int, list, str, none_type update_many_participants(request_body)

Update Many Participant External Ids

Update external_ids of participants by providing an update map

### Example

```python
import metamist

from metamist import ParticipantApi
api_instance = ParticipantApi()
request_body = {
        "key": "key_example",
    } # {str: (str,)} | 

# Update Many Participant External Ids
api_response = api_instance.update_many_participants(request_body)
print(api_response)
```


### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
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

# **update_participant**
> bool, date, datetime, dict, float, int, list, str, none_type update_participant(participant_id, participant_upsert)

Update Participant

Update Participant Data

### Example

```python
import metamist

from metamist import ParticipantApi
api_instance = ParticipantApi()
participant_id = 1 # int | 
participant_upsert = ParticipantUpsert(
        id=None,
        external_id=None,
        reported_sex=None,
        reported_gender=None,
        karyotype=None,
        meta=None,
        samples=[
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
        ],
    ) # ParticipantUpsert | 

# Update Participant
api_response = api_instance.update_participant(participant_id, participant_upsert)
print(api_response)
```


### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **participant_id** | **int**|  |
 **participant_upsert** | [**ParticipantUpsert**](ParticipantUpsert.md)|  |

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

# **update_participant_family**
> bool, date, datetime, dict, float, int, list, str, none_type update_participant_family(participant_id, old_family_id, new_family_id)

Update Participant Family

Change a participants family from old_family_id to new_family_id, maintaining all other fields. The new_family_id must already exist.

### Example

```python
import metamist

from metamist import ParticipantApi
api_instance = ParticipantApi()
participant_id = 1 # int | 
old_family_id = 1 # int | 
new_family_id = 1 # int | 

# Update Participant Family
api_response = api_instance.update_participant_family(participant_id, old_family_id, new_family_id)
print(api_response)
```


### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **participant_id** | **int**|  |
 **old_family_id** | **int**|  |
 **new_family_id** | **int**|  |

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

# **upsert_participants**
> bool, date, datetime, dict, float, int, list, str, none_type upsert_participants(project, participant_upsert)

Upsert Participants

Upserts a list of participants with samples and sequences Returns the list of internal sample IDs

### Example

```python
import metamist

from metamist import ParticipantApi
api_instance = ParticipantApi()
project = "project_example" # str | 
participant_upsert = [
        ParticipantUpsert(
            id=None,
            external_id=None,
            reported_sex=None,
            reported_gender=None,
            karyotype=None,
            meta=None,
            samples=[
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
            ],
        ),
    ] # [ParticipantUpsert] | 

# Upsert Participants
api_response = api_instance.upsert_participants(project, participant_upsert)
print(api_response)
```


### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **project** | **str**|  |
 **participant_upsert** | [**[ParticipantUpsert]**](ParticipantUpsert.md)|  |

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

