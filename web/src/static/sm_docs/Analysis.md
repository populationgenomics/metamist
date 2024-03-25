# Analysis

Model for Analysis

## Properties
Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**type** | **str** |  | 
**status** | [**AnalysisStatus**](AnalysisStatus.md) |  | 
**id** | **int** |  | [optional] 
**output** | **str** |  | [optional] 
**sequencing_group_ids** | **[str]** |  | [optional]  if omitted the server will use the default value of []
**author** | **str** |  | [optional] 
**timestamp_completed** | **str** |  | [optional] 
**project** | **int** |  | [optional] 
**active** | **bool** |  | [optional] 
**meta** | **{str: (bool, date, datetime, dict, float, int, list, str, none_type)}** |  | [optional]  if omitted the server will use the default value of {}
**any string name** | **bool, date, datetime, dict, float, int, list, str, none_type** | any string name can be used but the value must be the correct type | [optional]

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


