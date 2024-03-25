# ProjectSummary

Return class for the project summary endpoint

## Properties
Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**project** | [**WebProject**](WebProject.md) |  | 
**total_samples** | **int** |  | 
**total_samples_in_query** | **int** |  | 
**total_participants** | **int** |  | 
**total_sequencing_groups** | **int** |  | 
**total_assays** | **int** |  | 
**cram_seqr_stats** | **{str: ({str: (str,)},)}** |  | 
**batch_sequencing_group_stats** | **{str: ({str: (str,)},)}** |  | 
**participants** | [**[NestedParticipant]**](NestedParticipant.md) |  | 
**participant_keys** | **[list]** |  | 
**sample_keys** | **[list]** |  | 
**sequencing_group_keys** | **[list]** |  | 
**assay_keys** | **[list]** |  | 
**seqr_links** | **{str: (str,)}** |  | 
**seqr_sync_types** | **[str]** |  | 
**links** | [**PagingLinks**](PagingLinks.md) |  | [optional] 
**any string name** | **bool, date, datetime, dict, float, int, list, str, none_type** | any string name can be used but the value must be the correct type | [optional]

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


