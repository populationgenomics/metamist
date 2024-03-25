# BillingTotalCostQueryModel

Used to query for billing total cost TODO: needs to be fully implemented, esp. to_filter

## Properties
Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**fields** | [**[BillingColumn]**](BillingColumn.md) |  | 
**start_date** | **str** |  | 
**end_date** | **str** |  | 
**source** | [**BillingSource**](BillingSource.md) |  | [optional] 
**filters** | **{str: (bool, date, datetime, dict, float, int, list, str, none_type)}** |  | [optional] 
**filters_op** | **str** |  | [optional] 
**group_by** | **bool** |  | [optional]  if omitted the server will use the default value of True
**order_by** | **{str: (bool,)}** |  | [optional] 
**limit** | **int** |  | [optional] 
**offset** | **int** |  | [optional] 
**time_column** | [**BillingTimeColumn**](BillingTimeColumn.md) |  | [optional] 
**time_periods** | [**BillingTimePeriods**](BillingTimePeriods.md) |  | [optional] 
**min_cost** | **float** |  | [optional] 
**any string name** | **bool, date, datetime, dict, float, int, list, str, none_type** | any string name can be used but the value must be the correct type | [optional]

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


