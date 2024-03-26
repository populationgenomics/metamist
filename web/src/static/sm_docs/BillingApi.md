# metamist.BillingApi

Method | HTTP request | Description
------------- | ------------- | -------------
[**cost_by_ar_guid**](BillingApi.md#cost_by_ar_guid) | **GET** /api/v1/billing/cost-by-ar-guid/{ar_guid} | Get Cost By Ar Guid
[**cost_by_batch_id**](BillingApi.md#cost_by_batch_id) | **GET** /api/v1/billing/cost-by-batch-id/{batch_id} | Get Cost By Batch Id
[**get_compute_categories**](BillingApi.md#get_compute_categories) | **GET** /api/v1/billing/compute-categories | Get Compute Categories
[**get_cost_categories**](BillingApi.md#get_cost_categories) | **GET** /api/v1/billing/categories | Get Cost Categories
[**get_cromwell_sub_workflow_names**](BillingApi.md#get_cromwell_sub_workflow_names) | **GET** /api/v1/billing/cromwell-sub-workflow-names | Get Cromwell Sub Workflow Names
[**get_datasets**](BillingApi.md#get_datasets) | **GET** /api/v1/billing/datasets | Get Datasets
[**get_gcp_projects**](BillingApi.md#get_gcp_projects) | **GET** /api/v1/billing/gcp-projects | Get Gcp Projects
[**get_invoice_months**](BillingApi.md#get_invoice_months) | **GET** /api/v1/billing/invoice-months | Get Invoice Months
[**get_namespaces**](BillingApi.md#get_namespaces) | **GET** /api/v1/billing/namespaces | Get Namespaces
[**get_running_cost**](BillingApi.md#get_running_cost) | **GET** /api/v1/billing/running-cost/{field} | Get Running Costs
[**get_sequencing_groups**](BillingApi.md#get_sequencing_groups) | **GET** /api/v1/billing/sequencing-groups | Get Sequencing Groups
[**get_sequencing_types**](BillingApi.md#get_sequencing_types) | **GET** /api/v1/billing/sequencing-types | Get Sequencing Types
[**get_skus**](BillingApi.md#get_skus) | **GET** /api/v1/billing/skus | Get Skus
[**get_stages**](BillingApi.md#get_stages) | **GET** /api/v1/billing/stages | Get Stages
[**get_topics**](BillingApi.md#get_topics) | **GET** /api/v1/billing/topics | Get Topics
[**get_total_cost**](BillingApi.md#get_total_cost) | **POST** /api/v1/billing/total-cost | Get Total Cost
[**get_wdl_task_names**](BillingApi.md#get_wdl_task_names) | **GET** /api/v1/billing/wdl-task-names | Get Wdl Task Names
[**is_billing_enabled**](BillingApi.md#is_billing_enabled) | **GET** /api/v1/billing/is-billing-enabled | Is Billing Enabled


# **cost_by_ar_guid**
> [BillingBatchCostRecord] cost_by_ar_guid(ar_guid)

Get Cost By Ar Guid

Get Hail Batch costs by AR GUID

### Example

```python
import metamist

from metamist import BillingApi
api_instance = BillingApi()
ar_guid = "ar_guid_example" # str | 

# Get Cost By Ar Guid
api_response = api_instance.cost_by_ar_guid(ar_guid)
print(api_response)
```


### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **ar_guid** | **str**|  |

### Return type

[**[BillingBatchCostRecord]**](BillingBatchCostRecord.md)

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

# **cost_by_batch_id**
> [BillingBatchCostRecord] cost_by_batch_id(batch_id)

Get Cost By Batch Id

Get Hail Batch costs by Batch ID

### Example

```python
import metamist

from metamist import BillingApi
api_instance = BillingApi()
batch_id = "batch_id_example" # str | 

# Get Cost By Batch Id
api_response = api_instance.cost_by_batch_id(batch_id)
print(api_response)
```


### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **batch_id** | **str**|  |

### Return type

[**[BillingBatchCostRecord]**](BillingBatchCostRecord.md)

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

# **get_compute_categories**
> [str] get_compute_categories()

Get Compute Categories

Get list of all compute categories in database Results are sorted ASC

### Example

```python
import metamist

from metamist import BillingApi
api_instance = BillingApi()
# Get Compute Categories
api_response = api_instance.get_compute_categories()
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

# **get_cost_categories**
> [str] get_cost_categories()

Get Cost Categories

Get list of all service description / cost categories in database

### Example

```python
import metamist

from metamist import BillingApi
api_instance = BillingApi()
# Get Cost Categories
api_response = api_instance.get_cost_categories()
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

# **get_cromwell_sub_workflow_names**
> [str] get_cromwell_sub_workflow_names()

Get Cromwell Sub Workflow Names

Get list of all cromwell_sub_workflow_names in database Results are sorted ASC

### Example

```python
import metamist

from metamist import BillingApi
api_instance = BillingApi()
# Get Cromwell Sub Workflow Names
api_response = api_instance.get_cromwell_sub_workflow_names()
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

# **get_datasets**
> [str] get_datasets()

Get Datasets

Get list of all datasets in database Results are sorted ASC

### Example

```python
import metamist

from metamist import BillingApi
api_instance = BillingApi()
# Get Datasets
api_response = api_instance.get_datasets()
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

# **get_gcp_projects**
> [str] get_gcp_projects()

Get Gcp Projects

Get list of all GCP projects in database

### Example

```python
import metamist

from metamist import BillingApi
api_instance = BillingApi()
# Get Gcp Projects
api_response = api_instance.get_gcp_projects()
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

# **get_invoice_months**
> [str] get_invoice_months()

Get Invoice Months

Get list of all invoice months in database Results are sorted DESC

### Example

```python
import metamist

from metamist import BillingApi
api_instance = BillingApi()
# Get Invoice Months
api_response = api_instance.get_invoice_months()
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

# **get_namespaces**
> [str] get_namespaces()

Get Namespaces

Get list of all namespaces in database Results are sorted DESC

### Example

```python
import metamist

from metamist import BillingApi
api_instance = BillingApi()
# Get Namespaces
api_response = api_instance.get_namespaces()
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

# **get_running_cost**
> [BillingCostBudgetRecord] get_running_cost(field)

Get Running Costs

Get running cost for specified fields in database e.g. fields = ['gcp_project', 'topic', 'wdl_task_names', 'cromwell_sub_workflow_name', 'compute_category']

### Example

```python
import metamist

from metamist import BillingApi
api_instance = BillingApi()
field = BillingColumn("id") # BillingColumn | 
invoice_month = "invoice_month_example" # str |  (optional)
source = BillingSource("raw") # BillingSource |  (optional)

# Get Running Costs
api_response = api_instance.get_running_cost(field)
print(api_response)
# Get Running Costs
api_response = api_instance.get_running_cost(field, invoice_month=invoice_month, source=source)
print(api_response)
```


### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **field** | **BillingColumn**|  |
 **invoice_month** | **str**|  | [optional]
 **source** | **BillingSource**|  | [optional]

### Return type

[**[BillingCostBudgetRecord]**](BillingCostBudgetRecord.md)

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

# **get_sequencing_groups**
> [str] get_sequencing_groups()

Get Sequencing Groups

Get list of all sequencing_groups in database Results are sorted ASC

### Example

```python
import metamist

from metamist import BillingApi
api_instance = BillingApi()
# Get Sequencing Groups
api_response = api_instance.get_sequencing_groups()
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

# **get_sequencing_types**
> [str] get_sequencing_types()

Get Sequencing Types

Get list of all sequencing_types in database Results are sorted ASC

### Example

```python
import metamist

from metamist import BillingApi
api_instance = BillingApi()
# Get Sequencing Types
api_response = api_instance.get_sequencing_types()
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

# **get_skus**
> [str] get_skus()

Get Skus

Get list of all SKUs in database There is over 400 Skus so limit is required Results are sorted ASC

### Example

```python
import metamist

from metamist import BillingApi
api_instance = BillingApi()
limit = 10 # int |  (optional) if omitted the server will use the default value of 10
offset = 1 # int |  (optional)
# Get Skus
api_response = api_instance.get_skus(limit=limit, offset=offset)
print(api_response)
```


### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **limit** | **int**|  | [optional] if omitted the server will use the default value of 10
 **offset** | **int**|  | [optional]

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
**422** | Validation Error |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **get_stages**
> [str] get_stages()

Get Stages

Get list of all stages in database Results are sorted ASC

### Example

```python
import metamist

from metamist import BillingApi
api_instance = BillingApi()
# Get Stages
api_response = api_instance.get_stages()
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

# **get_topics**
> [str] get_topics()

Get Topics

Get list of all topics in database

### Example

```python
import metamist

from metamist import BillingApi
api_instance = BillingApi()
# Get Topics
api_response = api_instance.get_topics()
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

# **get_total_cost**
> [BillingTotalCostRecord] get_total_cost(billing_total_cost_query_model)

Get Total Cost

Get Total cost of selected fields for requested time interval  Here are few examples of requests:  1. Get total topic for month of March 2023, ordered by cost DESC:      {         \"fields\": [\"topic\"],         \"start_date\": \"2023-03-01\",         \"end_date\": \"2023-03-31\",         \"order_by\": {\"cost\": true},         \"source\": \"aggregate\"     }  2. Get total cost by day and topic for March 2023, order by day ASC and topic DESC:      {         \"fields\": [\"day\", \"topic\"],         \"start_date\": \"2023-03-01\",         \"end_date\": \"2023-03-05\",         \"order_by\": {\"day\": false, \"topic\": true},         \"limit\": 10     }  3. Get total cost of sku and cost_category for topic 'hail', March 2023,    order by cost DESC with Limit and Offset:      {         \"fields\": [\"sku\", \"cost_category\"],         \"start_date\": \"2023-03-01\",         \"end_date\": \"2023-03-31\",         \"filters\": { \"topic\": \"hail\"},         \"order_by\": {\"cost\": true},         \"limit\": 5,         \"offset\": 10     }  4. Get total cost per dataset for month of March, order by cost DESC:      {         \"fields\": [\"dataset\"],         \"start_date\": \"2023-03-01\",         \"end_date\": \"2023-03-31\",         \"order_by\": {\"cost\": true}     }  5. Get total cost of daily cost for dataset 'acute-care', March 2023,    order by day ASC and dataset DESC:      {         \"fields\": [\"day\", \"dataset\"],         \"start_date\": \"2023-03-01\",         \"end_date\": \"2023-03-31\",         \"filters\": { \"dataset\": \"acute-care\"},         \"order_by\": {\"day\": false, \"dataset\": true}     }  6. Get total cost for given batch_id and category for month of March,    order by cost DESC:      {         \"fields\": [\"cost_category\", \"batch_id\", \"sku\"],         \"start_date\": \"2023-03-01\",         \"end_date\": \"2023-03-31\",         \"filters\": { \"batch_id\": \"423094\"},         \"order_by\": {\"cost\": true},         \"limit\": 5     }  7. Get total sequencing_type for month of March 2023, ordered by cost DESC:      {         \"fields\": [\"sequencing_type\"],         \"start_date\": \"2023-03-01\",         \"end_date\": \"2023-03-31\",         \"order_by\": {\"cost\": true}     }  8. Get total stage for month of March 2023, ordered by cost DESC:      {         \"fields\": [\"stage\"],         \"start_date\": \"2023-03-01\",         \"end_date\": \"2023-03-31\",         \"order_by\": {\"cost\": true}     }  9. Get total sequencing_group for month of March 2023, ordered by cost DESC:      {         \"fields\": [\"sequencing_group\"],         \"start_date\": \"2023-03-01\",         \"end_date\": \"2023-03-31\",         \"order_by\": {\"cost\": true}     }  10. Get total gcp_project for month of March 2023, ordered by cost DESC:      {         \"fields\": [\"gcp_project\"],         \"start_date\": \"2023-03-01\",         \"end_date\": \"2023-03-31\",         \"order_by\": {\"cost\": true},         \"source\": \"gcp_billing\"     }  11. Get total cost by sku for given ar_guid, order by cost DESC:      {         \"fields\": [\"sku\"],         \"start_date\": \"2023-10-23\",         \"end_date\": \"2023-10-23\",         \"filters\": { \"ar_guid\": \"4e53702e-8b6c-48ea-857f-c5d33b7e72d7\"},         \"order_by\": {\"cost\": true}     }  12. Get total cost by compute_category order by cost DESC:      {         \"fields\": [\"compute_category\"],         \"start_date\": \"2023-11-10\",         \"end_date\": \"2023-11-10\",         \"order_by\": {\"cost\": true}     }  13. Get total cost by cromwell_sub_workflow_name, order by cost DESC:      {         \"fields\": [\"cromwell_sub_workflow_name\"],         \"start_date\": \"2023-11-10\",         \"end_date\": \"2023-11-10\",         \"order_by\": {\"cost\": true}     }  14. Get total cost by sku for given cromwell_workflow_id, order by cost DESC:      {         \"fields\": [\"sku\"],         \"start_date\": \"2023-11-10\",         \"end_date\": \"2023-11-10\",         \"filters\": {\"cromwell_workflow_id\": \"cromwell-00448f7b-8ef3-4d22-80ab-e302acdb2d28\"},         \"order_by\": {\"cost\": true}     }  15. Get total cost by sku for given goog_pipelines_worker, order by cost DESC:      {         \"fields\": [\"goog_pipelines_worker\"],         \"start_date\": \"2023-11-10\",         \"end_date\": \"2023-11-10\",         \"order_by\": {\"cost\": true}     }  16. Get total cost by sku for given wdl_task_name, order by cost DESC:      {         \"fields\": [\"wdl_task_name\"],         \"start_date\": \"2023-11-10\",         \"end_date\": \"2023-11-10\",         \"order_by\": {\"cost\": true}     }  17. Get total cost by sku for provided ID, which can be any of [ar_guid, batch_id, sequencing_group or cromwell_workflow_id], order by cost DESC:      {         \"fields\": [\"sku\", \"ar_guid\", \"batch_id\", \"sequencing_group\", \"cromwell_workflow_id\"],         \"start_date\": \"2023-11-01\",         \"end_date\": \"2023-11-30\",         \"filters\": {             \"ar_guid\": \"855a6153-033c-4398-8000-46ed74c02fe8\",             \"batch_id\": \"429518\",             \"sequencing_group\": \"cpg246751\",             \"cromwell_workflow_id\": \"cromwell-e252f430-4143-47ec-a9c0-5f7face1b296\"         },         \"filters_op\": \"OR\",         \"order_by\": {\"cost\": true}     }  18. Get weekly total cost by sku for selected cost_category, order by day ASC:      {         \"fields\": [\"sku\"],         \"start_date\": \"2022-11-01\",         \"end_date\": \"2023-12-07\",         \"filters\": {             \"cost_category\": \"Cloud Storage\"         },         \"order_by\": {\"day\": false},         \"time_periods\": \"week\"     }

### Example

```python
import metamist

from metamist import BillingApi
api_instance = BillingApi()
billing_total_cost_query_model = BillingTotalCostQueryModel(
        fields=[
            BillingColumn("id"),
        ],
        start_date="start_date_example",
        end_date="end_date_example",
        source=BillingSource("raw"),
        filters={
            "key": None,
        },
        filters_op="filters_op_example",
        group_by=True,
        order_by={
            "key": True,
        },
        limit=1,
        offset=1,
        time_column=BillingTimeColumn("day"),
        time_periods=BillingTimePeriods("day"),
        min_cost=3.14,
    ) # BillingTotalCostQueryModel | 

# Get Total Cost
api_response = api_instance.get_total_cost(billing_total_cost_query_model)
print(api_response)
```


### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **billing_total_cost_query_model** | [**BillingTotalCostQueryModel**](BillingTotalCostQueryModel.md)|  |

### Return type

[**[BillingTotalCostRecord]**](BillingTotalCostRecord.md)

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

# **get_wdl_task_names**
> [str] get_wdl_task_names()

Get Wdl Task Names

Get list of all wdl_task_names in database Results are sorted ASC

### Example

```python
import metamist

from metamist import BillingApi
api_instance = BillingApi()
# Get Wdl Task Names
api_response = api_instance.get_wdl_task_names()
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

# **is_billing_enabled**
> bool is_billing_enabled()

Is Billing Enabled

Return true if billing ie enabled, false otherwise

### Example

```python
import metamist

from metamist import BillingApi
api_instance = BillingApi()
# Is Billing Enabled
api_response = api_instance.is_billing_enabled()
print(api_response)
```


### Parameters
This endpoint does not need any parameter.

### Return type

**bool**

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json


### HTTP response details

| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | Successful Response |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

