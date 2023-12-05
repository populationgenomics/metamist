import * as React from 'react'
import { Dropdown, Input, Message } from 'semantic-ui-react'
import { BillingApi, BillingColumn, BillingTimePeriods } from '../../../sm-api'
import { convertFieldName } from '../../../shared/utilities/fieldName'

interface FieldSelectorProps {
    label: string
    fieldName: string
    selected?: string
    includeAll?: boolean
    autoSelect?: boolean
    onClickFunction: (_: any, { value }: any) => void
}

const FieldSelector: React.FunctionComponent<FieldSelectorProps> = ({
    label,
    fieldName,
    selected,
    includeAll,
    autoSelect,
    onClickFunction,
}) => {
    const [loading, setLoading] = React.useState<boolean>(true)
    const [error, setError] = React.useState<string | undefined>()
    const [records, setRecords] = React.useState<string[]>([])

    const extendRecords = (recs: string[]) => {
        if (includeAll) {
            return [`All ${convertFieldName(fieldName)}s`, ...recs]
        }
        return recs
    }

    const processResponse = (response_data: string[]) => {
        setLoading(false)
        const extRecords = extendRecords(response_data)
        setRecords(extRecords)
        if (!selected && autoSelect) {
            // set the first option as the default
            onClickFunction(undefined, { value: extRecords[0] })
        }
    }

    const getTopics = () => {
        setLoading(true)
        setError(undefined)
        new BillingApi()
            .getTopics()
            .then((response) => {
                processResponse(response.data)
            })
            .catch((er) => setError(er.message))
    }

    const getGcpProjects = () => {
        setLoading(true)
        setError(undefined)
        new BillingApi()
            .getGcpProjects()
            .then((response) => {
                processResponse(response.data)
            })
            .catch((er) => setError(er.message))
    }

    const getInvoiceMonths = () => {
        setLoading(true)
        setError(undefined)
        new BillingApi()
            .getInvoiceMonths()
            .then((response) => {
                processResponse(response.data)
            })
            .catch((er) => setError(er.message))
    }

    const getStages = () => {
        setLoading(true)
        setError(undefined)
        new BillingApi()
            .getStages()
            .then((response) => {
                processResponse(response.data)
            })
            .catch((er) => setError(er.message))
    }

    const getCostCategories = () => {
        setLoading(true)
        setError(undefined)
        new BillingApi()
            .getCostCategories()
            .then((response) => {
                processResponse(response.data)
            })
            .catch((er) => setError(er.message))
    }

    React.useEffect(() => {
        if (fieldName === BillingColumn.Topic) getTopics()
        else if (fieldName === BillingColumn.GcpProject) getGcpProjects()
        else if (fieldName === BillingColumn.InvoiceMonth) getInvoiceMonths()
        else if (fieldName === BillingColumn.Stage) getStages()
        else if (fieldName === BillingColumn.CostCategory) getCostCategories()
        else if (fieldName === 'Group') {
            setRecords([BillingColumn.GcpProject, BillingColumn.Topic, BillingColumn.Stage])
            setLoading(false)
        } else if (fieldName === 'Period') {
            setRecords([
                BillingTimePeriods.Day,
                BillingTimePeriods.Week,
                BillingTimePeriods.Month,
                BillingTimePeriods.InvoiceMonth,
            ])
            setLoading(false)
        } else {
            setError(`Could not load records for ${fieldName}`)
        }
    }, [fieldName])

    const capitalize = (str: string): string => {
        if (str === 'gcp_project') {
            return 'GCP-Project'
        }
        return str.charAt(0).toUpperCase() + str.slice(1)
    }

    const recordsMap = (records: any[]) => {
        if (fieldName === 'Group') {
            return records.map((p: BillingColumn) => ({
                key: p,
                text: capitalize(p),
                value: p,
            }))
        }
        return records.map((p: string) => ({
            key: p,
            text: p,
            value: p,
        }))
    }

    if (error) {
        return (
            <Message negative>
                <h4>An error occurred while getting projects</h4>
                <p>{JSON.stringify(error)}</p>
            </Message>
        )
    }

    return (
        <Input
            label={label}
            fluid
            input={
                <Dropdown
                    id="group-by-dropdown"
                    loading={loading}
                    search
                    selection
                    fluid
                    onChange={onClickFunction}
                    placeholder={`Select ${convertFieldName(fieldName)}`}
                    value={selected ?? ''}
                    options={records && recordsMap(records)}
                    style={{
                        borderRadius: '0 4px 4px 0',
                    }}
                />
            }
        />
    )
}

export default FieldSelector
