import * as React from 'react'

import { useParams } from 'react-router-dom'

import { Dropdown, Grid, Input, Message } from 'semantic-ui-react'

import { BillingApi, BillingColumn } from '../../sm-api'

import './Billing.css'

import { convertFieldName } from '../../shared/utilities/fieldName'

interface FieldSelectorProps {
    label: string
    fieldName: string
    selected?: string
    includeAll?: boolean
    onClickFunction: (_: any, { value }: any) => void
}

const FieldSelector: React.FunctionComponent<FieldSelectorProps> = ({
    label,
    fieldName,
    selected,
    includeAll,
    onClickFunction,
}) => {
    const [loading, setLoading] = React.useState<boolean>(true)
    const [error, setError] = React.useState<string | undefined>()
    const [records, setRecords] = React.useState<string[]>([])

    const extendRecords = (records: string[]) => {
        if (includeAll) {
            if (fieldName === 'GCP-Project') {
                return [`All ${convertFieldName(fieldName)}`, ...records]
            }
            return [`All ${convertFieldName(fieldName)}s`, ...records]
        }
        return records
    }

    const getTopics = () => {
        setLoading(true)
        setError(undefined)
        new BillingApi()
            .getTopics()
            .then((response) => {
                setLoading(false)
                setRecords(extendRecords(response.data))
            })
            .catch((er) => setError(er.message))
    }

    const getGcpProjects = () => {
        setLoading(true)
        setError(undefined)
        new BillingApi()
            .getGcpProjects()
            .then((response) => {
                setLoading(false)
                setRecords(extendRecords(response.data))
            })
            .catch((er) => setError(er.message))
    }

    const getInvoiceMonths = () => {
        setLoading(true)
        setError(undefined)
        new BillingApi()
            .getInvoiceMonths()
            .then((response) => {
                setLoading(false)
                setRecords(extendRecords(response.data))
            })
            .catch((er) => setError(er.message))
    }

    React.useEffect(() => {
        if (fieldName === BillingColumn.Topic) getTopics()
        else if (fieldName === BillingColumn.InvoiceMonth) getInvoiceMonths()
        else if (fieldName === 'Group') {
            setRecords([BillingColumn.GcpProject, BillingColumn.Topic])
            setLoading(false)
        } else if (fieldName === BillingColumn.GcpProject) getGcpProjects()
        else {
            setError(`Could not load records for ${fieldName}`)
        }
    }, [label, fieldName])

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
                <p>{JSON.stringify(error?.networkError || error)}</p>
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
