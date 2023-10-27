import * as React from 'react'

import { useParams } from 'react-router-dom'

import { Dropdown, Message } from 'semantic-ui-react'

import { BillingApi, BillingColumn } from '../../sm-api'

import './Billing.css'

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
                return [`All gcp_projects`, ...records]
            }
            return [`All ${fieldName.toLowerCase()}s`, ...records]
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
        if (fieldName === 'Topic') getTopics()
        else if (fieldName === 'InvoiceMonth') getInvoiceMonths()
        else if (fieldName === 'Group') {
            setRecords([BillingColumn.GcpProject, BillingColumn.Topic])
            setLoading(false)
        } else if (fieldName === 'GCP-Project') getGcpProjects()
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

    if (loading) {
        return <p>{`Loading ${fieldName}s ... `}</p>
    }

    return (
        <table>
            <tr>
                <td className="field-selector-label">
                    <h3>{label}</h3>
                </td>
                <td className="field-selector-dropdown">
                    <Dropdown
                        id="group-by-dropdown"
                        search
                        selection
                        fluid
                        onChange={onClickFunction}
                        placeholder={`Select ${fieldName}`}
                        value={selected ?? ''}
                        options={
                            records &&
                            recordsMap(records)
                        }
                    />
                </td>
            </tr>
        </table>
    )
}

export default FieldSelector
