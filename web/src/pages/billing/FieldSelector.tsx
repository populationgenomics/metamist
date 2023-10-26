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

    React.useEffect(() => {
        if (fieldName === 'Topic') getTopics()
        else if (fieldName === 'Group') {
            setRecords(['GCP-Project', 'Topic'])
            setLoading(false)
        } else if (fieldName === 'GCP-Project') getGcpProjects()
    }, [label, fieldName])

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
                            records.map((p) => ({
                                key: p,
                                text: p,
                                value: p,
                            }))
                        }
                    />
                </td>
            </tr>
        </table>
    )
}

export default FieldSelector
