import React from 'react'
import { Header, Table as SUITable } from 'semantic-ui-react'
import LoadingDucks from '../../../shared/components/LoadingDucks/LoadingDucks'
import Table from '../../../shared/components/Table'
import formatMoney from '../../../shared/utilities/formatMoney'

const date2Month = (dt: string): string => {
    if (dt === undefined || dt === null) {
        return ''
    }

    // Convert from format like "202505" to a proper date and format it nicely
    if (dt.length === 6) {
        const year = dt.substring(0, 4)
        const month = dt.substring(4, 6)
        const date = new Date(parseInt(year), parseInt(month) - 1, 1)
        return date.toLocaleDateString('en-US', {
            year: 'numeric',
            month: 'long',
        })
    }

    // Fallback for other date formats
    const date = new Date(dt)
    return date.toLocaleDateString('en-US', {
        year: 'numeric',
        month: 'long',
    })
}
type DataDict = { [key: string]: { [key: string]: { [key: string]: number } } }

interface IBillingCostByMonthTableProps {
    start: string
    end: string
    isLoading: boolean
    data: DataDict
    months: string[]
    orderedTopics: string[]
}

const BillingCostByMonthTable: React.FunctionComponent<IBillingCostByMonthTableProps> = ({
    start,
    end,
    isLoading,
    data,
    months,
    orderedTopics,
}) => {
    if (isLoading) {
        return (
            <div>
                <LoadingDucks />
            </div>
        )
    }
    const compTypes = ['Compute Cost', 'Storage Cost']

    // Get all topics in the order they were provided
    const getAllTopics = () => {
        return orderedTopics
    }

    const dataToBody = (data: DataDict) => {
        const allTopics = getAllTopics()

        return allTopics.map((key) => (
            <>
                {compTypes.map((compType, index) => (
                    <SUITable.Row key={`${key}-${index}-row`}>
                        <SUITable.Cell key={`${key}-${index}-topic`}>
                            {index === 0 && <b>{key}</b>}
                        </SUITable.Cell>
                        <SUITable.Cell key={`${key}-${index}-compType`}>{compType}</SUITable.Cell>
                        {months.map((month) => (
                            <SUITable.Cell key={`${key}-${index}-${month}`}>
                                {data[key] && data[key][month] && data[key][month][compType]
                                    ? formatMoney(data[key][month][compType])
                                    : null}
                            </SUITable.Cell>
                        ))}
                    </SUITable.Row>
                ))}
            </>
        ))
    }

    return (
        <>
            <Header as="h3">
                SUM of Cost in AUD (excluding GST) By Topic from {date2Month(start)} to{' '}
                {date2Month(end)}
            </Header>
            <Table celled compact sortable selectable>
                <SUITable.Header>
                    <SUITable.Row>
                        <SUITable.HeaderCell></SUITable.HeaderCell>
                        <SUITable.HeaderCell></SUITable.HeaderCell>
                        <SUITable.HeaderCell colSpan={months.length}>
                            Invoice Month
                        </SUITable.HeaderCell>
                    </SUITable.Row>
                    <SUITable.Row>
                        <SUITable.HeaderCell>Topic</SUITable.HeaderCell>
                        <SUITable.HeaderCell>Compute Type</SUITable.HeaderCell>
                        {months.map((month) => (
                            <SUITable.HeaderCell key={month}>
                                {date2Month(month)}
                            </SUITable.HeaderCell>
                        ))}
                    </SUITable.Row>
                </SUITable.Header>
                <SUITable.Body>{dataToBody(data)}</SUITable.Body>
            </Table>
        </>
    )
}

export default BillingCostByMonthTable
