import { Header, Table as SUITable } from 'semantic-ui-react'
import Table from '../../../shared/components/Table'
import React from 'react'
import formatMoney from '../../../shared/utilities/formatMoney'
import LoadingDucks from '../../../shared/components/LoadingDucks/LoadingDucks'

const date2Month = (dt: string): string => {
    if (dt === undefined || dt === null) {
        return ''
    }
    const date = new Date(dt)
    return `${date.getFullYear()}${(date.getMonth() + 1).toString().padStart(2, '0')}`
}

interface IBillingCostByMonthTableProps {
    start: string
    end: string
    isLoading: boolean
    data: any
    months: string[]
}

const BillingCostByMonthTable: React.FC<IBillingCostByMonthTableProps> = ({
    start,
    end,
    isLoading,
    data,
    months,
}) => {
    if (isLoading) {
        return (
            <div>
                <LoadingDucks />
            </div>
        )
    }
    const compTypes = ['Compute Cost', 'Storage Cost']

    const dataToBody = (data: any) => {
        const sortedKeys = Object.keys(data).sort()
        return sortedKeys.map((key) => (
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
                SUM of Cost (AUD) By Topic from {start} to {end}
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
