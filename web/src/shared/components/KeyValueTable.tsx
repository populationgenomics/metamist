import * as React from 'react'

interface IKeyValueTableProps {
    obj: { [key: string]: any }
    rightPadding?: string
    tableClass?: React.FC<{}>
}

// Define the default table like this, as it is not possible to set
// the default value to "table" in the IKeyValueTableProps interface
const DefaultTable: React.FC<
    React.DetailedHTMLProps<React.TableHTMLAttributes<HTMLTableElement>, HTMLTableElement>
> = (props) => <table {...props} />

function valueToDisplay(value: any) {
    // if string, return
    if (typeof value === 'string') {
        if (value.startsWith('https://')) {
            return (
                <a href={value} target="_blank" rel="noreferrer">
                    {value}
                </a>
            )
        }
        return value
    }
    // if react element, return
    if (React.isValidElement(value)) {
        return value
    }
    // else json.stringify
    return JSON.stringify(value)
}

/**
 * @name KeyValueTable
 * @description A table that displays key-value pairs
 * @param obj - object with key-value pairs
 * @param rightPadding - padding for the right side of the key column
 * @param tableClass - The class to use to present the table, by default, use the default html table
 */
export const KeyValueTable: React.FC<IKeyValueTableProps> = ({
    obj,
    tableClass,
    rightPadding = '20px',
}) => {
    const TableClass = tableClass || DefaultTable
    return (
        <TableClass style={{ maxWidth: '100%' }}>
            <tbody>
                {Object.entries(obj || {}).map(([key, value]) => (
                    <tr key={`key-value-table-row-${key}`}>
                        <td style={{ paddingRight: rightPadding }}>
                            <b>{key}</b>
                        </td>
                        <td style={{ wordBreak: 'break-all' }}>{valueToDisplay(value)}</td>
                    </tr>
                ))}
            </tbody>
        </TableClass>
    )
}
