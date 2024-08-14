import * as React from 'react'

interface IKeyValueTableProps {
    obj: { [key: string]: any }
    rightPadding?: string
    tableClass?: React.FC<{}>
}

const DefaultTable: React.FC<
    React.DetailedHTMLProps<React.TableHTMLAttributes<HTMLTableElement>, HTMLTableElement>
> = (props) => <table {...props} />

export const KeyValueTable: React.FC<IKeyValueTableProps> = ({
    obj,
    tableClass,
    rightPadding = '20px',
}) => {
    const TableClass = tableClass || DefaultTable
    return (
        <TableClass>
            <tbody>
                {Object.entries(obj || {}).map(([key, value]) => (
                    <tr key={`sample-meta-${key}`}>
                        <td style={{ paddingRight: rightPadding }}>
                            <b>{key}</b>
                        </td>
                        <td>{typeof value === 'string' ? value : JSON.stringify(value)}</td>
                    </tr>
                ))}
            </tbody>
        </TableClass>
    )
}
