import React, { useState } from 'react'
import { Table, Icon, Button, Input } from 'semantic-ui-react'
import { sortBy } from 'lodash'

import { SequencingGroup } from './types'

type Direction = 'ascending' | 'descending' | undefined

interface ISequencingGroupTableProps {
    editable?: boolean
    height?: number
    sequencingGroups?: SequencingGroup[]
    emptyMessage?: string
    onDelete?: (id: string) => void
}

const SequencingGroupTable: React.FC<ISequencingGroupTableProps> = ({
    editable = true,
    height = 650,
    sequencingGroups = [],
    emptyMessage = 'Nothing to display',
    onDelete = () => {},
}) => {
    const [sortColumn, setSortColumn] = useState('id')
    const [sortDirection, setSortDirection] = useState<Direction>('ascending')
    const [searchTerms, setSearchTerms] = useState<{ column: string; term: string }[]>([])

    const setSortInformation = (column: string) => {
        setSortColumn(column)
        setSortDirection(sortDirection === 'ascending' ? 'descending' : 'ascending')
    }

    const setSearchInformation = (column: string, term: string) => {
        if (searchTerms.find((st) => st.column === column)) {
            setSearchTerms(
                searchTerms.map((st) => (st.column === column ? { column, term: term.trim() } : st))
            )
        } else {
            setSearchTerms([...searchTerms, { column, term: term.trim() }])
        }
    }

    const filteredRows = sequencingGroups.filter((sg: SequencingGroup) => {
        if (!searchTerms.length) return true

        return searchTerms.every(({ column, term }) => {
            if (!term) return true

            // eslint-disable-next-line @typescript-eslint/ban-ts-comment
            // @ts-ignore
            const value: any = sg[column]
            return value.toString().toLowerCase().includes(term.toLowerCase())
        })
    })

    let sortedRows = sortBy(filteredRows, [sortColumn])
    sortedRows = sortDirection === 'ascending' ? sortedRows : sortedRows.reverse()

    const tableColumns = [
        { key: 'id', name: 'ID' },
        { key: 'project', name: 'Project' },
        { key: 'type', name: 'Type' },
        { key: 'technology', name: 'Technology' },
        { key: 'platform', name: 'Platform' },
    ]

    const renderTableBody = () => {
        if (sequencingGroups.length && !sortedRows.length) {
            return (
                <Table.Row>
                    <Table.Cell colSpan={5}>No rows matching your filters</Table.Cell>
                </Table.Row>
            )
        }

        if (!sequencingGroups.length) {
            return (
                <Table.Row>
                    <Table.Cell colSpan={5}>{emptyMessage}</Table.Cell>
                </Table.Row>
            )
        }

        return sortedRows.map((sg: SequencingGroup) => (
            <Table.Row key={sg.id}>
                {editable && sortedRows.length ? (
                    <Table.Cell>
                        <Button icon onClick={() => onDelete(sg.id)}>
                            <Icon name="delete" color="red" />
                        </Button>
                    </Table.Cell>
                ) : null}
                <Table.Cell>{sg.id}</Table.Cell>
                <Table.Cell>{sg.project.name}</Table.Cell>
                <Table.Cell>{sg.type}</Table.Cell>
                <Table.Cell>{sg.technology}</Table.Cell>
                <Table.Cell>{sg.platform}</Table.Cell>
            </Table.Row>
        ))
    }

    return (
        <div style={{ maxHeight: height, overflowY: 'scroll' }}>
            <Table sortable celled selectable>
                <Table.Header
                    inverted
                    style={{
                        backgroundColor: 'rgba(255,255,255,1)',
                        position: 'sticky',
                        top: 0,
                        zIndex: 1,
                    }}
                >
                    <Table.Row>
                        {editable && sortedRows.length ? <Table.HeaderCell /> : null}
                        {tableColumns.map((column) => (
                            <Table.HeaderCell
                                key={column.key}
                                sorted={sortColumn === column.key ? sortDirection : undefined}
                            >
                                <div onClick={() => setSortInformation(column.key)}>
                                    {column.name}
                                </div>
                                <Input
                                    size="mini"
                                    onKeyUp={(e: any) =>
                                        setSearchInformation(column.key, e.target.value)
                                    }
                                    placeholder="search..."
                                />
                            </Table.HeaderCell>
                        ))}
                    </Table.Row>
                </Table.Header>
                <Table.Body>{renderTableBody()}</Table.Body>
            </Table>
        </div>
    )
}

export default SequencingGroupTable
