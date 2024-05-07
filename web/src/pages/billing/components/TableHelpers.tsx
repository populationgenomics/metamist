import { range } from 'lodash'
import { Table as SUITable, Card, Checkbox } from 'semantic-ui-react'

import {
    AnalysisCostRecord,
    AnalysisCostRecordTotal,
    AnalysisCostRecordBatch,
    AnalysisCostRecordBatchJob,
    Analysis,
    AnalysisCostRecordSku,
    AnalysisCostRecordSeqGroup,
} from '../../../sm-api'

interface ICheckboxRowProps {
    isChecked: boolean
    setIsChecked: (isChecked: boolean) => void
    isVisible?: boolean
    colSpan?: number
    rowStyle?: React.CSSProperties
    leadingCells?: number
}

const CheckboxRow: React.FC<ICheckboxRowProps> = ({
    children,
    setIsChecked,
    isChecked,
    isVisible,
    colSpan,
    rowStyle,
    leadingCells,
    ...props
}) => (
    <SUITable.Row
        style={{
            display: (isVisible === undefined ? true : isVisible) ? 'table-row' : 'none',
            backgroundColor: 'var(--color-bg)',
            ...(rowStyle || {}),
        }}
        {...props}
    >
        {leadingCells &&
            range(0, leadingCells).map((cell, idx) => (
                <SUITable.Cell key={`leading-cell-${idx}`} />
            ))}
        {/* <SUITable.Cell style={{ border: 'none' }} /> */}
        <SUITable.Cell style={{ width: 50 }}>
            <Checkbox
                checked={isChecked}
                toggle
                onChange={(e) => {
                    // debugger
                    setIsChecked(!isChecked)
                }}
            />
        </SUITable.Cell>
        <SUITable.Cell colSpan={colSpan}>{children}</SUITable.Cell>
    </SUITable.Row>
)

const DisplayRow: React.FC<{ isVisible?: boolean; label: string }> = ({
    children,
    label,
    isVisible = true,
    ...props
}) => {
    const _isVisible = isVisible === undefined ? true : isVisible

    return (
        <SUITable.Row
            style={{
                display: _isVisible ? 'table-row' : 'none',
                backgroundColor: 'var(--color-bg)',
            }}
            {...props}
        >
            <SUITable.Cell />
            <SUITable.Cell style={{ width: 250 }}>
                <b>{label}</b>
            </SUITable.Cell>
            <SUITable.Cell>{children}</SUITable.Cell>
        </SUITable.Row>
    )
}
