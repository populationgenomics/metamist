// Table is a wrapper around semantic-ui-react's Table component that adds a class
// to the table if the user has dark mode enabled in their browser.
import * as React from 'react'

import { range } from 'lodash'

import { Table as SUITable, TableProps, Checkbox } from 'semantic-ui-react'

import { ThemeContext } from './ThemeProvider'

const Table: React.FunctionComponent<TableProps> = ({ className, ...props }) => {
    const theme = React.useContext(ThemeContext)

    const isDarkMode = theme.theme === 'dark-mode'

    let classNames = className || ''
    if (isDarkMode) {
        classNames += ' inverted'
    }
    return <SUITable className={classNames} {...props} />
}

interface ICheckboxRowProps {
    isChecked: boolean
    setIsChecked: (isChecked: boolean) => void
    isVisible?: boolean
    colSpan?: number
    rowStyle?: React.CSSProperties
    leadingCells?: number
}

export const CheckboxRow: React.FC<ICheckboxRowProps> = ({
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
interface IDisplayRowProps {
    isVisible?: boolean
    label?: string
    colSpan?: number
}

export const DisplayRow: React.FC<IDisplayRowProps> = ({
    children,
    label,
    colSpan,
    isVisible = true,
    ...props
}) => {
    const _isVisible = isVisible === undefined ? true : isVisible

    return (
        <SUITable.Row
            style={{
                display: _isVisible ? 'table-row' : 'none',
                backgroundColor: 'var(--color-bg)',
                color: 'var(--color-text-primary)',
            }}
            {...props}
        >
            <SUITable.Cell />
            <SUITable.Cell style={{ width: 30 }}>
                <b>{label}</b>
            </SUITable.Cell>
            <SUITable.Cell colSpan={colSpan}>{children}</SUITable.Cell>
        </SUITable.Row>
    )
}

export default Table
