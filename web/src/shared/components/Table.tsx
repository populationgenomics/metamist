// Table is a wrapper around semantic-ui-react's Table component that adds a class
// to the table if the user has dark mode enabled in their browser.

import { Table as SUITable, TableProps } from 'semantic-ui-react'
import { useMediaQuery } from 'react-responsive'

const Table: React.FunctionComponent<TableProps> = ({ className, ...props }) => {
    const isDarkMode = useMediaQuery({ query: '(prefers-color-scheme: dark)' })
    let classNames = className || ''
    if (isDarkMode) {
        classNames += ' inverted'
    }
    return <SUITable className={classNames} {...props} />
}

export default Table
