// Table is a wrapper around semantic-ui-react's Table component that adds a class
// to the table if the user has dark mode enabled in their browser.
import * as React from 'react'

import { Table as SUITable, TableProps } from 'semantic-ui-react'
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

export default Table
