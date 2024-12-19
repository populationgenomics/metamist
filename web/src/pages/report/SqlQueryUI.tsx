import Editor from '@monaco-editor/react'
import { editor, KeyCode, KeyMod } from 'monaco-editor'

import DeleteIcon from '@mui/icons-material/Delete'
import EditIcon from '@mui/icons-material/Edit'
import ErrorIcon from '@mui/icons-material/Error'
import ExpandLess from '@mui/icons-material/ExpandLess'
import ExpandMore from '@mui/icons-material/ExpandMore'
import EyeCon from '@mui/icons-material/Visibility'
import {
    Alert,
    Box,
    Button,
    Chip,
    CircularProgress,
    Collapse,
    Divider,
    Drawer,
    IconButton,
    List,
    ListItem,
    ListItemButton,
    ListItemText,
    ListSubheader,
    Typography,
} from '@mui/material'
import { Fragment, memo, useContext, useEffect, useRef, useState } from 'react'
import { Link, useNavigate, useParams } from 'react-router-dom'
import { ThemeContext } from '../../shared/components/ThemeProvider'
import { TableFromQuery } from './chart/TableFromQuery'
import { useProjectDbSetup } from './data/projectDatabase'

// Memoized version of the result table to avoid rerenders
const QueryResultTable = memo(function QueryResultTable(props: { project: string; query: string }) {
    return <TableFromQuery project={props.project} query={props.query} showToolbar />
})

function persistQueryToUrl(query: string) {
    const url = new URL(window.location.href)
    url.searchParams.set('query', query)
    window.history.replaceState({}, '', url.toString())
}

function getQueryFromUrl() {
    const url = new URL(window.location.href)
    return url.searchParams.get('query')
}

export default function ProjectDatabaseQuery() {
    const { projectName, tableName: selectedTableName } = useParams()
    const navigate = useNavigate()
    const localStorageQueriesKey = `project-db-queries-${projectName}`
    const editorRef = useRef<editor.IStandaloneCodeEditor | null>(null)

    const theme = useContext(ThemeContext)
    const [queryStringFromUrl, setqueryStringFromUrl] = useState<string | null>(null)
    const [activeQuery, setActiveQuery] = useState<string | null>(null)
    const [tableQueryValue, setTableQueryValue] = useState<string>('')
    const [queries, setQueries] = useState<Record<string, string | null>>({})
    const [expandedTables, setExpandedTables] = useState<string[]>([])

    const baseUrl = `/project/${projectName}/query`
    const viewType = selectedTableName ? 'table' : 'query'
    const selectedTableQuery = selectedTableName ? `SELECT * FROM ${selectedTableName}` : null

    function handleEditorDidMount(editor: editor.IStandaloneCodeEditor) {
        editorRef.current = editor

        // Allow running query one cmd/ctrl + enter
        editor.addAction({
            id: 'executeCurrentAndAdvance',
            label: 'Execute Block and Advance',
            keybindings: [KeyMod.CtrlCmd | KeyCode.Enter],
            contextMenuGroupId: '2_execution',
            run: () => {
                // Can't call handleSubmitActiveQuery here as it will refer to an old version
                // of the function, because this only gets called on editor mount. So get the
                // value from the editor and set the table query value directly
                const query = editor.getValue()
                setTableQueryValue(query)
            },
        })
    }

    function handleSidebarSectionToggle(name: string) {
        if (expandedTables.includes(name)) {
            setExpandedTables(expandedTables.filter((n) => n !== name))
        } else {
            setExpandedTables([...expandedTables, name])
        }
    }

    function handleQueryChange(queryName: string | null, queryValue: string | null) {
        const newQueries = { ...queries }
        let resolvedQueryName = queryName
        if (!resolvedQueryName) {
            resolvedQueryName = handleNewQuery()
        }

        // Allow deleting a query
        if (queryValue === null && resolvedQueryName in newQueries) {
            delete newQueries[resolvedQueryName]
            if (activeQuery === resolvedQueryName) {
                setActiveQuery(null)
            }
        } else {
            newQueries[resolvedQueryName] = queryValue
        }

        setQueries(newQueries)
        // Do this in a setTimeout so that it doesn't affect the render time
        setTimeout(() => {
            localStorage.setItem(localStorageQueriesKey, JSON.stringify(newQueries))
            if (resolvedQueryName === activeQuery) {
                persistQueryToUrl(queryValue || '')
            }
        }, 0)
    }

    function handleNewQuery(queryValue: string = '') {
        navigate(baseUrl, { replace: true })
        // Find the highest number of untitled queries so that name
        // won't collide with other names
        const untitledNumbers = Object.keys(queries).map((q) => {
            const match = q.match(/Untitled (\d+)$/)
            return match ? parseInt(match[1]) : 0
        })
        const maxUntitled = untitledNumbers.length > 0 ? Math.max(...untitledNumbers) : 0
        const queryName = `Untitled ${maxUntitled + 1}`

        setActiveQuery(queryName)
        handleQueryChange(queryName, queryValue)
        return queryName
    }

    function handleDeleteQuery(name: string) {
        handleQueryChange(name, null)
    }

    function handleRenameQuery(oldName: string, newName: string) {
        if (oldName === newName) return

        let resolvedNewName = newName
        const newQueries = { ...queries }
        const oldQuery = newQueries[oldName]
        const conflictingQuery = newQueries[newName]

        // Handle case where query has been renamed to an existing query, in this case add an
        // incrementing number to the end of the query name
        if (conflictingQuery && conflictingQuery !== oldQuery) {
            const otherNumbers = Object.keys(newQueries)
                .filter((q) => q.startsWith(newName))
                .map((q) => {
                    const suffix = q.slice(newName.length + 1)
                    const match = suffix.match(/^(\d+)$/)
                    return match ? parseInt(match[1]) : 0
                })
            const maxNumber = otherNumbers.length > 0 ? Math.max(...otherNumbers) : 0
            resolvedNewName = `${newName} ${maxNumber + 1}`
        }
        // if this was the active query, be sure to update the active query name
        if (oldName === activeQuery) setActiveQuery(resolvedNewName)
        // Remove the old query and add the new one
        delete newQueries[oldName]
        newQueries[resolvedNewName] = oldQuery
        // Update the state and local storage
        setQueries(newQueries)
        localStorage.setItem(localStorageQueriesKey, JSON.stringify(newQueries))
    }

    function handleSubmitActiveQuery() {
        const currentQuery = activeQuery ? queries[activeQuery] : null
        if (currentQuery) {
            setTableQueryValue(currentQuery)
        }
    }

    function handleSelectQuery(name: string) {
        setActiveQuery(name)
        navigate(baseUrl, { replace: true })
        const queryValue = queries[name] || ''
        persistQueryToUrl(queryValue)
    }

    const dbSetupStatus = useProjectDbSetup(projectName)

    // Only run this on load, we don't want to have the url be the source of truth for the query
    useEffect(() => {
        setqueryStringFromUrl(getQueryFromUrl())
    }, [])

    // Load queries out of local storage
    useEffect(() => {
        const storedQueries = localStorage.getItem(localStorageQueriesKey)

        if (storedQueries) {
            const parsed = JSON.parse(storedQueries)
            if (!parsed || typeof parsed !== 'object' || Array.isArray(parsed)) return
            const keys = Object.keys(parsed)
            if (keys.length === 0) return
            setQueries(parsed)
        }
    }, [localStorageQueriesKey])

    // Set the active query if it isn't already set
    useEffect(() => {
        // Don't do anything if it is already set, or viewing a table
        if (activeQuery || viewType === 'table') return

        // This is only meant to hydrate the query from the url on load, so no need to run if the
        // query has changed
        const queryChangedSinceLoad = queryStringFromUrl !== getQueryFromUrl()
        if (queryChangedSinceLoad) return

        const queryKeys = Object.keys(queries)
        const queryMatchingUrl = Object.entries(queries).find(
            ([_, query]) => query === queryStringFromUrl
        )?.[0]

        // If there is a query in the url and it matches a saved query, set that one to active
        if (queryStringFromUrl && queryMatchingUrl) {
            setActiveQuery(queryMatchingUrl)
        }
        // If there is a query in the url but it doesn't match a saved query, create new query
        else if (queryStringFromUrl) {
            handleNewQuery(queryStringFromUrl)
        }
        // Otherwise, set the first query as active
        else if (queryKeys.length > 0) {
            handleSelectQuery(queryKeys[0])
        }
    })

    const sidebar = dbSetupStatus && dbSetupStatus.status === 'success' && (
        <List dense={true} disablePadding>
            <ListSubheader sx={{ lineHeight: 1, mt: 2, mb: 1 }}>
                <Typography component={'span'} fontSize={12} fontWeight={'bold'}>
                    Queries
                </Typography>
            </ListSubheader>

            {Object.keys(queries).map((name) => (
                <ListItem key={name} disablePadding>
                    <ListItemButton
                        selected={activeQuery === name}
                        onClick={() => handleSelectQuery(name)}
                    >
                        <ListItemText>
                            <Typography component={'span'}>{name}</Typography>
                        </ListItemText>

                        <IconButton
                            aria-label="rename query"
                            size={'small'}
                            onClick={(e) => {
                                e.stopPropagation()
                                const newName = prompt('Rename query')
                                if (newName) handleRenameQuery(name, newName)
                            }}
                        >
                            <EditIcon />
                        </IconButton>

                        <IconButton
                            aria-label="delete query"
                            size={'small'}
                            onClick={(e) => {
                                e.stopPropagation()
                                const amSure = confirm(
                                    'Are you sure you want to delete this query?'
                                )
                                if (amSure) handleDeleteQuery(name)
                            }}
                        >
                            <DeleteIcon />
                        </IconButton>
                    </ListItemButton>
                </ListItem>
            ))}

            <ListItem disablePadding>
                <ListItemButton onClick={() => handleNewQuery()}>
                    <ListItemText>
                        <Typography component={'span'}>New Query</Typography>
                    </ListItemText>
                </ListItemButton>
            </ListItem>

            <Divider />

            <ListSubheader sx={{ lineHeight: 1, mt: 2, mb: 1 }}>
                <Typography component={'span'} fontSize={12} fontWeight={'bold'}>
                    Tables
                </Typography>
            </ListSubheader>

            {dbSetupStatus.tableSetupStatus.map((table) => (
                <Fragment key={table.name}>
                    <ListItem disablePadding>
                        <ListItemButton
                            onClick={() => handleSidebarSectionToggle(table.name)}
                            selected={selectedTableName === table.name}
                        >
                            <ListItemText>
                                <Typography component={'span'}>{table.name}</Typography>
                            </ListItemText>
                            {table.status === 'error' && <ErrorIcon color="error" />}
                            {table.status === 'success' && (
                                <Link
                                    to={`/project/${projectName}/query/${table.name}`}
                                    onClick={(e) => {
                                        e.stopPropagation()
                                    }}
                                >
                                    <IconButton aria-label="view table" size={'small'}>
                                        <EyeCon />
                                    </IconButton>
                                </Link>
                            )}
                            {expandedTables.includes(table.name) ? <ExpandLess /> : <ExpandMore />}
                        </ListItemButton>
                    </ListItem>

                    <Collapse in={expandedTables.includes(table.name)} timeout={150} unmountOnExit>
                        {table.status === 'success' && (
                            <List component="div" disablePadding dense={true}>
                                {table.tableInfo.columns.map((column) => (
                                    <ListItemText
                                        key={column.columnName}
                                        sx={{ pl: 4, py: 0.2, whiteSpace: 'nowrap' }}
                                    >
                                        <Typography component={'span'} noWrap>
                                            {column.columnName}
                                        </Typography>
                                        <Chip
                                            label={column.columnType}
                                            size="small"
                                            sx={{ fontSize: 10, p: 0, ml: 1 }}
                                        />
                                    </ListItemText>
                                ))}
                            </List>
                        )}
                        {table.status === 'error' && (
                            <Alert severity="error">{table.errorMessage}</Alert>
                        )}
                    </Collapse>
                </Fragment>
            ))}
        </List>
    )

    const sidebarWidth = 250

    return (
        <Box display={'flex'}>
            <Box component="nav" sx={{ width: sidebarWidth, flexShrink: { sm: 0 } }}>
                <Drawer
                    variant="permanent"
                    open
                    PaperProps={{ sx: { width: sidebarWidth, top: 56, bottom: 0, height: 'auto' } }}
                >
                    {dbSetupStatus && dbSetupStatus.status === 'loading' && (
                        <Box p={2}>
                            <CircularProgress />
                        </Box>
                    )}
                    {dbSetupStatus && dbSetupStatus.status === 'error' && (
                        <Box p={2}>
                            <Alert severity="error">{dbSetupStatus.errorMessage}</Alert>
                        </Box>
                    )}
                    {sidebar}
                </Drawer>
            </Box>

            <Box
                component="main"
                sx={{ flexGrow: 1, width: { sm: `calc(100% - ${sidebarWidth}px)` } }}
            >
                <Box display={'flex'} flexDirection={'column'} height={'calc(100vh - 80px)'}>
                    <Box
                        height={'20%'}
                        position={'relative'}
                        display={viewType === 'query' ? 'block' : 'none'}
                    >
                        <Editor
                            theme={theme.theme === 'dark-mode' ? 'vs-dark' : 'vs-light'}
                            language="sql"
                            onMount={handleEditorDidMount}
                            onChange={(value) => handleQueryChange(activeQuery, value || '')}
                            value={activeQuery ? queries[activeQuery] || '' : ''}
                            options={{
                                padding: { top: 10 },
                                minimap: { enabled: false },
                                automaticLayout: true,
                                scrollBeyondLastLine: false,
                            }}
                        />

                        <Box sx={{ position: 'absolute', bottom: 15, right: 25 }}>
                            <Button
                                variant="contained"
                                color="primary"
                                onClick={() => handleSubmitActiveQuery()}
                            >
                                <Typography mr={1} component={'span'}>
                                    Run Query
                                </Typography>
                                <Typography fontSize={10} component={'span'}>
                                    ({navigator.userAgent.includes('Mac') ? 'Cmd' : 'Ctrl'} + Enter)
                                </Typography>
                            </Button>
                        </Box>
                    </Box>
                    {viewType === 'query' && <Divider />}

                    <Box display={'flex'} flex={1} flexDirection={'column'} p={2} height={'80%'}>
                        {projectName && (selectedTableQuery || tableQueryValue) && (
                            <QueryResultTable
                                project={projectName}
                                query={selectedTableQuery || tableQueryValue}
                            />
                        )}
                    </Box>
                </Box>
            </Box>
        </Box>
    )
}
