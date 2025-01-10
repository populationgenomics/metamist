import Editor from '@monaco-editor/react'
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
import { debounce } from 'lodash'
import { editor, KeyCode, KeyMod } from 'monaco-editor'
import { Fragment, memo, useCallback, useContext, useEffect, useRef, useState } from 'react'
import { Link, useNavigate, useParams } from 'react-router-dom'
import { ThemeContext } from '../../shared/components/ThemeProvider'
import { TableFromQuery } from './chart/TableFromQuery'
import { useProjectDbSetup } from './data/projectDatabase'

const SIDEBAR_WIDTH = 250

// Memoized version of the result table to avoid rerenders, this is very important for performance
// without this each keystroke takes forever to render
const QueryResultTable = memo(function QueryResultTable(props: { project: string; query: string }) {
    return <TableFromQuery project={props.project} query={props.query} showToolbar />
})

// This fn is debounced to try and limit the amount of history entries.
const persistQueryToUrl = debounce(function persistQueryToUrl(query: string) {
    const url = new URL(window.location.href)
    url.searchParams.set('query', query)
    window.history.replaceState({}, '', url.toString())
}, 1000)

function getQueryFromUrl() {
    const url = new URL(window.location.href)
    return url.searchParams.get('query')
}

function getQueryId() {
    return crypto.randomUUID()
}

type QueryEntry = {
    id: string
    query: string
    name: string
}

export default function SqlQueryUi() {
    const { projectName, tableName: selectedTableName } = useParams()
    const navigate = useNavigate()
    // This is the key where the user's queries will be stored in local storage.
    // It includes the project name so that queries can be different for each project.
    const localStorageQueriesKey = `project-sql:${projectName}`
    const editorRef = useRef<editor.IStandaloneCodeEditor | null>(null)

    const theme = useContext(ThemeContext)
    const [queryStringFromUrl, setQueryStringFromUrl] = useState<string | null>(null)
    const [activeQueryId, setActiveQueryId] = useState<string | null>(null)
    const [tableQueryValue, setTableQueryValue] = useState<string>('')
    const [queries, setQueries] = useState<QueryEntry[]>([])
    const [expandedTables, setExpandedTables] = useState<string[]>([])

    const baseUrl = `/project/${projectName}/query`
    const viewType = selectedTableName ? 'table' : 'query'
    const selectedTableQuery = selectedTableName ? `SELECT * FROM ${selectedTableName}` : null

    const activeQuery = queries.find((qq) => qq.id === activeQueryId)

    function handleEditorDidMount(editor: editor.IStandaloneCodeEditor) {
        editorRef.current = editor

        // Allow running query on cmd/ctrl + enter
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

    // Handle showing/hiding the column names for tables
    function handleSidebarSectionToggle(name: string) {
        if (expandedTables.includes(name)) {
            setExpandedTables(expandedTables.filter((n) => n !== name))
        } else {
            setExpandedTables([...expandedTables, name])
        }
    }

    // handle any changes to a query, this updates both the local state, as well as
    // local storage and the URL.
    const handleQueryChange = useCallback(
        function handleQueryChange(queryId: string, queryValue: string | null) {
            let newQueries = queries

            // Allow deleting a query
            if (queryValue === null) {
                newQueries = newQueries.filter((qq) => qq.id !== queryId)
                if (activeQueryId === queryId) {
                    setActiveQueryId(null)
                }
            } else {
                newQueries = newQueries.map((qq) => {
                    if (qq.id === queryId) return { ...qq, query: queryValue }
                    return qq
                })
            }

            setQueries(newQueries)
            // Do this in a setTimeout so that it doesn't affect the render time
            setTimeout(() => {
                localStorage.setItem(localStorageQueriesKey, JSON.stringify(newQueries))
                if (queryId === activeQueryId) {
                    persistQueryToUrl(queryValue || '')
                }
            }, 0)
        },
        [queries, activeQueryId, localStorageQueriesKey]
    )

    // Handle creation of a new query
    const handleNewQuery = useCallback(
        function handleNewQuery(queryValue: string = '') {
            // Navigate to query url with query string removed
            navigate(baseUrl, { replace: true })

            const queryId = getQueryId()
            const queryName = 'Untitled'
            const newQueries = [...queries, { id: queryId, query: queryValue, name: queryName }]

            setQueries(newQueries)
            setActiveQueryId(queryId)

            // Do this in a setTimeout so that it doesn't affect the render time
            setTimeout(() => {
                localStorage.setItem(localStorageQueriesKey, JSON.stringify(newQueries))
                persistQueryToUrl(queryValue || '')
            }, 0)
        },
        [baseUrl, queries, navigate, localStorageQueriesKey]
    )

    function handleDeleteQuery(queryId: string) {
        handleQueryChange(queryId, null)
    }

    function handleRenameQuery(queryId: string, newName: string) {
        const newQueries = queries.map((qq) => {
            if (qq.id === queryId) return { ...qq, name: newName }
            return qq
        })

        // Update the state and local storage
        setQueries(newQueries)
        localStorage.setItem(localStorageQueriesKey, JSON.stringify(newQueries))
    }

    // Show results for the active query in the table
    function handleSubmitActiveQuery() {
        if (activeQuery) {
            setTableQueryValue(activeQuery.query)
        }
    }

    const handleSelectQuery = useCallback(
        function handleSelectQuery(id: string) {
            setActiveQueryId(id)
            navigate(baseUrl, { replace: true })
            persistQueryToUrl(activeQuery?.query || '')
        },
        [baseUrl, activeQuery, navigate]
    )

    const dbSetupStatus = useProjectDbSetup(projectName)

    // Only run this on load, we don't want to have the url be the source of truth for the query
    useEffect(() => {
        setQueryStringFromUrl(getQueryFromUrl())
    }, [])

    // Load queries out of local storage
    useEffect(() => {
        const storedQueries = localStorage.getItem(localStorageQueriesKey)

        if (storedQueries) {
            const parsed = JSON.parse(storedQueries)
            if (!parsed || typeof parsed !== 'object' || !Array.isArray(parsed)) return
            setQueries(parsed)
        }
    }, [localStorageQueriesKey])

    // Set the active query if it isn't already set, either from the URL or to the first query
    useEffect(() => {
        // Don't do anything if it is already set, or viewing a table
        if (activeQueryId || viewType === 'table') return

        // This is only meant to hydrate the query from the url on load, so no need to run if the
        // query has changed
        const queryChangedSinceLoad = queryStringFromUrl !== getQueryFromUrl()
        if (queryChangedSinceLoad) return

        const queryMatchingUrl = queries.find(({ query }) => query === queryStringFromUrl)

        // If there is a query in the url and it matches a saved query, set that one to active
        if (queryStringFromUrl && queryMatchingUrl) {
            setActiveQueryId(queryMatchingUrl.id)
        }
        // If there is a query in the url but it doesn't match a saved query, create new query
        else if (queryStringFromUrl) {
            handleNewQuery(queryStringFromUrl)
        }
        // Otherwise, set the first query as active
        else if (queries.length > 0) {
            handleSelectQuery(queries[0].id)
        }
    }, [activeQueryId, viewType, queryStringFromUrl, queries, handleSelectQuery, handleNewQuery])

    const sidebar = dbSetupStatus && dbSetupStatus.status === 'success' && (
        <List dense={true} disablePadding>
            <ListSubheader sx={{ lineHeight: 1, mt: 2, mb: 1 }}>
                <Typography component={'span'} fontSize={12} fontWeight={'bold'}>
                    Queries
                </Typography>
            </ListSubheader>

            {queries.map((query) => (
                <ListItem key={query.id} disablePadding>
                    <ListItemButton
                        selected={activeQueryId === query.id}
                        onClick={() => handleSelectQuery(query.id)}
                    >
                        <ListItemText>
                            <Typography component={'span'}>{query.name}</Typography>
                        </ListItemText>

                        <IconButton
                            aria-label="rename query"
                            size={'small'}
                            onClick={(e) => {
                                e.stopPropagation()
                                const newName = prompt('Rename query')
                                if (newName) handleRenameQuery(query.id, newName)
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
                                if (amSure) handleDeleteQuery(query.id)
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
                        <Button variant="outlined">New Query</Button>
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

    return (
        <Box display={'flex'}>
            <Box component="nav" sx={{ width: SIDEBAR_WIDTH, flexShrink: { sm: 0 } }}>
                <Drawer
                    variant="permanent"
                    open
                    PaperProps={{
                        sx: { width: SIDEBAR_WIDTH, top: 56, bottom: 0, height: 'auto' },
                    }}
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
                sx={{ flexGrow: 1, width: { sm: `calc(100% - ${SIDEBAR_WIDTH}px)` } }}
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
                            onChange={(value) => {
                                if (activeQueryId) {
                                    handleQueryChange(activeQueryId, value || '')
                                } else {
                                    handleNewQuery(value || '')
                                }
                            }}
                            value={activeQuery?.query || ''}
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
