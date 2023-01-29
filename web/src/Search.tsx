import * as React from 'react'
import { Search } from 'semantic-ui-react'
import _ from 'lodash'

import { useNavigate } from 'react-router-dom'
import Diversity3RoundedIcon from '@mui/icons-material/Diversity3Rounded'
import PersonRoundedIcon from '@mui/icons-material/PersonRounded'
import BloodtypeRoundedIcon from '@mui/icons-material/BloodtypeRounded'
import ErrorRoundedIcon from '@mui/icons-material/ErrorRounded'

import { SearchResponse, WebApi } from './sm-api/api'

type State = {
    loading: boolean
    results: SearchResponse[]
    value: string
}

enum ActionKind {
    Clear = 'CLEAN_QUERY',
    Start = 'START_SEARCH',
    Finish = 'FINISH_SEARCH',
    Update = 'UPDATE_SELECTION',
    Short = 'QUERY_TOO_SHORT',
    Error = 'ERROR',
}

type Action = {
    type: ActionKind
    query: string
    selection: string
    results: SearchResponse[]
}

const initialState: State = {
    loading: false,
    results: [],
    value: '',
}

/* eslint-disable consistent-return */
const SearchReducer = (state: State, action: Action): State => {
    switch (action.type) {
        case ActionKind.Clear:
            return initialState
        case ActionKind.Start:
            return {
                ...state,
                loading: true,
                value: action.query,
            }
        case ActionKind.Finish:
            return {
                ...state,
                loading: false,
                results: action.results,
            }
        case ActionKind.Update:
            return { ...state, value: action.selection }
        case ActionKind.Short:
            return { ...state, loading: false }
        case ActionKind.Error:
            return {
                ...state,
                loading: false,
                results: action.results,
            }
        // no default
    }
}

const resultRenderer = ({ ...props }) => {
    let components = []
    let icon: React.ReactElement = <></>
    let available: string | undefined
    let colour = 'black'
    if (!props.data.id) {
        available = `No access to this ${props.type}`
        colour = 'gray'
    }
    if (props.type === 'participant' || props.type === 'family') {
        available = `${_.capitalize(props.type)} result is not supported`
        colour = 'gray'
    }
    const style = { fontSize: 50, color: colour, height: '33px' }

    switch (props.type) {
        case 'sample': {
            components.push(...(props.data.sample_external_ids || []))
            icon = <BloodtypeRoundedIcon sx={style} />
            break
        }
        case 'participant': {
            components.push(...(props.data.participant_external_ids || []))
            icon = <PersonRoundedIcon sx={style} />
            break
        }
        case 'family': {
            components.push(...(props.data.family_external_ids || []))
            icon = <Diversity3RoundedIcon sx={style} />
            break
        }
        case 'error': {
            components.push(props.data.error)
            icon = <ErrorRoundedIcon sx={style} />
            break
        }
        // no default
    }

    components = components.filter((f) => f !== props.title)

    const subtitle = components.length > 0 ? components.join(' · ') : null

    const key = String(props.data.id || `${props.data.project}|${props.data.title}`)

    // prefer early return for empty results
    if (!props.title || !props.type) return <></>

    return (
        <div key={key} className="content">
            <div style={{ display: 'flex' }}>
                <div style={{ flex: 1, order: 1, width: '20%' }}>{icon}</div>
                <div
                    style={{
                        order: 2,
                        flex: 3,
                        display: 'inline',
                    }}
                >
                    <div className="title">{props.title}</div>
                    {subtitle && <div className="description">{subtitle}</div>}
                    {available && <div>{available}</div>}
                </div>
                <div
                    style={{
                        order: 3,
                        flex: 2,
                        textAlign: 'right',
                        fontStyle: 'italic',
                    }}
                >
                    {props.data.project}
                </div>
            </div>
        </div>
    )
}

const Searchbar: React.FunctionComponent = () => {
    const navigate = useNavigate()
    const [{ loading, results, value }, dispatch] = React.useReducer(SearchReducer, initialState)

    const searchResultToRoute = (project: string, type: string, id: string) => {
        // handle "no access to this project"
        if (!id) return

        switch (type) {
            case 'participant':
                // alert("Participants page not implemented yet");
                // navigate(`/project/${project}/participant/${id}`);
                break
            case 'sample':
                navigate(`/project/${project}/sample/${id}`)
                break
            case 'family':
                // alert("Family page not implemented yet");
                // navigate(`/project/${project}/family/${id}`);
                break
            // no default
        }
    }

    const timeoutRef = React.useRef<NodeJS.Timeout | null>(null)

    const handleSearchChange = React.useCallback((e, data) => {
        clearTimeout(timeoutRef.current as NodeJS.Timeout)
        dispatch({
            type: ActionKind.Start,
            query: data.value,
            selection: '',
            results: {},
        } as Action)

        timeoutRef.current = setTimeout(async () => {
            if (data.value.length === 0) {
                dispatch({
                    type: ActionKind.Clear,
                    query: '',
                    selection: '',
                    results: {},
                } as Action)
                return
            }
            if (data.value.length < 3) {
                dispatch({
                    type: ActionKind.Short,
                    query: '',
                    selection: '',
                    results: {},
                } as Action)
                return
            }
            new WebApi()
                .searchByKeyword(data.value)
                .then((resp) => {
                    dispatch({
                        type: ActionKind.Finish,
                        results: resp.data.responses,
                        query: '',
                        selection: '',
                    } as Action)
                })
                .catch((er) => {
                    dispatch({
                        type: ActionKind.Error,
                        results: [
                            {
                                title: 'Error',
                                type: 'error',
                                error: { error: er.message },
                            },
                        ] as SearchResponse[],
                        query: '',
                        selection: '',
                    } as Action)
                })
        }, 300)
    }, [])

    React.useEffect(
        () => () => {
            clearTimeout(timeoutRef.current as NodeJS.Timeout)
        },
        []
    )

    return (
        <Search
            showNoResults={false}
            selectFirstResult
            fluid
            input={{ fluid: true }}
            loading={loading}
            placeholder="Search..."
            onResultSelect={(e, data) => {
                dispatch({
                    type: ActionKind.Update,
                    selection: data.result.title,
                    query: '',
                    results: {},
                } as Action)
                searchResultToRoute(data.result.data.project, data.result.type, data.result.data.id)
            }}
            resultRenderer={resultRenderer}
            onSearchChange={handleSearchChange}
            results={results}
            value={value}
            style={{ marginLeft: 'auto', width: '400px' }}
        />
    )
}

export default Searchbar
