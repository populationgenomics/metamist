import * as React from "react";
import { Search } from "semantic-ui-react";
import { WebApi } from "./sm-api/api";
import { useNavigate } from "react-router-dom";

type State = {
    loading: boolean;
    results: {}[];
    value: string;
};

enum ActionKind {
    Clear = "CLEAN_QUERY",
    Start = "START_SEARCH",
    Finish = "FINISH_SEARCH",
    Update = "UPDATE_SELECTION",
    Short = "QUERY_TOO_SHORT",
}

type Action = {
    type: ActionKind;
    query?: string;
    selection?: string;
    results?: {}[];
};

const initialState: State = {
    loading: false,
    results: [],
    value: "",
};

const assertUnreachable = (x: ActionKind): never => {
    throw new Error(`Unexpected value: ${x}`);
};

const SearchReducer = (state: State, action: Action): State => {
    switch (action.type) {
        case "CLEAN_QUERY":
            return initialState;
        case "START_SEARCH":
            return { ...state, loading: true, value: action.query! };
        case "FINISH_SEARCH":
            return { ...state, loading: false, results: action.results! };
        case "UPDATE_SELECTION":
            return { ...state, value: action.selection! };
        case "QUERY_TOO_SHORT":
            return { ...state, loading: false };
    }
    return assertUnreachable(action.type);
};

const resultRenderer = ({ ...props }) => {
    let components = [];

    switch (props.type) {
        case "sample": {
            if (props.data.id !== props.title) {
                components.push(props.data.id);
            }
            components.push(...(props.data.sample_external_ids || []));
            break;
        }
        case "participant": {
            components.push(...(props.data.participant_external_ids || []));
            break;
        }
        case "family": {
            components.push(...(props.data.family_external_ids || []));
            break;
        }
    }

    let subtitle = props.type + ": " + components.join(" · ");

    return (
        <>
            {props.title && props.type && (
                <div key="content" className="content">
                    <div className="title">{props.title}</div>
                    <div className="description">{subtitle}</div>
                </div>
            )}
        </>
    );
};

export const Searchbar: React.FunctionComponent = () => {
    const navigate = useNavigate();
    const [{ loading, results, value }, dispatch] = React.useReducer(
        SearchReducer,
        initialState
    );

    const timeoutRef = React.useRef<NodeJS.Timeout | null>(null);

    const handleSearchChange = React.useCallback((e, data) => {
        clearTimeout(timeoutRef.current as NodeJS.Timeout);
        dispatch({ type: ActionKind.Start, query: data.value });

        timeoutRef.current = setTimeout(async () => {
            if (data.value.length === 0) {
                dispatch({ type: ActionKind.Clear });
                return;
            }
            if (data.value.length < 3) {
                dispatch({ type: ActionKind.Short });
                return;
            }
            new WebApi()
                .searchByKeyword(data.value)
                .then((resp) => {
                    dispatch({
                        type: ActionKind.Finish,
                        results: resp.data.responses,
                    });
                })
                .catch((er) => {
                    console.log(er.message);
                });
        }, 300);
    }, []);

    React.useEffect(() => {
        return () => {
            clearTimeout(timeoutRef.current as NodeJS.Timeout);
        };
    }, []);

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
                });
                navigate(
                    `/project/${data.result.data.project}/${data.result.type}/${data.result.title}`
                );
            }}
            resultRenderer={resultRenderer}
            onSearchChange={handleSearchChange}
            results={results}
            value={value}
            style={{ marginLeft: "auto", width: "400px" }}
        />
    );
};
