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

const exampleReducer = (state: State, action: Action): State => {
    switch (action.type) {
        case ActionKind.Clear:
            return initialState;
        case ActionKind.Start:
            return { ...state, loading: true, value: action.query! };
        case ActionKind.Finish:
            return { ...state, loading: false, results: action.results! };
        case ActionKind.Update:
            return { ...state, value: action.selection! };

        default:
            throw new Error();
    }
};

const resultRenderer = ({ ...props }) => {
    return (
        <>
            {props.title && props.type && (
                <div key="content" className="content">
                    <div className="title">{props.title}</div>
                    <div className="description">{props.type}</div>
                </div>
            )}
        </>
    );
};

export const Searchbar: React.FunctionComponent = () => {
    const navigate = useNavigate();
    const [{ loading, results, value }, dispatch] = React.useReducer(
        exampleReducer,
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
            new WebApi().searchByKeyword(data.value).then((resp) => {
                dispatch({
                    type: ActionKind.Finish,
                    results: resp.data.responses,
                });
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
                    `/project/${data.result.data.project}/${data.result.type}/${data.result.title}`,
                    {
                        replace: true,
                    }
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
