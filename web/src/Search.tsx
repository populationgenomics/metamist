import * as React from "react";
import { Search } from "semantic-ui-react";
import { WebApi } from "./sm-api/api";
import { useNavigate } from "react-router-dom";
import Diversity3RoundedIcon from "@mui/icons-material/Diversity3Rounded";
import PersonRoundedIcon from "@mui/icons-material/PersonRounded";
import BloodtypeRoundedIcon from "@mui/icons-material/BloodtypeRounded";
import ErrorRoundedIcon from "@mui/icons-material/ErrorRounded";

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
    Error = "ERROR",
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
        case "ERROR":
            return { ...state, loading: false, results: action.results! };
    }
};

const resultRenderer = ({ ...props }) => {
    let components = [];
    // let icon = <Diversity3RoundedIcon />;
    let icon: React.ReactElement = <></>;

    switch (props.type) {
        case "sample": {
            if (props.data.id !== props.title) {
                components.push(props.data.id);
            }
            components.push(...(props.data.sample_external_ids || []));
            icon = <BloodtypeRoundedIcon />;
            break;
        }
        case "participant": {
            components.push(...(props.data.participant_external_ids || []));
            icon = <PersonRoundedIcon />;
            break;
        }
        case "family": {
            components.push(...(props.data.family_external_ids || []));
            icon = <Diversity3RoundedIcon />;
            break;
        }
        case "error": {
            components.push(props.data.error);
            icon = <ErrorRoundedIcon />;
            break;
        }
    }

    let subtitle = components.join(" · ");

    return (
        <>
            {props.title && props.type && (
                <div key="content" className="content">
                    <div className="title">
                        {icon}
                        {"  "}
                        {props.title}
                        <span
                            style={{
                                float: "right",
                                fontWeight: "normal",
                                fontStyle: "italic",
                            }}
                        >
                            {props.data.project}
                        </span>
                    </div>
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

    const searchResultToRoute = (project: string, type: string, id: string) => {
        switch (type) {
            case "participant":
                alert("Participants page not implemented yet");
                // navigate(`/project/${project}/participant/${id}`);
                break;
            case "sample":
                navigate(`/project/${project}/sample/${id}`);
                break;
            case "family":
                alert("Family page not implemented yet");
                // navigate(`/project/${project}/family/${id}`);
                break;
        }
    };

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
                    dispatch({
                        type: ActionKind.Error,
                        results: [
                            {
                                title: "Error",
                                type: "error",
                                data: { error: er.message },
                            },
                        ],
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
                searchResultToRoute(
                    data.result.data.project,
                    data.result.type,
                    data.result.data.id
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
