/* eslint react-hooks/exhaustive-deps: 0 */

import * as React from "react";
import { useParams, useNavigate, Link } from "react-router-dom";
import _ from "lodash";

import { ProjectSelector } from "./ProjectSelector";
import { WebApi, ProjectSummaryResponse } from "../sm-api/api";

import { Input, Button } from "reactstrap";

import { Icon, Label, Menu, Table } from "semantic-ui-react";

const PAGE_SIZES = [20, 40, 100, 1000];

const sanitiseValue = (value: any) => {
    const tvalue = typeof value;
    if (tvalue === "string" || tvalue === "number") return value;
    if (tvalue === "boolean") return value ? "true" : "false";
    if (value === undefined || value === null) return "";
    return JSON.stringify(value);
};

export const ProjectSummary = () => {
    const navigate = useNavigate();

    const { projectName, page, pageSize } = useParams();
    let directToken = 0;
    if (page && pageSize) {
        directToken = +page * +pageSize - +pageSize;
    }

    const validPages = !!(
        page &&
        +page &&
        pageSize &&
        +pageSize &&
        PAGE_SIZES.includes(+pageSize)
    );

    const [summary, setSummary] = React.useState<
        ProjectSummaryResponse | undefined
    >();
    const [pageNumber, setPageNumber] = React.useState<number>(
        validPages ? +page : 1
    );
    const [isLoading, setIsLoading] = React.useState<boolean>(false);
    const [error, setError] = React.useState<string | undefined>();
    const [pageLimit, _setPageLimit] = React.useState<number>(
        validPages ? +pageSize : PAGE_SIZES[0]
    );

    const handleOnClick = React.useCallback(
        (p) => {
            navigate(`/project/${projectName}/${p}/${pageLimit}`);
            setPageNumber(p);
        },
        [navigate]
    );

    const getProjectSummary = React.useCallback(
        async (token: any) => {
            if (!projectName) return;
            let sanitisedToken = !!token ? token : undefined;
            setError(undefined);
            setIsLoading(true);
            new WebApi()
                .getProjectSummary(projectName, pageLimit, sanitisedToken)
                .then((resp) => {
                    setIsLoading(false);
                    setSummary(resp.data);
                })
                .catch((er) => {
                    setIsLoading(false);
                    setError(er.message);
                });
        },
        [projectName, pageLimit, page]
    );

    const setPageLimit = React.useCallback(
        (e: React.ChangeEvent<HTMLInputElement>) => {
            const value = e.currentTarget.value;
            navigate(`/project/${projectName}/1/${parseInt(value)}`);
            _setPageLimit(parseInt(value));
            setPageNumber(1);
        },
        [projectName, pageLimit, page]
    );

    const _updateProjectSummary = React.useCallback(() => {
        getProjectSummary(directToken);
    }, [getProjectSummary, directToken]);

    // retrigger if project changes, or pageLimit changes
    React.useEffect(_updateProjectSummary, [projectName, pageLimit, page]);

    let table: React.ReactElement = <></>;

    const totalPageNumbers = Math.ceil(
        (summary?.total_samples || 0) / pageLimit
    );

    const renderLink = (s: any, k: any, external_id: string) => (
        <Link to={`/project/${projectName}/sample/${external_id}`}>
            {sanitiseValue(_.get(s, k))}
        </Link>
    );

    const pageOptions = (
        <>
            {pageNumber > 1 && (
                <Button
                    disabled={isLoading}
                    onClick={() => {
                        handleOnClick(pageNumber - 1);
                    }}
                >
                    Previous
                </Button>
            )}
            {!!summary?.total_samples && (
                <span style={{ padding: "0 10px" }}>
                    Page {pageNumber} / {totalPageNumbers} (
                    {summary?.total_samples} samples)
                </span>
            )}
            {summary?._links?.token && (
                <Button
                    disabled={isLoading}
                    onClick={() => {
                        handleOnClick(pageNumber + 1);
                    }}
                >
                    Next
                </Button>
            )}
        </>
    );
    // }

    if (error) {
        table = (
            <p>
                <em>An error occurred when fetching samples: {error}</em>
            </p>
        );
    } else if (!summary) {
        if (projectName) {
            table = <p>Loading...</p>;
        } else {
            table = (
                <p>
                    <em>Please select a project</em>
                </p>
            );
        }
    } else {
        if (summary.participants.length === 0) {
            table = (
                <p>
                    <em>No samples</em>
                </p>
            );
        } else {
            const headers = [
                "Family ID",
                ...summary.participant_keys.map((pk) =>
                    pk.replace(".meta", "")
                ),
                ...summary.sample_keys.map((sk) => sk.replace(".meta", "")),
                ...summary.sequence_keys.map(
                    (sk) => "sequence." + sk.replace(".meta", "")
                ),
            ];

            table = (
                <table className="table table-bordered">
                    <thead>
                        <tr>
                            {headers.map((k, i) => (
                                <th key={`${k}-${i}`}>{k}</th>
                            ))}
                        </tr>
                    </thead>
                    <tbody>
                        {summary.participants.map((p, pidx) =>
                            p.samples.map((s, sidx) => {
                                // @ts-ignore
                                const backgroundColor =
                                    pidx % 2 === 0
                                        ? "white"
                                        : "var(--bs-table-striped-bg)";
                                const lengthOfParticipant = p.samples
                                    .map((s) => s.sequences.length)
                                    .reduce((a, b) => a + b, 0);
                                return s.sequences.map((seq, seqidx) => {
                                    const isFirstOfGroup =
                                        sidx === 0 && seqidx === 0;
                                    return (
                                        <tr
                                            key={`${p.external_id}-${s.id}-${seq.id}`}
                                        >
                                            {isFirstOfGroup && (
                                                <td
                                                    style={{ backgroundColor }}
                                                    rowSpan={
                                                        lengthOfParticipant
                                                    }
                                                >
                                                    {p.families
                                                        .map(
                                                            (f) => f.external_id
                                                        )
                                                        .join(", ")}
                                                </td>
                                            )}
                                            {isFirstOfGroup &&
                                                summary.participant_keys.map(
                                                    (k) => (
                                                        <td
                                                            style={{
                                                                backgroundColor,
                                                            }}
                                                            key={
                                                                p.id +
                                                                "participant." +
                                                                k
                                                            }
                                                            rowSpan={
                                                                lengthOfParticipant
                                                            }
                                                        >
                                                            {sanitiseValue(
                                                                _.get(p, k)
                                                            )}
                                                        </td>
                                                    )
                                                )}
                                            {seqidx === 0 &&
                                                summary.sample_keys.map((k) => (
                                                    <td
                                                        style={{
                                                            backgroundColor,
                                                        }}
                                                        key={
                                                            s.id + "sample." + k
                                                        }
                                                        rowSpan={
                                                            s.sequences.length
                                                        }
                                                    >
                                                        {k === "external_id" ||
                                                        k === "id"
                                                            ? renderLink(
                                                                  s,
                                                                  k,
                                                                  s.id
                                                              )
                                                            : sanitiseValue(
                                                                  _.get(s, k)
                                                              )}
                                                    </td>
                                                ))}
                                            {seq &&
                                                summary.sequence_keys.map(
                                                    (k) => (
                                                        <td
                                                            style={{
                                                                backgroundColor,
                                                            }}
                                                            key={
                                                                s.id +
                                                                "sequence." +
                                                                k
                                                            }
                                                        >
                                                            {sanitiseValue(
                                                                _.get(seq, k)
                                                            )}
                                                        </td>
                                                    )
                                                )}
                                        </tr>
                                    );
                                });
                            })
                        )}
                    </tbody>
                </table>
            );
        }
    }

    const titleCase = (s: string) => {
        return s[0].toUpperCase() + s.slice(1).toLowerCase();
    };

    const projectSummaryStats: React.ReactElement = (
        <>
            <h5>Project Summary Stats</h5>
            Total Participants: {summary?.total_participants}
            <br />
            Total Samples: {summary?.total_samples}
            <br />
            Sequence Statistics:
            <br />
            <Table celled>
                <Table.Header>
                    <Table.Row>
                        <Table.HeaderCell>Type</Table.HeaderCell>
                        <Table.HeaderCell>Sequences</Table.HeaderCell>
                        <Table.HeaderCell>CRAMs</Table.HeaderCell>
                        <Table.HeaderCell>Seqr</Table.HeaderCell>
                    </Table.Row>
                </Table.Header>

                <Table.Body>
                    {summary?.sequence_stats &&
                        Object.entries(summary?.sequence_stats).map(
                            ([key, value], index) => (
                                <Table.Row>
                                    <Table.Cell>{titleCase(key)}</Table.Cell>
                                    {Object.entries(value).map(([k1, v1]) => (
                                        <Table.Cell key={`${key}-${k1}`}>
                                            {`${v1}`}
                                        </Table.Cell>
                                    ))}
                                </Table.Row>
                            )
                        )}
                </Table.Body>
            </Table>
        </>
    );

    return (
        <>
            <br />
            <ProjectSelector />
            <br />
            {summary && projectSummaryStats}
            <hr />
            {projectName && (
                <div
                    style={{
                        marginBottom: "10px",
                        justifyContent: "flex-end",
                        display: "flex",
                        flexDirection: "row",
                    }}
                >
                    <Input
                        type="select"
                        onChange={setPageLimit}
                        value={pageLimit}
                        style={{
                            display: "inline",
                            width: "150px",
                            marginRight: "10px",
                        }}
                    >
                        {PAGE_SIZES.map((s) => (
                            <option key={`page-size-${s}`} value={s}>
                                {s} samples
                            </option>
                        ))}
                    </Input>
                    {pageOptions}
                </div>
            )}
            {table}
            {pageOptions}
        </>
    );
};
