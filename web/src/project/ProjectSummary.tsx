/* eslint react-hooks/exhaustive-deps: 0 */

import * as React from "react";
import { useParams, useNavigate, useSearchParams } from "react-router-dom";
import _ from "lodash";

import { ProjectSelector } from "./ProjectSelector";
import { WebApi, ProjectSummaryResponse } from "../sm-api/api";

import { Table, Button, Dropdown } from "semantic-ui-react";

import { SampleLink } from "../Links";

const PAGE_SIZES = [20, 40, 100, 1000];

const REPORT_PREFIX = "https://main-web.populationgenomics.org.au/";

const REPORT_TYPES = {
    "WGS Cram MultiQC": "/qc/cram/multiqc.html",
    "WGS GVCF MultiQC": "/qc/gvcf/multiqc.html",
    "WGS FASTQC MultiQC": "qc/fastqc/multiqc.html",
    "Exome Cram MultiQC": "exome/qc/cram/multiqc.html",
    "Exome GVCF MultiQC": "exome/qc/gvcf/multiqc.html",
    "Exome FASTQC MultiQC": "exome/qc/fastqc/multiqc.html",
};

const sanitiseValue = (value: any) => {
    const tvalue = typeof value;
    if (tvalue === "string" || tvalue === "number") return value;
    if (tvalue === "boolean") return value ? "true" : "false";
    if (value === undefined || value === null) return "";
    return JSON.stringify(value);
};

export const ProjectSummary = () => {
    const navigate = useNavigate();

    const { projectName, page } = useParams();

    const [searchParams] = useSearchParams();
    const pageSize = searchParams.get("size") || 20;
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
            navigate(`/project/${projectName}/${p}?size=${pageLimit}`);
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
        [projectName, pageLimit, pageNumber]
    );

    const setPageLimit = React.useCallback(
        (e: React.SyntheticEvent<HTMLElement>, { value }) => {
            navigate(`/project/${projectName}/1?size=${parseInt(value)}`);
            _setPageLimit(parseInt(value));
            setPageNumber(1);
        },
        [projectName, pageLimit, page]
    );

    const _updateProjectSummary = React.useCallback(() => {
        getProjectSummary(directToken);
    }, [getProjectSummary, directToken]);

    // retrigger if project changes, or pageLimit changes
    React.useEffect(_updateProjectSummary, [
        projectName,
        pageLimit,
        pageNumber,
    ]);

    const totalPageNumbers = Math.ceil(
        (summary?.total_samples || 0) / pageLimit
    );

    const pageOptions = (
        <>
            {pageNumber > 1 && (
                <Button
                    style={{ marginLeft: "10px" }}
                    disabled={isLoading}
                    onClick={() => {
                        handleOnClick(pageNumber - 1);
                    }}
                >
                    Previous
                </Button>
            )}
            {!!summary?.total_samples && (
                <span style={{ padding: "8px 10px 0 10px" }}>
                    Page {pageNumber} / {totalPageNumbers} (
                    {summary?.total_samples} samples)
                </span>
            )}
            {pageNumber < totalPageNumbers && (
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

    const renderGrid = () => {
        if (error) {
            return (
                <p>
                    <em>An error occurred when fetching samples: {error}</em>
                </p>
            );
        }
        if (!summary) {
            if (projectName) {
                return <p>Loading...</p>;
            } else {
                return (
                    <p>
                        <em>Please select a project</em>
                    </p>
                );
            }
        }
        if (summary.participants.length === 0) {
            return (
                <p>
                    <em>No samples</em>
                </p>
            );
        }
        const headers = [
            "Family ID",
            ...summary.participant_keys.map((field) => field[1]),
            ...summary.sample_keys.map((field) => field[1]),
            ...summary.sequence_keys.map((field) => "sequence." + field[1]),
        ];

        return (
            <Table celled>
                <Table.Header>
                    <Table.Row>
                        {headers.map((k, i) => (
                            <Table.HeaderCell key={`${k}-${i}`}>
                                {k}
                            </Table.HeaderCell>
                        ))}
                    </Table.Row>
                </Table.Header>
                <Table.Body>
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
                                    <Table.Row
                                        key={`${p.external_id}-${s.id}-${seq.id}`}
                                    >
                                        {isFirstOfGroup && (
                                            <Table.Cell
                                                style={{ backgroundColor }}
                                                rowSpan={lengthOfParticipant}
                                            >
                                                {p.families
                                                    .map((f) => f.external_id)
                                                    .join(", ")}
                                            </Table.Cell>
                                        )}
                                        {isFirstOfGroup &&
                                            summary.participant_keys.map(
                                                ([k, dn]) => (
                                                    <Table.Cell
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
                                                    </Table.Cell>
                                                )
                                            )}
                                        {seqidx === 0 &&
                                            summary.sample_keys.map(
                                                ([k, dn]) => (
                                                    <Table.Cell
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
                                                        k === "id" ? (
                                                            <SampleLink
                                                                id={s.id}
                                                                projectName={
                                                                    projectName
                                                                }
                                                            >
                                                                {sanitiseValue(
                                                                    _.get(s, k)
                                                                )}
                                                            </SampleLink>
                                                        ) : (
                                                            sanitiseValue(
                                                                _.get(s, k)
                                                            )
                                                        )}
                                                    </Table.Cell>
                                                )
                                            )}
                                        {seq &&
                                            summary.sequence_keys.map(
                                                ([k, dn]) => (
                                                    <Table.Cell
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
                                                    </Table.Cell>
                                                )
                                            )}
                                    </Table.Row>
                                );
                            });
                        })
                    )}
                </Table.Body>
            </Table>
        );
    };

    const titleCase = (s: string) => {
        return s[0].toUpperCase() + s.slice(1).toLowerCase();
    };

    const batchTable = () => {
        if (
            !summary?.cram_seqr_stats ||
            _.isEmpty(summary.cram_seqr_stats) ||
            !summary?.batch_sequence_stats ||
            _.isEmpty(summary.batch_sequence_stats)
        ) {
            return <></>;
        }

        const seqTypes = Object.keys(summary?.cram_seqr_stats);
        return (
            <Table celled compact>
                <Table.Header>
                    <Table.Row>
                        <Table.HeaderCell>Batch</Table.HeaderCell>
                        {seqTypes.map((item) => (
                            <Table.HeaderCell
                                key={`header-${item}-${projectName}`}
                            >
                                {titleCase(item)}
                            </Table.HeaderCell>
                        ))}
                        <Table.HeaderCell>Total</Table.HeaderCell>
                    </Table.Row>
                </Table.Header>

                <Table.Body>
                    {Object.entries(summary?.batch_sequence_stats)
                        .sort((a, b) => {
                            if (a[0] === b[0]) {
                                return 0;
                            }
                            if (a[0] === "no-batch") {
                                return 1;
                            }
                            if (b[0] === "no-batch") {
                                return -1;
                            }
                            return a[0] > b[0] ? 1 : -1;
                        })
                        .map(([key, value]) => (
                            <Table.Row key={`body-${key}-${projectName}`}>
                                <Table.Cell>{titleCase(key)}</Table.Cell>
                                {seqTypes.map((seq) => (
                                    <Table.Cell
                                        key={`${key}-${seq}`}
                                    >{`${value[seq]}`}</Table.Cell>
                                ))}
                                <Table.Cell>
                                    {Object.values(value).reduce(
                                        (a, b) => +a + +b,
                                        0
                                    )}
                                </Table.Cell>
                            </Table.Row>
                        ))}
                </Table.Body>
            </Table>
        );
    };

    const totalsStats: React.ReactElement = (
        <>
            <h2>Project Summary Stats</h2>
            <b>Total Participants: </b>
            {summary?.total_participants}
            <br />
            <b>Total Samples: </b>
            {"    "}
            {summary?.total_samples}
            <br />
            <b>Total Sequences: </b>
            {"    "}
            {summary?.total_sequences}
            <br />
        </>
    );

    const seqStats = () => {
        if (!summary?.cram_seqr_stats || _.isEmpty(summary?.cram_seqr_stats)) {
            return <></>;
        }
        return (
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
                    {Object.entries(summary?.cram_seqr_stats).map(
                        ([key, value]) => (
                            <React.Fragment key={`${key}-${projectName}`}>
                                <Table.Row>
                                    <Table.Cell>{titleCase(key)}</Table.Cell>
                                    {Object.entries(value).map(([k1, v1]) => (
                                        <Table.Cell
                                            key={`${key}-${k1}-${projectName}`}
                                        >
                                            {`${v1}`}
                                        </Table.Cell>
                                    ))}
                                </Table.Row>
                            </React.Fragment>
                        )
                    )}
                </Table.Body>
            </Table>
        );
    };

    const multiQCReports = () => {
        return Object.entries(REPORT_TYPES).map(([key, value]) => (
            <a
                href={`${REPORT_PREFIX}${projectName}${value}`}
                className="ui button"
                key={key}
            >
                {key}
            </a>
        ));
    };

    return (
        <>
            <br />
            <ProjectSelector
                setPageLimit={_setPageLimit}
                setPageNumber={setPageNumber}
                pageLimit={PAGE_SIZES[0]}
            />
            <br />
            <hr />
            {totalsStats}
            {seqStats()}
            {batchTable()}
            <hr />
            {multiQCReports()}
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
                    <Dropdown
                        selection
                        onChange={setPageLimit}
                        value={pageLimit}
                        options={PAGE_SIZES.map((s) => ({
                            key: s,
                            text: `${s} samples`,
                            value: s,
                        }))}
                    />
                    {pageOptions}
                </div>
            )}
            {renderGrid()}
            {pageOptions}
        </>
    );
};
