/* eslint react-hooks/exhaustive-deps: 0 */

import * as React from "react";

import { useParams, useNavigate } from "react-router-dom";
import {
    FamilyApi,
    SampleApi,
    ParticipantApi,
    SequenceApi,
    // ParticipantModel,
    // Sample,
    // SampleSequencing,
} from "./sm-api/api";
import { Table } from "semantic-ui-react";

// TODO Move interfaces to appropriate API/routes
interface PedigreeEntry {
    affected: number;
    family_id: string;
    individual_id: string;
    maternal_id: string;
    paternal_id: string;
    sex: number;
}
interface File {
    location: string;
    basename: string;
    class: string;
    checksum: string | null;
    size: number;
    secondaryFiles?: File[];
}

interface SequenceMeta {
    reads?: File | Array<File> | Array<Array<File>>;
    [key: string]: any;
}

// temporary
interface ParticipantModel {
    id: number;
    external_id: string;
    reported_sex?: number;
    reported_gender?: string;
    karyotype?: string;
    meta: { [key: string]: any };
}

interface SampleSequencing {
    id: number;
    external_ids?: { [name: string]: string };
    sample_id: string;
    type: string;
    meta?: SequenceMeta;
    status: string;
}

enum SampleType {
    BLOOD = "blood",
    SALIVA = "saliva",
}

interface Sample {
    id: string;
    external_id: string;
    participant_id?: number;
    active?: boolean;
    meta?: { [key: string]: any };
    type?: SampleType;
    project?: number;
    author?: string;
}

const sampleFieldsToDisplay = ["active", "type", "participant_id"];

export class DetailedInfoPage extends React.Component<{}, { error?: Error }> {
    constructor(props: any) {
        super(props);
        this.state = {};
    }

    static getDerivedStateFromError(error: Error) {
        // Update state so the next render will show the fallback UI.
        return { error: error };
    }

    render() {
        if (this.state.error) {
            return <p>{this.state.error.toString()}</p>;
        }
        return <DetailedInfoPage_ />; /* eslint react/jsx-pascal-case: 0 */
    }
}

export const DetailedInfoPage_: React.FunctionComponent<{}> = () => {
    const { projectName, sampleName } = useParams();
    const navigate = useNavigate();
    const [samples, setSamples] = React.useState();
    const [isLoading, setIsLoading] = React.useState<boolean>(true);
    const [error, setError] = React.useState<string | undefined>();
    const [sample, setSample] = React.useState<string>();
    const [sampleInfo, setSampleInfo] = React.useState<Sample>();
    const [pedigree, setPedigree] = React.useState<PedigreeEntry[]>();
    const [participants, setParticipants] =
        React.useState<ParticipantModel[]>();
    const [selectedExternalID, setSelectedExternalID] =
        React.useState<string>();
    const [idNameMap, setIdNameMap] = React.useState<[string, string][]>();
    const [sequenceInfo, setSequenceInfo] =
        React.useState<SampleSequencing[]>();

    const getSamplesFromProject = React.useCallback(async () => {
        if (!projectName) return;
        new SampleApi()
            .getAllSampleIdMapByInternal(projectName)
            .then((resp) => {
                setSamples(resp.data);
            })
            .catch((er) => {
                setError(er.message);
            });
    }, [projectName]);

    const getParticipantIdToSampleId = React.useCallback(async () => {
        if (!projectName) return;
        new ParticipantApi()
            .getExternalParticipantIdToInternalSampleId(projectName)
            .then((resp) => {
                setIdNameMap(resp.data);
            })
            .catch((er) => {
                setError(er.message);
            });
    }, [projectName]);

    const getSamplesInfo = React.useCallback(async () => {
        if (!sample) return;
        if (!projectName) return;
        if (!participants) return;
        new SampleApi()
            .getSampleByExternalId(sample, projectName)
            .then((resp) => {
                setSampleInfo(resp.data);
                setSelectedExternalID(
                    participants.find(
                        (item) => item.id === resp.data.participant_id
                    )?.external_id
                );
            })
            .catch((er) => {
                setError(er.message);
            });
    }, [projectName, sample, participants]);

    const getPedigree = React.useCallback(async () => {
        if (!projectName) return;
        new FamilyApi()
            .getPedigree(projectName)
            .then((resp) => {
                setPedigree(resp.data);
            })
            .catch((er) => {
                setError(er.message);
            });
    }, [projectName]);

    const getParticipants = React.useCallback(async () => {
        if (!projectName) return;
        new ParticipantApi()
            .getParticipants(projectName)
            .then((resp) => {
                setParticipants(resp.data);
            })
            .catch((er) => {
                setError(er.message);
            });
    }, [projectName]);

    const getSequenceInfo = React.useCallback(async () => {
        if (!sampleInfo) return;
        if (!projectName) return;
        new SequenceApi()
            .getSequencesBySampleIds([sampleInfo.id.toString()])
            .then((resp) => {
                setSequenceInfo(resp.data);
            })
            .catch((er) => {
                setError(er.message);
            });
    }, [sampleInfo]);

    // retrigger if selections change
    React.useEffect(() => {
        getSamplesFromProject();
    }, [projectName]);

    React.useEffect(() => {
        getSamplesInfo();
    }, [sample, participants]);

    React.useEffect(() => {
        getPedigree();
    }, [projectName]);

    React.useEffect(() => {
        getParticipants();
    }, [projectName]);

    React.useEffect(() => {
        getParticipantIdToSampleId();
    }, [projectName]);

    React.useEffect(() => {
        getSequenceInfo();
    }, [sampleInfo]);

    const getCPGID = (id: string) => {
        const CPGID = idNameMap?.find(([name, _]) => name === id);
        return CPGID![1]; // You'd expect this to always work. Should I add the case where it doesn't?
    };

    const formatBytes = (bytes: number, decimals = 2) => {
        if (!+bytes) return "0 Bytes";

        const k = 1024;
        const dm = decimals < 0 ? 0 : decimals;
        const sizes = ["Bytes", "KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB"];

        const i = Math.floor(Math.log(bytes) / Math.log(k));

        return `${parseFloat((bytes / Math.pow(k, i)).toFixed(dm))} ${
            sizes[i]
        }`;
    };

    const prepReadMetadata = (data: SequenceMeta) => {
        if (!data.reads) return <></>;
        if (!Array.isArray(data.reads))
            return (
                <>
                    <b>Reads:</b>
                    {renderReadsMetadata([data.reads], 1)}
                </>
            );
        return (
            <>
                <b>reads:</b>
                {data.reads.map((v, i) => {
                    return renderReadsMetadata(Array.isArray(v) ? v : [v], i);
                })}
            </>
        );
    };

    const renderReadsMetadata = (data: File[], key: number | string) => {
        return (
            <Table celled key={key}>
                <Table.Body>
                    {data.map((item: File) => (
                        <Table.Row key={item.location}>
                            <Table.Cell>{item.location}</Table.Cell>
                            <Table.Cell>{formatBytes(item.size)}</Table.Cell>
                        </Table.Row>
                    ))}
                </Table.Body>
            </Table>
        );
    };

    const drawTrio = (
        paternal: string,
        affectedPaternal: number,
        maternal: string,
        affectedMaternal: number,
        proband: string,
        affectedProband: number,
        probandSex: number
    ) => {
        return (
            <svg viewBox="75 90 250 200" width="250" height="200">
                <rect
                    x={100}
                    y={100}
                    width={50}
                    height={50}
                    stroke="black"
                    cursor="pointer"
                    fill={affectedPaternal === 2 ? "black" : "white"}
                    onClick={() => {
                        if (!samples) return;
                        navigate(
                            `/project/${projectName}/sample/${getCPGID(
                                paternal
                            )}`
                        );
                    }}
                />
                <text x={125} y={170} textAnchor="middle">
                    {paternal}
                </text>
                <circle
                    cx={250}
                    cy={125}
                    r={25}
                    stroke="black"
                    cursor="pointer"
                    fill={affectedMaternal === 2 ? "black" : "white"}
                    onClick={() => {
                        if (!samples) return;
                        navigate(
                            `/project/${projectName}/sample/${getCPGID(
                                maternal
                            )}`
                        );
                    }}
                />
                <text x={250} y={170} textAnchor="middle">
                    {maternal}
                </text>
                <line x1={150} y1={125} x2={225} y2={125} stroke="black" />
                <line x1={187.5} y1={125} x2={187.5} y2={200} stroke="black" />
                {probandSex === 1 ? (
                    <rect
                        x={162.5}
                        y={200}
                        width={50}
                        height={50}
                        stroke="black"
                        fill={affectedProband === 2 ? "black" : "white"}
                    />
                ) : (
                    <circle
                        cx={187.5}
                        cy={225}
                        r={25}
                        stroke="black"
                        fill={affectedProband === 2 ? "black" : "white"}
                    />
                )}
                <text x={187.5} y={270} textAnchor="middle">
                    {proband}
                </text>
            </svg>
        );
    };

    React.useEffect(() => {
        if (!samples || !sampleName) return;
        setIsLoading(true);
        setSample(samples[sampleName]);
        setIsLoading(false);
    }, [projectName, samples, sampleName]);

    const safeValue = (value: any): string => {
        if (!value) return value;
        if (Array.isArray(value)) {
            return value.map(safeValue).join(", ");
        }
        if (typeof value === "number") {
            return value.toString();
        }
        if (typeof value === "string") {
            return value;
        }
        if (value && typeof value === "object" && !Array.isArray(value)) {
            if (!!value.location && !!value.size) {
                return `${value.location} (${formatBytes(value.size)})`;
            }
        }
        return JSON.stringify(value);
    };

    const renderSeqInfo = (seqInfo: SampleSequencing) => {
        return Object.entries(seqInfo)
            .filter(([key, value]) => key !== "id")
            .map(([key, value]) => {
                if (key === "external_ids") {
                    if (Object.keys(seqInfo.external_ids!).length) {
                        return (
                            <>
                                <b>External Ids:</b>{" "}
                                {Object.entries(seqInfo!.external_ids!)
                                    .map(([k1, v1]) => (
                                        <React.Fragment
                                            key={`${v1} (${k1})`}
                                        >{`${v1} (${k1})`}</React.Fragment>
                                    ))
                                    .join()}
                            </>
                        );
                    }
                    return <React.Fragment key="ExternalID"></React.Fragment>;
                }
                if (key === "meta") {
                    return Object.entries(seqInfo.meta!)
                        .filter(([k1, v1]) => k1 !== "reads")
                        .map(([k1, v1]) => {
                            if (
                                Array.isArray(v1) &&
                                v1.filter((v) => !!v.location && !!v.size)
                                    .length === v1.length
                            ) {
                                // all are files coincidentally
                                return (
                                    <React.Fragment key={`${k1}`}>
                                        <b>{k1}:</b>
                                        {renderReadsMetadata(v1 as File[], key)}
                                    </React.Fragment>
                                );
                            }
                            const stringifiedValue = safeValue(v1);
                            return (
                                <div key={`${k1}-${stringifiedValue}`}>
                                    <b>{k1}:</b> {stringifiedValue}
                                </div>
                            );
                        });
                }

                const stringifiedValue = safeValue(value);
                return (
                    <div key={`${key}-${stringifiedValue}`}>
                        <b>{key}:</b> {stringifiedValue}
                    </div>
                );
            });
    };

    const renderSeqSection = () => {
        if (!sample || !sequenceInfo) {
            return <></>;
        }
        return (
            <>
                <h4
                    style={{
                        borderBottom: `1px solid black`,
                    }}
                >
                    Sequence Information
                </h4>
                {sequenceInfo.map((seq) => {
                    return (
                        <React.Fragment key={seq.id}>
                            <h6>
                                <b>Sequence ID:</b> {seq.id}
                            </h6>

                            <div style={{ marginLeft: "30px" }}>
                                {renderSeqInfo(seq)}
                                {prepReadMetadata(seq.meta || {})}
                            </div>
                            <br />
                        </React.Fragment>
                    );
                })}
            </>
        );
    };

    const renderSampleSection = () => {
        if (!sample || !sampleInfo) {
            return <></>;
        }
        return (
            <>
                <h4
                    style={{
                        borderBottom: `1px solid black`,
                    }}
                >
                    Sample Information
                </h4>
                {Object.entries(sampleInfo)
                    .filter(([key, value]) =>
                        sampleFieldsToDisplay.includes(key)
                    )
                    .map(([key, value]) => (
                        <div key={`${key}-${value}`}>
                            <b>{key}:</b>{" "}
                            {value?.toString() ?? <em>no value</em>}
                        </div>
                    ))}
            </>
        );
    };

    const renderPedigreeSection = () => {
        if (!sample || !pedigree) {
            return <></>;
        }
        return (
            <>
                {pedigree
                    .filter((item) => item.individual_id === selectedExternalID)
                    .map((item) => (
                        <React.Fragment
                            key={`${item.individual_id}-${item.maternal_id}-${item.paternal_id}`}
                        >
                            {item.paternal_id &&
                                item.maternal_id &&
                                drawTrio(
                                    item.paternal_id,
                                    pedigree?.find(
                                        (i) =>
                                            i.individual_id === item.paternal_id
                                    )!.affected,
                                    item.maternal_id,
                                    pedigree?.find(
                                        (i) =>
                                            i.individual_id === item.maternal_id
                                    )!.affected,
                                    item.individual_id,
                                    item.affected,
                                    item.sex
                                )}
                        </React.Fragment>
                    ))}
            </>
        );
    };

    const renderTitle = (
        <div
            style={{
                borderBottom: `1px solid black`,
            }}
        >
            {participants &&
                participants
                    .filter((item) => item.id === sampleInfo?.participant_id)
                    .map((item) => {
                        return (
                            <React.Fragment key={item.external_id}>
                                <h1
                                    style={{
                                        display: "inline",
                                    }}
                                    key={`${item.external_id}`}
                                >
                                    {`${item.external_id}\t`}
                                </h1>
                            </React.Fragment>
                        );
                    })}
            {sampleInfo && (
                <>
                    <h3
                        style={{
                            display: "inline",
                        }}
                    >
                        {`${sampleInfo?.id}\t${sampleInfo?.external_id}`}
                    </h3>
                </>
            )}
        </div>
    );

    return (
        <>
            <br />
            {isLoading && <h1>LOADING</h1>}
            {!!error && <h1>error</h1>}
            {projectName && !isLoading && !error && (
                <>
                    <div className="detailedInfo">
                        <div>{renderTitle}</div>
                        <br />
                        <div>{renderPedigreeSection()}</div>
                        <br />
                        <div>{renderSampleSection()}</div>
                        <br />
                        <div>{renderSeqSection()}</div>
                    </div>
                </>
            )}
        </>
    );
};
