/* eslint react-hooks/exhaustive-deps: 0 */

import * as React from "react";
import "./mystyle.css";

import { useParams } from "react-router-dom";
import {
    FamilyApi,
    SampleApi,
    ParticipantApi,
    SequenceApi,
} from "./sm-api/api";

// TODO Move interfaces to appropriate API/routes
interface Participant {
    external_id: string;
    id: number;
    reported_sex: number;
}

interface Sample {
    active: boolean;
    author: string;
    external_id: string;
    id: string;
    meta: {
        collection_date: string;
        specimen: string;
    };
    participant_id: string;
    project: number;
    type: string;
}

interface PedigreeEntry {
    affected: number;
    family_id: string;
    individual_id: string;
    maternal_id: string;
    paternal_id: string;
    sex: number;
}

interface Sequence {
    id: number;
    meta: {
        facility: string;
        emoji: string;
        technology: string;
        coverage: string;
    };
    sample_id: string;
    status: string;
    type: string;
}

const sampleFieldsToDisplay = ["author", "active", "type", "participant_id"];

export const DetailedInfoPage: React.FunctionComponent<{}> = () => {
    const { projectName, sampleName } = useParams();

    const [samples, setSamples] = React.useState();
    const [isLoading, setIsLoading] = React.useState<boolean>(true);
    const [error, setError] = React.useState<string | undefined>();
    const [sample, setSample] = React.useState<string>();
    const [sampleInfo, setSampleInfo] = React.useState<Sample>();
    const [pedigree, setPedigree] = React.useState<PedigreeEntry[]>();
    const [participants, setParticipants] = React.useState<Participant[]>();
    const [selectedExternalID, setSelectedExternalID] =
        React.useState<string>();
    const [idNameMap, setIdNameMap] = React.useState<[string, string][]>();
    const [sequenceInfo, setSequenceInfo] = React.useState<Sequence>();

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
                        (item) =>
                            item.id.toString() === resp.data.participant_id
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
            .getSequencesBySampleIds([sampleInfo.id])
            .then((resp) => {
                setSequenceInfo(resp.data[0]);
                setIsLoading(false);
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

    const updateSample = (id: string) => {
        const CPGID = idNameMap?.find(([name, _]) => name === id);
        if (CPGID && samples) {
            setSample(samples[CPGID[1]]);
        }
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
            <svg viewBox="75 90 250 300" width="250" height="300">
                <rect
                    x={100}
                    y={100}
                    width={50}
                    height={50}
                    stroke="black"
                    fill={affectedPaternal === 2 ? "black" : "white"}
                    onClick={() => updateSample(paternal)}
                />
                <text x={125} y={170} textAnchor="middle">
                    {paternal}
                </text>
                <circle
                    cx={250}
                    cy={125}
                    r={25}
                    stroke="black"
                    fill={affectedMaternal === 2 ? "black" : "white"}
                    onClick={() => updateSample(maternal)}
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
                        fill={affectedPaternal === 2 ? "black" : "white"}
                    />
                ) : (
                    <circle
                        cx={187.5}
                        cy={225}
                        r={25}
                        stroke="black"
                        fill={affectedProband ? "black" : "white"}
                    />
                )}
                <text x={187.5} y={270} textAnchor="middle">
                    {proband}
                </text>
            </svg>
        );
    };

    React.useEffect(() => {
        setIsLoading(true);
        setSample(sampleName);
        setIsLoading(false);
    }, [projectName, sampleName]);

    return (
        <>
            <br />
            {isLoading && <h1>LOADING</h1>}
            {!!error && <h1>error</h1>}
            {projectName && !isLoading && !error && (
                <>
                    <div
                        style={{
                            display: "grid",
                            gridTemplateAreas: `'header header header' 'pedigree sample sequence'`,
                            gridTemplateRows: `50px 1fr`,
                            gridTemplateColumns: `300px 400px 1fr`,
                            gap: 20,
                        }}
                    >
                        {sample && (
                            <>
                                <div
                                    style={{
                                        gridArea: "header",
                                        borderBottom: `1px solid black`,
                                    }}
                                >
                                    {participants &&
                                        participants
                                            .filter(
                                                (item) =>
                                                    item.id.toString() ===
                                                    sampleInfo?.participant_id
                                            )
                                            .map((item) => {
                                                return (
                                                    <React.Fragment
                                                        key={item.external_id}
                                                    >
                                                        <h1
                                                            style={{
                                                                display:
                                                                    "inline",
                                                            }}
                                                            key={`${item.external_id}`}
                                                        >
                                                            {`${item.external_id}\t`}
                                                        </h1>
                                                        <h3
                                                            style={{
                                                                display:
                                                                    "inline",
                                                            }}
                                                        >
                                                            {`${sampleInfo?.id}\t
                                                                ${sampleInfo?.external_id}`}
                                                        </h3>
                                                    </React.Fragment>
                                                );
                                            })}
                                </div>
                                <div
                                    style={{
                                        gridArea: "sample",
                                    }}
                                >
                                    {sample && (
                                        <>
                                            <h4>Sample Information</h4>
                                            <hr />
                                            {sampleInfo &&
                                                Object.entries(sampleInfo)
                                                    .filter(([key, value]) =>
                                                        sampleFieldsToDisplay.includes(
                                                            key
                                                        )
                                                    )
                                                    .map(([key, value]) => (
                                                        <div
                                                            key={`${key}-${value}`}
                                                        >
                                                            <b>{key}:</b>{" "}
                                                            {value.toString()}
                                                        </div>
                                                    ))}
                                        </>
                                    )}
                                </div>
                                <div
                                    style={{
                                        gridArea: "pedigree",
                                        borderRight: `1px solid black`,
                                    }}
                                >
                                    {sample && (
                                        <>
                                            {pedigree &&
                                                pedigree
                                                    .filter(
                                                        (item) =>
                                                            item.individual_id ===
                                                            selectedExternalID
                                                    )
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
                                                                            i.individual_id ===
                                                                            item.paternal_id
                                                                    )!.affected,
                                                                    item.maternal_id,
                                                                    pedigree?.find(
                                                                        (i) =>
                                                                            i.individual_id ===
                                                                            item.maternal_id
                                                                    )!.affected,
                                                                    item.individual_id,
                                                                    item.affected,
                                                                    item.sex
                                                                )}
                                                        </React.Fragment>
                                                    ))}
                                        </>
                                    )}
                                </div>
                                <div
                                    style={{
                                        gridArea: "sequence",
                                        borderLeft: `1px solid black`,
                                        paddingLeft: 20,
                                    }}
                                >
                                    {sample && (
                                        <>
                                            <h4>Sequence Information</h4>
                                            <hr />
                                            {sequenceInfo &&
                                                Object.entries(
                                                    sequenceInfo
                                                ).map(([key, value]) => {
                                                    if (key === "meta") {
                                                        return Object.entries(
                                                            sequenceInfo.meta
                                                        ).map(([k1, v1]) => (
                                                            <div
                                                                key={`${k1}-${v1}`}
                                                            >
                                                                <b>{k1}:</b>{" "}
                                                                {v1}
                                                            </div>
                                                        ));
                                                    }
                                                    return (
                                                        <div
                                                            key={`${key}-${value}`}
                                                        >
                                                            <b>{key}:</b>{" "}
                                                            {value}
                                                        </div>
                                                    );
                                                })}
                                        </>
                                    )}
                                </div>
                            </>
                        )}
                    </div>
                </>
            )}
        </>
    );
};
