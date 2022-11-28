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
import { Table, Accordion, Popup, Button, Grid } from "semantic-ui-react";

import Diversity3RoundedIcon from "@mui/icons-material/Diversity3Rounded";
import ScienceRoundedIcon from "@mui/icons-material/ScienceRounded";
import PersonRoundedIcon from "@mui/icons-material/PersonRounded";
import BloodtypeRoundedIcon from "@mui/icons-material/BloodtypeRounded";

import { TangledTree } from "./TangledTree";
import MuckTheDuck from "./MuckTheDuck";

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

const resetLoadingState = {
    family: true,
    pedigree: true,
    participant_ids: true,
    samples: true,
    sequences: true,
};

const iconStyle = {
    fontSize: 30,
    height: "33px",
    verticalAlign: "bottom",
    marginRight: "10px",
};

export class FamilyView extends React.Component<{}, { error?: Error }> {
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
        return <FamilyView_ />; /* eslint react/jsx-pascal-case: 0 */
    }
}

export const FamilyView_: React.FunctionComponent<{}> = () => {
    const { projectName, familyID } = useParams();
    const navigate = useNavigate();

    const [loadingStates, setLoadingStates] = React.useState<{
        [key: string]: Boolean;
    }>(resetLoadingState);
    const [error, setError] = React.useState<string | undefined>();

    const [familyName, setFamilyName] = React.useState<string>();
    const [pedigree, setPedigree] = React.useState<PedigreeEntry[]>();
    const [participantIDs, setParticipantIDs] = React.useState<string[]>();
    const [sampleIDsInFamily, setSampleIDsInFamily] =
        React.useState<string[]>();
    const [sampleIDsByParticipant, setSampleIDsByParticipant] = React.useState<{
        [key: string]: string[];
    }>();
    const [samples, setSamples] = React.useState<{ [key: string]: Sample }>();
    const [sequenceInfo, setSequenceInfo] = React.useState<{
        [key: string]: SampleSequencing[];
    }>();

    const [activeIndices, setActiveIndices] = React.useState<number[]>([-1]);
    const [mostRecent, setMostRecent] = React.useState<string>();
    const [otherFamilies, setOtherFamilies] = React.useState<string[]>();

    const getFamilies = React.useCallback(async () => {
        if (!projectName || !familyID) return;
        new FamilyApi()
            .getFamilies(projectName)
            .then((resp) => {
                const entry = resp.data.find((d) => d.id === +familyID);
                if (entry) {
                    setFamilyName(entry.external_id);
                    setOtherFamilies(
                        Array.from(
                            new Set(
                                resp.data.filter(
                                    (value) => value.id !== +familyID
                                )
                            )
                        )
                    );
                } else {
                    setError("ID did not exist in project families");
                }
            })
            .then(() => {
                setLoadingStates((prev) => ({ ...prev, family: false }));
            })
            .catch((er) => {
                setError(er.message);
            });
    }, [projectName, familyID]);

    const getPedigree = React.useCallback(async () => {
        if (!projectName || !familyName) return;
        new FamilyApi()
            .getPedigree(projectName)
            .then((resp) => {
                const ped = resp.data.filter(
                    (value: PedigreeEntry) =>
                        value.family_id.toUpperCase() ===
                        familyName.toUpperCase()
                );
                setPedigree(ped);
                setParticipantIDs(
                    ped.map((item: PedigreeEntry) => item.individual_id)
                );
            })
            .then(() => {
                setLoadingStates((prev) => ({ ...prev, pedigree: false }));
            })
            .catch((er) => {
                setError(er.message);
            });
    }, [projectName, familyName]);

    const getParticipantIdToSampleId = React.useCallback(async () => {
        if (!projectName || !participantIDs) return;
        new ParticipantApi()
            .getExternalParticipantIdToInternalSampleId(projectName)
            .then((resp) => {
                setSampleIDsByParticipant(
                    resp.data.reduce(
                        (
                            obj: { [key: string]: string[] },
                            [pid, sid]: [string, string]
                        ) => {
                            if (participantIDs.includes(pid)) {
                                (obj[pid] = obj[pid] || []).push(sid);
                            }
                            return obj;
                        },
                        {}
                    )
                );
                setSampleIDsInFamily(
                    resp.data.reduce(
                        (arr: string[], [pid, sid]: [string, string]) => {
                            if (participantIDs.includes(pid)) {
                                arr.push(sid);
                            }
                            return arr;
                        },
                        []
                    )
                );
            })
            .then(() => {
                setLoadingStates((prev) => ({
                    ...prev,
                    participant_ids: false,
                }));
            })
            .catch((er) => {
                setError(er.message);
            });
    }, [projectName, participantIDs]);

    const getAllSamples = React.useCallback(async () => {
        if (!projectName || !sampleIDsInFamily) return;
        new SampleApi()
            .getSamples({
                sample_ids: sampleIDsInFamily,
                project_ids: [projectName],
            })
            .then((resp) => {
                setSamples(
                    resp.data.reduce(
                        (obj: { [key: string]: Sample }, s: Sample) => {
                            obj[s.id] = s;
                            return obj;
                        },
                        {}
                    )
                );
            })
            .then(() => {
                setLoadingStates((prev) => ({ ...prev, samples: false }));
            })
            .catch((er) => {
                setError(er.message);
            });
    }, [projectName, sampleIDsInFamily]);

    const getSequenceInfo = React.useCallback(async () => {
        if (!projectName || !sampleIDsInFamily) return;
        new SequenceApi()
            .getSequencesBySampleIds(sampleIDsInFamily)
            .then((resp) => {
                setSequenceInfo(
                    resp.data.reduce(
                        (
                            obj: { [key: string]: SampleSequencing[] },
                            s: SampleSequencing
                        ) => {
                            (obj[s.sample_id] = obj[s.sample_id] || []).push(s);
                            return obj;
                        },
                        {}
                    )
                );
            })
            .then(() => {
                setLoadingStates((prev) => ({
                    ...prev,
                    sequences: false,
                }));
            })
            .catch((er) => {
                setError(er.message);
            });
    }, [projectName, sampleIDsInFamily]);

    React.useEffect(() => {
        setLoadingStates(resetLoadingState);
    }, [projectName, familyID]);

    // retrigger if selections change
    React.useEffect(() => {
        getFamilies();
    }, [projectName, familyID, getFamilies]);

    React.useEffect(() => {
        getPedigree();
    }, [familyName, getPedigree]);

    React.useEffect(() => {
        getParticipantIdToSampleId();
    }, [participantIDs, getParticipantIdToSampleId]);

    React.useEffect(() => {
        getAllSamples();
    }, [sampleIDsInFamily, getAllSamples]);

    React.useEffect(() => {
        getSequenceInfo();
    }, [sampleIDsInFamily, getSequenceInfo]);

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
                    <b>reads:</b>
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
                            <Table.Cell collapsing>{item.location}</Table.Cell>
                            <Table.Cell collapsing>
                                {formatBytes(item.size)}
                            </Table.Cell>
                        </Table.Row>
                    ))}
                </Table.Body>
            </Table>
        );
    };

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
        return (
            <Table celled collapsing>
                <Table.Body>
                    {Object.entries(seqInfo)
                        .filter(([key, value]) => key !== "id")
                        .map(([key, value]) => {
                            if (key === "external_ids") {
                                if (Object.keys(seqInfo.external_ids!).length) {
                                    return (
                                        <Table.Row>
                                            <Table.Cell>
                                                <b>External Ids</b>
                                            </Table.Cell>
                                            <Table.Cell>
                                                {Object.entries(
                                                    seqInfo!.external_ids!
                                                )
                                                    .map(([k1, v1]) => (
                                                        <React.Fragment
                                                            key={`${v1} (${k1})`}
                                                        >{`${v1} (${k1})`}</React.Fragment>
                                                    ))
                                                    .join()}
                                            </Table.Cell>
                                        </Table.Row>
                                    );
                                }
                                return (
                                    <React.Fragment key="ExternalID"></React.Fragment>
                                );
                            }
                            if (key === "meta") {
                                return Object.entries(seqInfo.meta!)
                                    .filter(([k1, v1]) => k1 !== "reads")
                                    .map(([k1, v1]) => {
                                        if (
                                            Array.isArray(v1) &&
                                            v1.filter(
                                                (v) => !!v.location && !!v.size
                                            ).length === v1.length
                                        ) {
                                            // all are files coincidentally
                                            return (
                                                <Table.Row key={`${k1}`}>
                                                    <Table.Cell>
                                                        <b>{k1}</b>
                                                    </Table.Cell>
                                                    <Table.Cell>
                                                        {renderReadsMetadata(
                                                            v1 as File[],
                                                            key
                                                        )}
                                                    </Table.Cell>
                                                </Table.Row>
                                            );
                                        }
                                        if (
                                            v1 &&
                                            typeof v1 === "object" &&
                                            !Array.isArray(v1)
                                        ) {
                                            if (!!v1.location && !!v1.size) {
                                                return (
                                                    <Table.Row key={`${k1}`}>
                                                        <Table.Cell>
                                                            <b>{k1}:</b>
                                                        </Table.Cell>
                                                        <Table.Cell>
                                                            {renderReadsMetadata(
                                                                [v1] as File[],
                                                                k1
                                                            )}
                                                        </Table.Cell>
                                                    </Table.Row>
                                                );
                                            }
                                        }
                                        const stringifiedValue = safeValue(v1);
                                        return (
                                            <Table.Row
                                                key={`${k1}-${stringifiedValue}`}
                                            >
                                                <Table.Cell>
                                                    <b>{k1}</b>
                                                </Table.Cell>
                                                <Table.Cell>
                                                    {stringifiedValue ?? (
                                                        <em>no-value</em>
                                                    )}
                                                </Table.Cell>
                                            </Table.Row>
                                        );
                                    });
                            }

                            const stringifiedValue = safeValue(value);
                            return (
                                <Table.Row key={`${key}-${stringifiedValue}`}>
                                    <Table.Cell>
                                        <b>{key}</b>
                                    </Table.Cell>
                                    <Table.Cell>
                                        {stringifiedValue ?? <em>no-value</em>}
                                    </Table.Cell>
                                </Table.Row>
                            );
                        })}
                </Table.Body>
            </Table>
        );
    };

    const renderSeqSection = (s: SampleSequencing[]) => {
        return (
            <Accordion
                styled
                className="accordionStyle"
                panels={s.map((seq) => ({
                    key: seq.id,
                    title: {
                        content: (
                            <h3
                                style={{
                                    display: "inline",
                                }}
                            >
                                Sequence ID: {seq.id}
                            </h3>
                        ),
                        icon: <ScienceRoundedIcon sx={iconStyle} />,
                    },
                    content: {
                        content: (
                            <div style={{ marginLeft: "30px" }}>
                                {renderSeqInfo(seq)}
                                {prepReadMetadata(seq.meta || {})}
                            </div>
                        ),
                    },
                }))}
                exclusive={false}
                fluid
            />
        );
    };

    const renderSampleSection = (sample: Sample) => {
        return (
            <Table celled collapsing>
                <Table.Body>
                    {Object.entries(sample)
                        .filter(([key, value]) =>
                            sampleFieldsToDisplay.includes(key)
                        )
                        .map(([key, value]) => (
                            <Table.Row key={`${key}-${value}`}>
                                <Table.Cell>
                                    <b>{key}</b>
                                </Table.Cell>
                                <Table.Cell>
                                    {value?.toString() ?? <em>no value</em>}
                                </Table.Cell>
                            </Table.Row>
                        ))}
                </Table.Body>
            </Table>
        );
    };

    const renderTitle = (
        <div
            style={{
                borderBottom: `1px solid black`,
            }}
        >
            <h1
                style={{
                    display: "inline",
                }}
            >
                {familyName}
            </h1>
            {otherFamilies && !!otherFamilies.length && (
                <div style={{ float: "right" }}>
                    {`Change Families within ${projectName}\t`}
                    <Popup
                        trigger={
                            <Diversity3RoundedIcon
                                sx={{
                                    fontSize: 40,
                                }}
                            />
                        }
                        hoverable
                        style={{ float: "right" }}
                    >
                        <Grid divided centered rows={3}>
                            {otherFamilies.map((item) => (
                                <Grid.Row key={item.id} textAlign="center">
                                    <Button
                                        onClick={() => {
                                            setLoadingStates(resetLoadingState);
                                            setActiveIndices([-1]);
                                            navigate(
                                                `/project/${projectName}/family/${item.id}`
                                            );
                                        }}
                                    >
                                        {item.external_id}
                                    </Button>
                                </Grid.Row>
                            ))}
                        </Grid>
                    </Popup>
                </div>
            )}
        </div>
    );

    const onClick = React.useCallback(
        (e: string) => {
            if (!sampleIDsByParticipant) return;
            const indexToSet = Object.entries(sampleIDsByParticipant).findIndex(
                (i) => i[0] === e
            );
            if (!activeIndices.includes(indexToSet)) {
                setActiveIndices([...activeIndices, indexToSet]);
            }
            setMostRecent(e);
            const element = document.getElementById(e);
            if (element) {
                const y =
                    element.getBoundingClientRect().top +
                    window.pageYOffset -
                    100;
                window.scrollTo({ top: y, behavior: "smooth" });
            }
        },
        [sampleIDsByParticipant, activeIndices]
    );

    const handleTitleClick = (e, itemProps) => {
        setMostRecent("");
        const index = itemProps.index;
        if (activeIndices.indexOf(index) > -1) {
            setActiveIndices(activeIndices.filter((i) => i !== index));
        } else {
            setActiveIndices([...activeIndices, index]);
        }
    };

    const isLoading = () => {
        return Object.values(loadingStates).includes(true);
    };

    return (
        <>
            <br />
            {!!error && <h1>{error}</h1>}
            {!error && isLoading() && (
                <div
                    className="familyView"
                    style={{ textAlign: "center", paddingTop: "200px" }}
                >
                    <MuckTheDuck height={28} className="loadingScreen" />
                    <br />
                    {Object.entries(loadingStates)
                        .filter(([key, value]) => value)
                        .map(([key, value]) => (
                            <React.Fragment key={key}>
                                <h5>{` Retrieving ${key}`}</h5>
                                <br />
                            </React.Fragment>
                        ))}
                </div>
            )}
            {projectName && !isLoading() && !error && (
                <>
                    <div className="familyView" style={{ width: "100%" }}>
                        {renderTitle}
                        {pedigree && (
                            <TangledTree data={pedigree} click={onClick} />
                        )}
                        {samples && sequenceInfo && sampleIDsByParticipant && (
                            <Accordion
                                onTitleClick={handleTitleClick}
                                activeIndex={activeIndices}
                                styled
                                className="accordionStyle"
                                exclusive={false}
                                panels={Object.entries(
                                    sampleIDsByParticipant
                                ).map(([key, value]) => ({
                                    key: key,
                                    title: {
                                        content: (
                                            <h2
                                                style={{
                                                    display: "inline",
                                                }}
                                                className={
                                                    mostRecent === key
                                                        ? "selectedParticipant"
                                                        : undefined
                                                }
                                                id={key}
                                            >
                                                {key}
                                            </h2>
                                        ),
                                        icon: (
                                            <PersonRoundedIcon
                                                className={
                                                    mostRecent === key
                                                        ? "selectedParticipant"
                                                        : undefined
                                                }
                                                sx={iconStyle}
                                            />
                                        ),
                                    },
                                    content: {
                                        content: (
                                            <div
                                                style={{
                                                    marginLeft: "30px",
                                                }}
                                            >
                                                <Accordion
                                                    styled
                                                    className="accordionStyle"
                                                    panels={value.map(
                                                        (item) => {
                                                            const s =
                                                                samples[item];
                                                            return {
                                                                key: s.id,
                                                                title: {
                                                                    content: (
                                                                        <>
                                                                            <h2
                                                                                style={{
                                                                                    display:
                                                                                        "inline",
                                                                                }}
                                                                            >
                                                                                {`${s.id}\t`}
                                                                            </h2>

                                                                            <h3
                                                                                style={{
                                                                                    display:
                                                                                        "inline",
                                                                                }}
                                                                            >
                                                                                {
                                                                                    s.external_id
                                                                                }
                                                                            </h3>
                                                                        </>
                                                                    ),
                                                                    icon: (
                                                                        <BloodtypeRoundedIcon
                                                                            sx={
                                                                                iconStyle
                                                                            }
                                                                        />
                                                                    ),
                                                                },
                                                                content: {
                                                                    content: (
                                                                        <>
                                                                            <div
                                                                                style={{
                                                                                    marginLeft:
                                                                                        "30px",
                                                                                }}
                                                                            >
                                                                                {renderSampleSection(
                                                                                    s
                                                                                )}
                                                                                {renderSeqSection(
                                                                                    sequenceInfo[
                                                                                        item
                                                                                    ]
                                                                                )}
                                                                            </div>
                                                                        </>
                                                                    ),
                                                                },
                                                            };
                                                        }
                                                    )}
                                                    exclusive={false}
                                                />
                                            </div>
                                        ),
                                    },
                                }))}
                            />
                        )}
                    </div>
                </>
            )}
        </>
    );
};
