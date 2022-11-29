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
    Family,
} from "../sm-api/api";
import {
    Table,
    Accordion,
    Popup,
    Button,
    Grid,
    AccordionTitleProps,
} from "semantic-ui-react";

import Diversity3RoundedIcon from "@mui/icons-material/Diversity3Rounded";
import ScienceRoundedIcon from "@mui/icons-material/ScienceRounded";
import PersonRoundedIcon from "@mui/icons-material/PersonRounded";
import BloodtypeRoundedIcon from "@mui/icons-material/BloodtypeRounded";

import { TangledTree } from "../renders/TangledTree";
import MuckTheDuck from "../renders/MuckTheDuck";
import { SeqInfo } from "../renders/SeqInfo";

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
    const [otherFamilies, setOtherFamilies] = React.useState<Family[]>();

    const getFamilies = React.useCallback(async () => {
        console.log("In getFamilies");
        if (!projectName || !familyID) return;
        console.log("continued getFamilies");
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
                    setError(
                        `${familyID} ID does not exist in ${projectName} families`
                    );
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
        console.log("In getPedigree");
        console.log(projectName, familyName);
        if (!projectName || !familyName) return;
        console.log("continued getPedigree");
        new FamilyApi()
            .getPedigree(projectName)
            .then((resp) => {
                const ped = resp.data.filter(
                    (value: PedigreeEntry) =>
                        value.family_id.toUpperCase() ===
                        familyName.toUpperCase()
                );
                console.log(ped);
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
    }, [familyName]);

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
    }, [participantIDs]);

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
    }, [sampleIDsInFamily]);

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
    }, [sampleIDsInFamily]);

    React.useEffect(() => {
        setLoadingStates(resetLoadingState);
        console.log("Updated Loading States");
    }, [projectName, familyID]);

    // retrigger if selections change
    React.useEffect(() => {
        getFamilies();
        console.log("Updated Families");
    }, [getFamilies]);

    React.useEffect(() => {
        getPedigree();
        console.log("Updated Pedigree");
    }, [getPedigree]);

    React.useEffect(() => {
        getParticipantIdToSampleId();
        console.log("Updated PID");
    }, [getParticipantIdToSampleId]);

    React.useEffect(() => {
        getAllSamples();
        console.log("Updated Samples");
    }, [getAllSamples]);

    React.useEffect(() => {
        getSequenceInfo();
        console.log("Updated Sequences");
    }, [getSequenceInfo]);

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
                                <SeqInfo data={seq} />
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

    const handleTitleClick = (
        e: React.MouseEvent,
        itemProps: AccordionTitleProps
    ) => {
        setMostRecent("");
        const index = itemProps.index ?? -1;
        if (index === -1) return;
        if (activeIndices.indexOf(+index) > -1) {
            setActiveIndices(activeIndices.filter((i) => i !== index));
        } else {
            setActiveIndices([...activeIndices, +index]);
        }
    };

    const isLoading = () => {
        return Object.values(loadingStates).includes(true);
    };

    return (
        <div className="familyView" style={{ width: "100%" }}>
            <br />
            {!!error && <h1>{error}</h1>}
            {!error && isLoading() && (
                <div style={{ textAlign: "center", paddingTop: "200px" }}>
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
                            panels={Object.entries(sampleIDsByParticipant).map(
                                ([key, value]) => ({
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
                                })
                            )}
                        />
                    )}
                </>
            )}
        </div>
    );
};
