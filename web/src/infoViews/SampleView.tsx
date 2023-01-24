import * as React from 'react'

import { useParams, useNavigate } from 'react-router-dom'
import {
    FamilyApi,
    SampleApi,
    ParticipantApi,
    SequenceApi,
    // ParticipantModel,
    // Sample,
    // SampleSequencing,
} from '../sm-api/api'
import { SeqInfo } from '../renders/SeqInfo'

// TODO Move interfaces to appropriate API/routes
interface PedigreeEntry {
    affected: number
    family_id: string
    individual_id: string
    maternal_id: string
    paternal_id: string
    sex: number
}
interface File {
    location: string
    basename: string
    class: string
    checksum: string | null
    size: number
    secondaryFiles?: File[]
}

interface SequenceMeta {
    reads?: File | Array<File> | Array<Array<File>>
    [key: string]: any
}

// temporary
interface ParticipantModel {
    id: number
    external_id: string
    reported_sex?: number
    reported_gender?: string
    karyotype?: string
    meta: { [key: string]: any }
}

interface SampleSequencing {
    id: number
    external_ids?: { [name: string]: string }
    sample_id: string
    type: string
    meta?: SequenceMeta
    status: string
}

enum SampleType {
    BLOOD = 'blood',
    SALIVA = 'saliva',
}

interface Sample {
    id: string
    external_id: string
    participant_id?: number
    active?: boolean
    meta?: { [key: string]: any }
    type?: SampleType
    project?: number
    author?: string
}

const sampleFieldsToDisplay = ['active', 'type', 'participant_id']

const SampleView_: React.FunctionComponent<Record<string, unknown>> = () => {
    const { projectName, sampleName } = useParams()
    const navigate = useNavigate()
    const [samples, setSamples] = React.useState()
    const [isLoading, setIsLoading] = React.useState<boolean>(true)
    const [error, setError] = React.useState<string | undefined>()
    const [sample, setSample] = React.useState<string>()
    const [sampleInfo, setSampleInfo] = React.useState<Sample>()
    const [pedigree, setPedigree] = React.useState<PedigreeEntry[]>()
    const [participants, setParticipants] = React.useState<ParticipantModel[]>()
    const [selectedExternalID, setSelectedExternalID] = React.useState<string>()
    const [idNameMap, setIdNameMap] = React.useState<[string, string][]>()
    const [sequenceInfo, setSequenceInfo] = React.useState<SampleSequencing[]>()

    const getSamplesFromProject = React.useCallback(async () => {
        if (!projectName) return
        new SampleApi()
            .getAllSampleIdMapByInternal(projectName)
            .then((resp) => {
                setSamples(resp.data)
            })
            .catch((er) => {
                setError(er.message)
            })
    }, [projectName])

    const getParticipantIdToSampleId = React.useCallback(async () => {
        if (!projectName) return
        new ParticipantApi()
            .getExternalParticipantIdToInternalSampleId(projectName)
            .then((resp) => {
                setIdNameMap(resp.data)
            })
            .catch((er) => {
                setError(er.message)
            })
    }, [projectName])

    const getSamplesInfo = React.useCallback(async () => {
        if (!sample) return
        if (!projectName) return
        if (!participants) return
        new SampleApi()
            .getSampleByExternalId(sample, projectName)
            .then((resp) => {
                setSampleInfo(resp.data)
                setSelectedExternalID(
                    participants.find((item) => item.id === resp.data.participant_id)?.external_id
                )
            })
            .catch((er) => {
                setError(er.message)
            })
    }, [projectName, sample, participants])

    const getPedigree = React.useCallback(async () => {
        if (!projectName) return
        new FamilyApi()
            .getPedigree(projectName)
            .then((resp) => {
                setPedigree(resp.data)
            })
            .catch((er) => {
                setError(er.message)
            })
    }, [projectName])

    const getParticipants = React.useCallback(async () => {
        if (!projectName) return
        new ParticipantApi()
            .getParticipants(projectName)
            .then((resp) => {
                setParticipants(resp.data)
            })
            .catch((er) => {
                setError(er.message)
            })
    }, [projectName])

    const getSequenceInfo = React.useCallback(async () => {
        if (!sampleInfo) return
        if (!projectName) return
        new SequenceApi()
            .getSequencesBySampleIds([sampleInfo.id.toString()])
            .then((resp) => {
                setSequenceInfo(resp.data)
            })
            .catch((er) => {
                setError(er.message)
            })
    }, [sampleInfo, projectName])

    // retrigger if selections change
    React.useEffect(() => {
        getSamplesFromProject()
    }, [getSamplesFromProject])

    React.useEffect(() => {
        getSamplesInfo()
    }, [getSamplesInfo])

    React.useEffect(() => {
        getPedigree()
    }, [getPedigree])

    React.useEffect(() => {
        getParticipants()
    }, [getParticipants])

    React.useEffect(() => {
        getParticipantIdToSampleId()
    }, [getParticipantIdToSampleId])

    React.useEffect(() => {
        getSequenceInfo()
    }, [getSequenceInfo])

    const getCPGID = (id: string) => {
        // placeholder - will be deleted when the pedigree stuff is merged
        const CPGID = idNameMap?.find(([name]) => name === id)
        return CPGID ? CPGID[1] : -1
    }

    const drawTrio = (
        paternal: string,
        affectedPaternal: number,
        maternal: string,
        affectedMaternal: number,
        proband: string,
        affectedProband: number,
        probandSex: number
    ) => (
        <svg viewBox="75 90 250 200" width="250" height="200">
            <rect
                x={100}
                y={100}
                width={50}
                height={50}
                stroke="black"
                cursor="pointer"
                fill={affectedPaternal === 2 ? 'black' : 'white'}
                onClick={() => {
                    if (!samples) return
                    navigate(`/project/${projectName}/sample/${getCPGID(paternal)}`)
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
                fill={affectedMaternal === 2 ? 'black' : 'white'}
                onClick={() => {
                    if (!samples) return
                    navigate(`/project/${projectName}/sample/${getCPGID(maternal)}`)
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
                    fill={affectedProband === 2 ? 'black' : 'white'}
                />
            ) : (
                <circle
                    cx={187.5}
                    cy={225}
                    r={25}
                    stroke="black"
                    fill={affectedProband === 2 ? 'black' : 'white'}
                />
            )}
            <text x={187.5} y={270} textAnchor="middle">
                {proband}
            </text>
        </svg>
    )

    React.useEffect(() => {
        if (!samples || !sampleName) return
        setIsLoading(true)
        setSample(samples[sampleName])
        setIsLoading(false)
    }, [projectName, samples, sampleName])

    const renderSeqSection = () => {
        if (!sample || !sequenceInfo) {
            return <></>
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
                {sequenceInfo.map((seq) => (
                    <React.Fragment key={seq.id}>
                        <h6>
                            <b>Sequence ID:</b> {seq.id}
                        </h6>

                        <div style={{ marginLeft: '30px' }}>
                            <SeqInfo data={seq} />
                        </div>
                        <br />
                    </React.Fragment>
                ))}
            </>
        )
    }

    const renderSampleSection = () => {
        if (!sample || !sampleInfo) {
            return <></>
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
                    .filter(([key]) => sampleFieldsToDisplay.includes(key))
                    .map(([key, value]) => (
                        <div key={`${key}-${value}`}>
                            <b>{key}:</b> {value?.toString() ?? <em>no value</em>}
                        </div>
                    ))}
            </>
        )
    }

    const renderPedigreeSection = () => {
        if (!sample || !pedigree) {
            return <></>
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
                                    pedigree?.find((i) => i.individual_id === item.paternal_id)
                                        ?.affected ?? -1,
                                    item.maternal_id,
                                    pedigree?.find((i) => i.individual_id === item.maternal_id)
                                        ?.affected ?? -1,
                                    item.individual_id,
                                    item.affected,
                                    item.sex
                                )}
                        </React.Fragment>
                    ))}
            </>
        )
    }

    const renderTitle = (
        <div
            style={{
                borderBottom: `1px solid black`,
            }}
        >
            {participants &&
                participants
                    .filter((item) => item.id === sampleInfo?.participant_id)
                    .map((item) => (
                        <React.Fragment key={item.external_id}>
                            <h1
                                style={{
                                    display: 'inline',
                                }}
                                key={`${item.external_id}`}
                            >
                                {`${item.external_id}\t`}
                            </h1>
                        </React.Fragment>
                    ))}
            {sampleInfo && (
                <>
                    <h3
                        style={{
                            display: 'inline',
                        }}
                    >
                        {`${sampleInfo?.id}\t${sampleInfo?.external_id}`}
                    </h3>
                </>
            )}
        </div>
    )

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
    )
}

export default class SampleView extends React.Component<
    Record<string, unknown>,
    { error?: Error }
> {
    constructor(props: Record<string, unknown>) {
        super(props)
        this.state = {}
    }

    static getDerivedStateFromError(error: Error): { error: Error } {
        // Update state so the next render will show the fallback UI.
        return { error }
    }

    render(): React.ReactNode {
        if (this.state.error) {
            return <p>{this.state.error.toString()}</p>
        }
        return <SampleView_ /> /* eslint react/jsx-pascal-case: 0 */
    }
}
