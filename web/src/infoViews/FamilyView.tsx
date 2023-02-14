import * as React from 'react'

import { useParams, useNavigate } from 'react-router-dom'
import { Table, Accordion, Popup, Button, Grid, AccordionTitleProps } from 'semantic-ui-react'

import Diversity3RoundedIcon from '@mui/icons-material/Diversity3Rounded'
import ScienceRoundedIcon from '@mui/icons-material/ScienceRounded'
import PersonRoundedIcon from '@mui/icons-material/PersonRounded'
import BloodtypeRoundedIcon from '@mui/icons-material/BloodtypeRounded'

import { useQuery } from '@apollo/client'
import _ from 'lodash'
import TangledTree from '../renders/TangledTree'
import { SeqInfo } from '../renders/SeqInfo'
import LoadingDucks from '../renders/LoadingDucks'

import { gql } from '../__generated__/gql'
import { GraphQlSampleSequencing, GraphQlSample } from '../__generated__/graphql'

const sampleFieldsToDisplay = ['active', 'type', 'participantId']

const iconStyle = {
    fontSize: 30,
    height: '33px',
    verticalAlign: 'bottom',
    marginRight: '10px',
}

const GET_FAMILY_INFO = gql(`
query FamilyInfo($family_id: Int!) {
  family(familyId: $family_id) {
    id
    externalId
    participants {
      id
      samples {
        active
        author
        externalId
        id
        meta
        type
        participantId
        sequences {
          externalIds
          id
          meta
          type
        }
      }
      externalId
    }
    project {
      families {
        externalId
        id
      }
      pedigree(internalFamilyIds: [$family_id])
      name
    }
  }
}`)

const FamilyView_: React.FunctionComponent<Record<string, unknown>> = () => {
    const { projectName, familyID } = useParams()
    const family_ID = familyID ? +familyID : -1
    const navigate = useNavigate()

    const [activeIndices, setActiveIndices] = React.useState<number[]>([-1])
    const [mostRecent, setMostRecent] = React.useState<string>('')

    const { loading, error, data } = useQuery(GET_FAMILY_INFO, {
        variables: { family_id: family_ID },
    })

    const renderSeqSection = (s: Partial<GraphQlSampleSequencing>[]) => (
        <Accordion
            styled
            className="accordionStyle"
            panels={s.map((seq) => ({
                key: seq.id,
                title: {
                    content: (
                        <h3
                            style={{
                                display: 'inline',
                            }}
                        >
                            Sequence ID: {seq.id}
                        </h3>
                    ),
                    icon: <ScienceRoundedIcon sx={iconStyle} />,
                },
                content: {
                    content: (
                        <div style={{ marginLeft: '30px' }}>
                            <SeqInfo data={seq} />
                        </div>
                    ),
                },
            }))}
            exclusive={false}
            fluid
        />
    )

    const renderSampleSection = (sample: Partial<GraphQlSample>) => (
        <Table celled collapsing>
            <Table.Body>
                {Object.entries(sample).map(([key, value]) => (
                    <Table.Row key={`${key}-${value}`}>
                        <Table.Cell>
                            <b>{_.capitalize(key)}</b>
                        </Table.Cell>
                        <Table.Cell>{value?.toString() ?? <em>no value</em>}</Table.Cell>
                    </Table.Row>
                ))}
            </Table.Body>
        </Table>
    )

    const renderTitle = () => {
        if (!data) {
            return <></>
        }
        return (
            <div
                style={{
                    borderBottom: `1px solid black`,
                }}
            >
                <h1
                    style={{
                        display: 'inline',
                    }}
                >
                    {data?.family.externalId}
                </h1>
                {data.family.project.families.length > 1 && (
                    <div style={{ float: 'right' }}>
                        {`Change Families within ${data.family.project.name}\t`}
                        <Popup
                            trigger={
                                <Diversity3RoundedIcon
                                    sx={{
                                        fontSize: 40,
                                    }}
                                />
                            }
                            hoverable
                            style={{ float: 'right' }}
                        >
                            <Grid divided centered rows={3}>
                                {data.family.project.families.map((item) => (
                                    <Grid.Row key={item.id} textAlign="center">
                                        <Button
                                            onClick={() => {
                                                navigate(
                                                    `/project/${projectName}/family/${item.id}`
                                                )
                                            }}
                                        >
                                            {item.externalId}
                                        </Button>
                                    </Grid.Row>
                                ))}
                            </Grid>
                        </Popup>
                    </div>
                )}
            </div>
        )
    }

    const onClick = React.useCallback(
        (e: string) => {
            if (!data) return
            const indexToSet = Object.entries(data?.family.participants)
                .map(([, value]) => value.externalId)
                .findIndex((i) => i === e)
            if (!activeIndices.includes(indexToSet)) {
                setActiveIndices([...activeIndices, indexToSet])
            }
            setMostRecent(e)
            const element = document.getElementById(e)
            if (element) {
                const y = element.getBoundingClientRect().top + window.pageYOffset - 100
                window.scrollTo({ top: y, behavior: 'smooth' })
            }
        },
        [data, activeIndices]
    )

    const handleTitleClick = (e: React.MouseEvent, itemProps: AccordionTitleProps) => {
        setMostRecent('')
        const index = itemProps.index ?? -1
        if (index === -1) return
        if (activeIndices.indexOf(+index) > -1) {
            setActiveIndices(activeIndices.filter((i) => i !== index))
        } else {
            setActiveIndices([...activeIndices, +index])
        }
    }

    const renderedPedigree = React.useMemo(
        () =>
            data?.family.project.pedigree && (
                <TangledTree data={data?.family.project.pedigree} click={onClick} />
            ),
        [data, onClick]
    )

    if (loading) return <LoadingDucks />
    if (error) return <>Error! {error.message}</>

    return (
        <div className="familyView" style={{ width: '100%' }}>
            <>
                {renderTitle()}
                {renderedPedigree}
                <Accordion
                    onTitleClick={handleTitleClick}
                    activeIndex={activeIndices}
                    styled
                    className="accordionStyle"
                    exclusive={false}
                    panels={data?.family.participants.map((item) => ({
                        key: item.id,
                        title: {
                            content: (
                                <h2
                                    style={{
                                        display: 'inline',
                                    }}
                                    className={
                                        mostRecent === item.externalId
                                            ? 'selectedParticipant'
                                            : undefined
                                    }
                                    id={item.externalId}
                                >
                                    {item.externalId}
                                </h2>
                            ),
                            icon: (
                                <PersonRoundedIcon
                                    className={
                                        mostRecent === item.externalId
                                            ? 'selectedParticipant'
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
                                        marginLeft: '30px',
                                    }}
                                >
                                    <Accordion
                                        styled
                                        className="accordionStyle"
                                        panels={item.samples.map((s) => ({
                                            key: s.id,
                                            title: {
                                                content: (
                                                    <>
                                                        <h2
                                                            style={{
                                                                display: 'inline',
                                                            }}
                                                        >
                                                            {`${s.id}\t`}
                                                        </h2>

                                                        <h3
                                                            style={{
                                                                display: 'inline',
                                                            }}
                                                        >
                                                            {s.externalId}
                                                        </h3>
                                                    </>
                                                ),
                                                icon: <BloodtypeRoundedIcon sx={iconStyle} />,
                                            },
                                            content: {
                                                content: (
                                                    <>
                                                        <div
                                                            style={{
                                                                marginLeft: '30px',
                                                            }}
                                                        >
                                                            {renderSampleSection(
                                                                Object.fromEntries(
                                                                    Object.entries(s).filter(
                                                                        ([key]) =>
                                                                            sampleFieldsToDisplay.includes(
                                                                                key
                                                                            )
                                                                    )
                                                                )
                                                            )}
                                                            {renderSeqSection(s.sequences)}
                                                        </div>
                                                    </>
                                                ),
                                            },
                                        }))}
                                        exclusive={false}
                                    />
                                </div>
                            ),
                        },
                    }))}
                />
            </>
        </div>
    )
}

export default class FamilyView extends React.Component<
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
        return <FamilyView_ /> /* eslint react/jsx-pascal-case: 0 */
    }
}
