import * as React from 'react'

import { useParams } from 'react-router-dom'
import { Accordion, AccordionTitleProps } from 'semantic-ui-react'

import PersonRoundedIcon from '@mui/icons-material/PersonRounded'
import BloodtypeRoundedIcon from '@mui/icons-material/BloodtypeRounded'

import { useQuery } from '@apollo/client'
import Pedigree from '../../shared/components/pedigree/Pedigree'
import LoadingDucks from '../../shared/components/LoadingDucks/LoadingDucks'

import { gql } from '../../__generated__/gql'
import FamilyViewTitle from './FamilyViewTitle'

import iconStyle from '../../shared/iconStyle'
import SeqPanel from '../../shared/components/SeqPanel'
import SampleInfo from '../../shared/components/SampleInfo'

const sampleFieldsToDisplay = ['active', 'type', 'participantId']

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
        participants {
            id
        }
      }
      pedigree(internalFamilyIds: [$family_id])
      name
    }
  }
}`)

const FamilyView: React.FunctionComponent<Record<string, unknown>> = () => {
    const { familyID } = useParams()
    const family_ID = familyID ? +familyID : -1

    const [activeIndices, setActiveIndices] = React.useState<number[]>([-1])
    const [mostRecent, setMostRecent] = React.useState<string>('')

    const { loading, error, data } = useQuery(GET_FAMILY_INFO, {
        variables: { family_id: family_ID },
    })

    const onPedigreeClick = React.useCallback(
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

    if (loading) return <LoadingDucks />
    if (error) return <>Error! {error.message}</>

    return data ? (
        <div className="dataStyle" style={{ width: '100%' }}>
            <>
                <FamilyViewTitle
                    projectName={data?.family.project.name}
                    families={data?.family.project.families.filter(
                        (family) => family.participants.length
                    )}
                    externalId={data?.family.externalId}
                />
                <Pedigree familyID={family_ID} onClick={onPedigreeClick} />
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
                                                            <SampleInfo
                                                                sample={Object.fromEntries(
                                                                    Object.entries(s).filter(
                                                                        ([key]) =>
                                                                            sampleFieldsToDisplay.includes(
                                                                                key
                                                                            )
                                                                    )
                                                                )}
                                                            />

                                                            <SeqPanel sequences={s.sequences} />
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
    ) : (
        <></>
    )
}

export default FamilyView
