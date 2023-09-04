import * as React from 'react'

import { useParams } from 'react-router-dom'
import { Table as SUITable } from 'semantic-ui-react'
import { useQuery } from '@apollo/client'
import Table from '../../shared/components/Table'

import Pedigree from '../../shared/components/pedigree/Pedigree'
import LoadingDucks from '../../shared/components/LoadingDucks/LoadingDucks'

import { gql } from '../../__generated__/gql'
import FamilyViewTitle from './FamilyViewTitle'

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
        sequencingGroups {
            id
            platform
            technology
            type
            assays {
                id
                meta
                type
            }
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

const HEADINGS = [
    'Individual ID',
    'External ID',
    'Maternal ID',
    'Paternal ID',
    'Sex',
    'Affected',
    'SG ID(s)',
    'Samples',
    'Individuals with SG Type Genome',
    'Individuals with SG Type Exome',
    'Individuals with SG type Transcriptome',
    'Individuals with SG type Mtseq',
    'Report Links',
]

const FamilyView: React.FunctionComponent<Record<string, unknown>> = () => {
    const { familyID } = useParams()
    const family_ID = familyID ? +familyID : -1

    const [activeIndices, setActiveIndices] = React.useState<number[]>([-1])

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
            const element = document.getElementById(e)
            if (element) {
                const y = element.getBoundingClientRect().top + window.pageYOffset - 100
                window.scrollTo({ top: y, behavior: 'smooth' })
            }
        },
        [data, activeIndices]
    )

    if (loading) return <LoadingDucks />
    if (error) return <>Error! {error.message}</>

    const tableData = data?.family.participants.map((participant) => {
        const types = [
            ...new Set(
                participant.samples.map((s) => s.sequencingGroups.map((sg) => sg.type)).flat()
            ),
        ]
            .flat()
            .reduce((acc, color) => ((acc[color] = (acc[color] || 0) + 1), acc), {})
        const pedEntry = data?.family.project.pedigree.find(
            (p) => p.individual_id === participant.externalId
        )
        return {
            individualID: participant.id,
            externaldID: participant.externalId,
            maternalID: pedEntry?.maternal_id,
            paternalID: pedEntry?.paternal_id,
            sex: pedEntry?.sex,
            affected: pedEntry?.affected,
            SG_ids: participant.samples
                .map((sample) => sample.sequencingGroups.map((sg) => sg.id).join(', '))
                .join(', '),
            samples: participant.samples.map((sample) => sample.id).join(', '),
            individuals_SG_genome: types.genome ?? 0,
            individuals_SG_exome: types.exome ?? 0,
            individuals_SG_transcriptome: types.transcriptome ?? 0,
            individuals_SG_mtseq: types.mtseq ?? 0,
        }
    })

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
                <Table celled compact sortable>
                    <SUITable.Header>
                        <SUITable.Row>
                            {HEADINGS.map((title) => (
                                <SUITable.HeaderCell
                                    key={title}
                                    style={{
                                        borderBottom: 'none',
                                        position: 'sticky',
                                        resize: 'horizontal',
                                    }}
                                >
                                    {title}
                                </SUITable.HeaderCell>
                            ))}
                        </SUITable.Row>
                    </SUITable.Header>
                    <SUITable.Body>
                        {tableData?.map((row, i) => (
                            <SUITable.Row key={i}>
                                {Object.values(row).map((cell, j) => (
                                    <SUITable.Cell key={`${i}-${j}`}>{cell}</SUITable.Cell>
                                ))}
                            </SUITable.Row>
                        ))}
                    </SUITable.Body>
                </Table>
            </>
        </div>
    ) : (
        <></>
    )
}

export default FamilyView
