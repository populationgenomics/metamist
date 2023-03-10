import * as React from 'react'

import { useParams } from 'react-router-dom'
import { useQuery } from '@apollo/client'
import LoadingDucks from '../../shared/components/LoadingDucks/LoadingDucks'

import { gql } from '../../__generated__/gql'
import Pedigree from '../../shared/components/pedigree/Pedigree'
import SeqPanel from '../../shared/components/SeqPanel'
import MuckError from '../../shared/components/MuckError'
import SampleInfo from '../../shared/components/SampleInfo'

const GET_SAMPLE_INFO = gql(`
query SampleInfo($sample_id: String!) {
    sample(id: $sample_id) {
      id
      participantId
      externalId
      participant {
        externalId
        families {
            id
        }
      }
      sequences {
        id
        externalIds
        type
        meta
      }
      type
      active
    }
  }
`)

const sampleFieldsToDisplay = ['active', 'type', 'participantId']

const SampleView: React.FunctionComponent<Record<string, unknown>> = () => {
    const { sampleName } = useParams()
    const sampleID = sampleName || ''

    const { loading, error, data } = useQuery(GET_SAMPLE_INFO, {
        variables: { sample_id: sampleID },
    })

    if (loading) return <LoadingDucks />
    if (error) return <>Error! {error.message}</>

    return data ? (
        <>
            <div className="dataStyle">
                <div
                    style={{
                        borderBottom: `1px solid black`,
                    }}
                >
                    <h1
                        style={{
                            display: 'inline',
                        }}
                        key={`${data.sample.participant?.externalId}`}
                    >
                        {`${data.sample.participant?.externalId}\t`}
                    </h1>
                    <h3
                        style={{
                            display: 'inline',
                        }}
                    >
                        {`${data.sample.id}\t${data.sample.externalId}`}
                    </h3>
                </div>
                <div style={{ paddingBottom: '20px' }}>
                    {data?.sample.participant?.families.map((family) => (
                        <React.Fragment key={family.id}>
                            <Pedigree familyID={family.id} />
                        </React.Fragment>
                    ))}
                </div>
                <div style={{ paddingBottom: '20px' }}>
                    <h4
                        style={{
                            borderBottom: `1px solid black`,
                        }}
                    >
                        Sample Information
                    </h4>
                    <SampleInfo
                        sample={Object.fromEntries(
                            Object.entries(data.sample).filter(([key]) =>
                                sampleFieldsToDisplay.includes(key)
                            )
                        )}
                    />
                </div>
                <SeqPanel isOpen sequences={data.sample.sequences} />
            </div>
        </>
    ) : (
        <MuckError message={`Ah Muck, there's no data here`} />
    )
}

export default SampleView
