import * as React from 'react'

import { useParams } from 'react-router-dom'
import { useQuery } from '@apollo/client'
import LoadingDucks from '../../shared/components/LoadingDucks/LoadingDucks'

import { gql } from '../../__generated__/gql'
import Pedigree from '../../shared/components/pedigree/Pedigree'
import SeqPanel from '../../shared/components/SeqPanel'
import MuckError from '../../shared/components/MuckError'
import SampleInfo from '../../shared/components/SampleInfo'
import { ThemeContext } from '../../shared/components/ThemeProvider'

const GET_SAMPLE_INFO = gql(`
query SampleInfo($sample_id: String!) {
    sample(id: $sample_id) {
        id
        externalId
        participant {
            id
            externalId
            families {
                id
            }
        }
        type
        active
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
}`)

const sampleFieldsToDisplay = ['active', 'type']

const SampleView: React.FunctionComponent<Record<string, unknown>> = () => {
    const theme = React.useContext(ThemeContext)
    const isDarkMode = theme.theme === 'dark-mode'

    const { sampleName, sequenceGroupName } = useParams()
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
                        borderBottom: `1px solid ${isDarkMode ? 'white' : 'black'}`,
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
                            borderBottom: `1px solid ${isDarkMode ? 'white' : 'black'}`,
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
                <SeqPanel
                    isOpen
                    highlighted={sequenceGroupName ?? ''}
                    sequencingGroups={data.sample.sequencingGroups}
                />
            </div>
        </>
    ) : (
        <MuckError message={`Ah Muck, there's no data here`} />
    )
}

export default SampleView
