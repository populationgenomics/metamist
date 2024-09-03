import * as React from 'react'

import { useQuery } from '@apollo/client'
import { useParams } from 'react-router-dom'
import LoadingDucks from '../../shared/components/LoadingDucks/LoadingDucks'

import { gql } from '../../__generated__/gql'
import { SplitPage } from '../../shared/components/Layout/SplitPage'
import MuckError from '../../shared/components/MuckError'
import Pedigree from '../../shared/components/pedigree/Pedigree'
import SampleInfo from '../../shared/components/SampleInfo'
import SeqPanel from '../../shared/components/SeqPanel'
import { ThemeContext } from '../../shared/components/ThemeProvider'
import { SampleCommentsView } from '../comments/SampleCommentsView'

const GET_SAMPLE_INFO = gql(`
query SampleInfo($sample_id: String!) {
    sample(id: {eq: $sample_id }) {
        id
        externalId
        project {
            name
        }
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

    const { sampleName, sequencingGroupName } = useParams()
    const sampleID = sampleName || ''

    const { loading, error, data } = useQuery(GET_SAMPLE_INFO, {
        variables: { sample_id: sampleID },
    })

    if (loading) return <LoadingDucks />
    if (error) return <>Error! {error.message}</>

    return data ? (
        <SplitPage
            main={() => (
                <>
                    {data.sample.map((sample) => (
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
                                        key={`${sample.participant?.externalId}`}
                                    >
                                        {`${sample.participant?.externalId}\t`}
                                    </h1>
                                    <h3
                                        style={{
                                            display: 'inline',
                                        }}
                                    >
                                        {`${sample.id}\t${sample.externalId}`}
                                    </h3>
                                </div>
                                <div style={{ paddingBottom: '20px' }}>
                                    {sample.participant?.families.map((family) => (
                                        <React.Fragment key={family.id}>
                                            <Pedigree familyId={family.id} />
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
                                            Object.entries(sample).filter(([key]) =>
                                                sampleFieldsToDisplay.includes(key)
                                            )
                                        )}
                                    />
                                </div>
                                <SeqPanel
                                    isOpen
                                    highlighted={sequencingGroupName ?? ''}
                                    sequencingGroups={sample.sequencingGroups}
                                />
                            </div>
                        </>
                    ))}
                </>
            )}
            side={() => {
                const projectName = data.sample[0]?.project.name
                const sampleId = data.sample[0]?.id
                if (!projectName || !sampleId) return null
                return <SampleCommentsView projectName={projectName} sampleId={sampleId} />
            }}
        />
    ) : (
        <MuckError message={`Ah Muck, there's no data here`} />
    )
}

export default SampleView
