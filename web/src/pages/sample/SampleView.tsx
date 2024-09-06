import * as React from 'react'

import { useQuery } from '@apollo/client'
import { useParams } from 'react-router-dom'
import LoadingDucks from '../../shared/components/LoadingDucks/LoadingDucks'

import { gql } from '../../__generated__/gql'
import { PaddedPage } from '../../shared/components/Layout/PaddedPage'
import { SplitPage } from '../../shared/components/Layout/SplitPage'
import SampleLink from '../../shared/components/links/SampleLink'
import MuckError from '../../shared/components/MuckError'
import Pedigree from '../../shared/components/pedigree/Pedigree'
import SampleInfo from '../../shared/components/SampleInfo'
import SeqPanel from '../../shared/components/SeqPanel'
import { ThemeContext } from '../../shared/components/ThemeProvider'
import { SampleCommentsView } from '../comments/entities/SampleCommentsView'
import { SequencingGroupCommentsView } from '../comments/entities/SequencingGroupCommentsView'

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
            collapsed={true}
            collapsedWidth={64}
            main={() => (
                <PaddedPage>
                    {data.sample.map((sample) => (
                        <div key={sample.id}>
                            <div className="dataStyle">
                                <SampleLink id={sample.id}>
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
                                </SampleLink>
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
                                    sampleId={sample.id}
                                    highlighted={sequencingGroupName ?? ''}
                                    sequencingGroups={sample.sequencingGroups}
                                />
                            </div>
                        </div>
                    ))}
                </PaddedPage>
            )}
            side={({ collapsed, onToggleCollapsed }) => {
                const projectName = data.sample[0]?.project.name
                const sampleId = data.sample[0]?.id
                if (!projectName || !sampleId) return null

                // Return seq group comments if focussed on a sequencing group
                if (sequencingGroupName) {
                    return (
                        <SequencingGroupCommentsView
                            projectName={projectName}
                            sequencingGroupId={sequencingGroupName}
                            collapsed={collapsed}
                            onToggleCollapsed={onToggleCollapsed}
                        />
                    )
                }
                return (
                    <SampleCommentsView
                        projectName={projectName}
                        sampleId={sampleId}
                        collapsed={collapsed}
                        onToggleCollapsed={onToggleCollapsed}
                    />
                )
            }}
        />
    ) : (
        <MuckError message={`Ah Muck, there's no data here`} />
    )
}

export default SampleView
