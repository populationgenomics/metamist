import * as React from 'react'

import { useParams } from 'react-router-dom'
import { useQuery } from '@apollo/client'
import _ from 'lodash'
import { SeqInfo } from '../renders/SeqInfo'

import { gql } from '../__generated__/gql'

const GET_SAMPLE_INFO = gql(`
query SampleInfo($sample_id: String!) {
    sample(id: $sample_id) {
      id
      participantId
      externalId
      participant {
        externalId
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

const SampleView_: React.FunctionComponent<Record<string, unknown>> = () => {
    const { sampleName } = useParams()
    // const sampleID = sampleName || ''
    const sampleID = sampleName || 'CPGLCL978'

    const { loading, error, data } = useQuery(GET_SAMPLE_INFO, {
        variables: { sample_id: sampleID },
    })
    if (loading) return <>Loading...</>
    if (error) return <>Error! ${error.message}</>

    const renderSeqSection = () => {
        if (!data) {
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
                {data.sample.sequences.map((seq) => (
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
        if (!data) {
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
                {Object.entries(data.sample)
                    .filter(([key]) => sampleFieldsToDisplay.includes(key))
                    .map(([key, value]) => (
                        <div key={`${key}-${value}`}>
                            <b>{_.capitalize(key)}:</b> {value?.toString() ?? <em>no value</em>}
                        </div>
                    ))}
            </>
        )
    }

    const renderTitle = () => {
        if (!data) {
            return <>Test</>
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
        )
    }

    return (
        <>
            <div className="detailedInfo">
                <div>{renderTitle()}</div>
                {/* <br />
                <div>{renderPedigreeSection()}</div> */}
                <br />
                <div>{renderSampleSection()}</div>
                <br />
                <div>{renderSeqSection()}</div>
            </div>
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
