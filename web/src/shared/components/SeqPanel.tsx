import * as React from 'react'

import ScienceRoundedIcon from '@mui/icons-material/ScienceRounded'
import { Accordion } from 'semantic-ui-react'
import { GraphQlSequencingGroup } from '../../__generated__/graphql'
import iconStyle from '../iconStyle'
import { DeepPartial } from '../utilities/deepPartial'
import SequencingGroupLink from './links/SequencingGroupLink'
import SequencingGroupInfo from './SequencingGroupInfo'

const SeqPanel: React.FunctionComponent<{
    sequencingGroups: DeepPartial<GraphQlSequencingGroup>[]
    sampleId: string
    highlighted?: string
    isOpen?: boolean
}> = ({ sequencingGroups, isOpen = false, highlighted, sampleId }) => (
    <Accordion
        styled
        className="accordionStyle"
        panels={sequencingGroups.map((seq) => ({
            key: seq.id,
            title: {
                content: (
                    <SequencingGroupLink id={seq.id || ''} sampleId={sampleId}>
                        <h3
                            style={{
                                display: 'inline',
                                color:
                                    seq.id === highlighted
                                        ? 'var(--color-text-red)'
                                        : 'var(--color-text-primary)',
                            }}
                        >
                            Sequencing Group ID: {seq.id}
                        </h3>
                    </SequencingGroupLink>
                ),
                icon: <ScienceRoundedIcon sx={iconStyle} />,
            },
            content: {
                content: (
                    <div style={{ marginLeft: '30px' }}>
                        <SequencingGroupInfo data={seq} />
                    </div>
                ),
            },
            ...(isOpen && { active: isOpen }),
        }))}
        exclusive={false}
        fluid
    />
)

export default SeqPanel
