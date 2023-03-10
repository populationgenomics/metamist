import * as React from 'react'

import { Accordion } from 'semantic-ui-react'
import ScienceRoundedIcon from '@mui/icons-material/ScienceRounded'
import { GraphQlSampleSequencing } from '../../__generated__/graphql'
import SeqInfo from './SeqInfo'
import iconStyle from '../iconStyle'

const SeqPanel: React.FunctionComponent<{
    sequences: Partial<GraphQlSampleSequencing>[]
    isOpen?: boolean
}> = ({ sequences, isOpen = false }) => (
    <Accordion
        styled
        className="accordionStyle"
        panels={sequences.map((seq) => ({
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
            ...(isOpen && { active: isOpen }),
        }))}
        exclusive={false}
        fluid
    />
)

export default SeqPanel
