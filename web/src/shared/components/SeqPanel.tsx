import * as React from 'react'

import { Accordion } from 'semantic-ui-react'
import ScienceRoundedIcon from '@mui/icons-material/ScienceRounded'
import { GraphQlSequencingGroup } from '../../__generated__/graphql'
import iconStyle from '../iconStyle'
import { DeepPartial } from '../utilities/deepPartial'
import SeqGroupInfo from './SeqGroupInfo'

const SeqPanel: React.FunctionComponent<{
    sequencingGroups: DeepPartial<GraphQlSequencingGroup>[]
    isOpen?: boolean
}> = ({ sequencingGroups, isOpen = false }) => (
    <Accordion
        styled
        className="accordionStyle"
        panels={sequencingGroups.map((seq) => ({
            key: seq.id,
            title: {
                content: (
                    <h3
                        style={{
                            display: 'inline',
                        }}
                    >
                        Sequencing Group ID: {seq.id}
                    </h3>
                ),
                icon: <ScienceRoundedIcon sx={iconStyle} />,
            },
            content: {
                content: (
                    <div style={{ marginLeft: '30px' }}>
                        <SeqGroupInfo data={seq} />
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
