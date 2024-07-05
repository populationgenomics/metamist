import * as React from 'react'

import { Message } from 'semantic-ui-react'
import MuckTheDuck from './MuckTheDuck'

const MuckError: React.FunctionComponent<{
    message?: string
}> = ({ message, children }) => (
    <Message negative>
        <MuckTheDuck height={28} style={{ transform: 'scaleY(-1)' }} />
        <em>{message}</em>
        {children}
    </Message>
)

export default MuckError
