import * as React from 'react'

import MuckTheDuck from './MuckTheDuck'

const MuckError: React.FunctionComponent<{
    message: string
}> = ({ message }) => (
    <p>
        <em>{message}</em> <MuckTheDuck height={28} style={{ transform: 'scaleY(-1)' }} />
    </p>
)

export default MuckError
