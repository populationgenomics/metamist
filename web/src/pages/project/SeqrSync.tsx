import * as React from 'react'
import _ from 'lodash'
import { SequenceType, WebApi } from '../../sm-api'
import { Button, Message } from 'semantic-ui-react'
import LoadingDucks from '../../shared/components/LoadingDucks/LoadingDucks'
import MuckTheDuck from '../../shared/components/MuckTheDuck'

interface SeqrSyncProps {
    project: string

    syncTypes: SequenceType[]
}
const SeqrSync: React.FunctionComponent<SeqrSyncProps> = ({ syncTypes, project }) => {
    const [loadState, setLoadState] = React.useState<{ [stype: string]: boolean }>({})
    const [messages, setMessages] = React.useState<string[]>([])
    const [error, setError] = React.useState<string | null>()

    if (!syncTypes || syncTypes.length == 0) return <></>

    const syncSeqrProject = (seqType: SequenceType) => {
        setLoadState({ ...loadState, [seqType]: true })
        new WebApi()
            .syncSeqrProject(seqType, project)
            .then((resp) => {
                setError(null)
                setLoadState({ ...loadState, [seqType]: false })
                setMessages(resp.data.messages)
            })
            .catch((er) => {
                let message = er.message
                if (er?.response?.data) {
                    message = er?.response?.data.description + '\n' + er?.response.data.stacktrace
                }

                setLoadState({ ...loadState, [seqType]: false })
                setError(message)
            })
    }

    return (
        <>
            <h4>Synchronise Seqr</h4>
            {!!error && (
                <Message negative onDismiss={() => setError(null)}>
                    <h4>
                        <b>An error occurred while syncing a seqr project</b>
                    </h4>
                    <p style={{ whiteSpace: 'pre-line' }}>{error}</p>
                </Message>
            )}
            {(messages?.length || 0) > 0 && (
                <Message info>
                    <h4>
                        <b>Seqr project was synchronised successfully</b>
                    </h4>
                    <ul>
                        {messages.map((m) => (
                            <li key={m}>{m}</li>
                        ))}
                    </ul>
                </Message>
            )}
            {syncTypes.map((st) => (
                <Button key={`sync-seqr-${st}`} onClick={() => syncSeqrProject(st)}>
                    {_.capitalize(st)}
                    {loadState[st] && <MuckTheDuck height={28} className="loadingScreen" />}
                </Button>
            ))}
        </>
    )
}

export default SeqrSync
