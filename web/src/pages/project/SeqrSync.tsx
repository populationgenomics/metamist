import * as React from 'react'
import _ from 'lodash'
import { SequenceType, WebApi } from '../../sm-api'
import { Button, CheckboxProps, Form, Message, Modal } from 'semantic-ui-react'
import LoadingDucks from '../../shared/components/LoadingDucks/LoadingDucks'
import MuckTheDuck from '../../shared/components/MuckTheDuck'

interface SeqrSyncProps {
    project: string

    syncTypes: SequenceType[]
}

interface SeqrSyncFormOptions {
    syncFamilies?: boolean
    syncIndividualMetadata?: boolean
    syncIndividuals?: boolean
    syncEsIndex?: boolean
    syncSavedVariants?: boolean
    syncCramMap?: boolean
    postSlackNotification?: boolean
}

const SeqrSync: React.FunctionComponent<SeqrSyncProps> = ({ syncTypes, project }) => {
    const [isLoading, setIsLoading] = React.useState<boolean>(false)
    const [messages, setMessages] = React.useState<string[]>([])
    const [seqTypeInModal, setSeqTypeInModal] = React.useState<SequenceType | null>(null)
    const [errors, setErrors] = React.useState<string[] | null>()

    const [syncOptions, setSyncOptions] = React.useState<SeqrSyncFormOptions>({
        syncFamilies: true,
        syncIndividualMetadata: true,
        syncIndividuals: true,
        syncEsIndex: true,
        syncSavedVariants: true,
        syncCramMap: true,
        postSlackNotification: true,
    })

    if (!syncTypes || syncTypes.length == 0) return <></>

    const syncSeqrProject = (seqType: SequenceType) => {
        setIsLoading(true)
        setErrors(null)
        new WebApi()
            .syncSeqrProject(
                seqType,
                project,
                syncOptions.syncFamilies,
                syncOptions.syncIndividualMetadata,
                syncOptions.syncIndividuals,
                syncOptions.syncEsIndex,
                syncOptions.syncEsIndex, // tie syncSavedVariants, to syncing es-index
                syncOptions.syncCramMap,
                syncOptions.postSlackNotification
            )
            .then((resp) => {
                setIsLoading(false)
                if (!resp.data.success) {
                    setErrors(resp.data.errors)
                } else {
                    setErrors(null)
                }
                setMessages(resp.data.messages)
            })
            .catch((er) => {
                let message = er.message
                if (er?.response?.data) {
                    message = er?.response?.data.description + '\n' + er?.response.data.stacktrace
                }

                setIsLoading(false)
                setErrors([message])
            })
    }

    const updateStateFromForm = (e: React.FormEvent<HTMLInputElement>, data: CheckboxProps) =>
        setSyncOptions({ ...syncOptions, [data.id || '']: data.checked })

    return (
        <>
            <h4>Synchronise Seqr</h4>

            {syncTypes.map((st) => (
                <Button key={`sync-seqr-${st}`} onClick={() => setSeqTypeInModal(st)}>
                    {_.capitalize(st)}
                </Button>
            ))}
            <Modal
                size="small"
                // dimmer='blurring'
                open={!!seqTypeInModal}
                onClose={() => setSeqTypeInModal(null)}
                style={{ height: 'unset', top: 'unset', left: 'unset' }}
            >
                <Modal.Header>
                    Synchronise {seqTypeInModal} <em>seqr</em> project
                </Modal.Header>
                <Modal.Content>
                    Select sync options:
                    <Form style={{ paddingTop: '20px', paddingLeft: '20px' }}>
                        <Form.Checkbox
                            id="syncIndividuals"
                            checked={syncOptions.syncIndividuals}
                            onChange={updateStateFromForm}
                            label="Sync pedigree"
                        />
                        <Form.Checkbox
                            id="syncFamilies"
                            checked={syncOptions.syncFamilies}
                            onChange={updateStateFromForm}
                            label="Sync families"
                        />
                        <Form.Checkbox
                            id="syncIndividualMetadata"
                            checked={syncOptions.syncIndividualMetadata}
                            onChange={updateStateFromForm}
                            label="Sync individuals metadata"
                        />
                        <Form.Checkbox
                            id="syncEsIndex"
                            checked={syncOptions.syncEsIndex}
                            onChange={updateStateFromForm}
                            label="Sync elastic-search index"
                        />
                        <Form.Checkbox
                            id="syncCramMap"
                            checked={syncOptions.syncCramMap}
                            onChange={updateStateFromForm}
                            label="Sync CRAMs"
                        />
                        <br />
                        <Form.Checkbox
                            id="postSlackNotification"
                            checked={syncOptions.postSlackNotification}
                            onChange={updateStateFromForm}
                            label="Post slack notification"
                        />

                        {!!errors && (
                            <Message negative onDismiss={() => setErrors(null)}>
                                <h4>
                                    <b>An error occurred while syncing a seqr project</b>
                                </h4>
                                <ul style={{ whiteSpace: 'pre-line' }}>
                                    {errors.map((e) => (
                                        <li key={e}>{e}</li>
                                    ))}
                                </ul>
                                {messages.length > 0 && (
                                    <>
                                        <b>Messages</b>
                                        <ul>
                                            {messages.map((m) => (
                                                <li key={m}>{m}</li>
                                            ))}
                                        </ul>
                                    </>
                                )}
                            </Message>
                        )}
                        {!errors && (messages?.length || 0) > 0 && (
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
                        {isLoading && <MuckTheDuck height={28} className="loadingScreen" />}
                    </Form>
                </Modal.Content>
                <Modal.Actions>
                    <Button onClick={() => syncSeqrProject(seqTypeInModal!)}>Synchronise</Button>
                </Modal.Actions>
            </Modal>
        </>
    )
}

export default SeqrSync
