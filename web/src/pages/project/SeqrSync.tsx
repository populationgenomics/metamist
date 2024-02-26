import * as React from 'react'
import _ from 'lodash'
import { Button, CheckboxProps, DropdownProps, Form, Message, Modal } from 'semantic-ui-react'
import { WebApi } from '../../sm-api'
import MuckTheDuck from '../../shared/components/MuckTheDuck'

interface SeqrSyncProps {
    project: string
    syncTypes: string[]
}

interface SeqrSyncFormOptions {
    syncFamilies?: boolean
    syncIndividualMetadata?: boolean
    syncIndividuals?: boolean
    syncEsIndex?: boolean
    esIndexType: string
    syncSavedVariants?: boolean
    syncCramMap?: boolean
    postSlackNotification?: boolean
}

const SeqrSync: React.FunctionComponent<SeqrSyncProps> = ({ syncTypes, project }) => {
    const [isLoading, setIsLoading] = React.useState<boolean>(false)
    const [messages, setMessages] = React.useState<string[]>([])
    const [seqTypeInModal, setSeqTypeInModal] = React.useState<string | null>(null)
    const [errors, setErrors] = React.useState<string[] | null>()

    const [syncOptions, setSyncOptions] = React.useState<SeqrSyncFormOptions>({
        syncFamilies: true,
        syncIndividualMetadata: true,
        syncIndividuals: true,
        syncEsIndex: true,
        esIndexType: 'Haplotypecaller',
        syncSavedVariants: true,
        syncCramMap: true,
        postSlackNotification: true,
    })

    if (!syncTypes || syncTypes.length === 0) return <></>

    const syncSeqrProject = (seqType: string | null) => {
        if (!seqType) return
        setIsLoading(true)
        setErrors(null)
        new WebApi()
            .syncSeqrProject(
                seqType,
                project,
                syncOptions.esIndexType,
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
                let { message } = er
                if (er?.response?.data) {
                    message = `${er?.response?.data.description}\n${er?.response.data.stacktrace}`
                }

                setIsLoading(false)
                setErrors([message])
            })
    }

    const updateStateFromCheckbox = (e: React.FormEvent<HTMLInputElement>, data: CheckboxProps) => {
        const value = data.checked
        setSyncOptions({ ...syncOptions, [data.id || '']: value })
    }

    const updateStateFromDropdown = (
        e: React.SyntheticEvent<HTMLElement, Event>,
        data: DropdownProps
    ) => {
        const value = (data as any).value
        setSyncOptions({ ...syncOptions, [data.id || '']: value })
    }

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
                            onChange={updateStateFromCheckbox}
                            label="Sync pedigree"
                        />
                        <Form.Checkbox
                            id="syncFamilies"
                            checked={syncOptions.syncFamilies}
                            onChange={updateStateFromCheckbox}
                            label="Sync families"
                        />
                        <Form.Checkbox
                            id="syncIndividualMetadata"
                            checked={syncOptions.syncIndividualMetadata}
                            onChange={updateStateFromCheckbox}
                            label="Sync individuals metadata"
                        />
                        <Form.Checkbox
                            id="syncEsIndex"
                            checked={syncOptions.syncEsIndex}
                            onChange={updateStateFromCheckbox}
                            label="Sync elastic-search index"
                        />
                        <Form.Dropdown
                            id="esIndexType"
                            options={[
                                {
                                    key: 'Haplotypecaller',
                                    text: 'Haplotypecaller',
                                    value: 'Haplotypecaller',
                                },
                                { key: 'SV_Caller', text: 'SV_Caller', value: 'SV_Caller' },
                                {
                                    key: 'Mitochondria_Caller',
                                    text: 'Mitochondria_Caller',
                                    value: 'Mitochondria_Caller',
                                },
                            ]}
                            value={syncOptions.esIndexType}
                            onChange={updateStateFromDropdown}
                            label="ES index type"
                        />
                        <Form.Checkbox
                            id="syncCramMap"
                            checked={syncOptions.syncCramMap}
                            onChange={updateStateFromCheckbox}
                            label="Sync CRAMs"
                        />
                        <br />
                        <Form.Checkbox
                            id="postSlackNotification"
                            checked={syncOptions.postSlackNotification}
                            onChange={updateStateFromCheckbox}
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
                    <Button onClick={() => syncSeqrProject(seqTypeInModal)}>Synchronise</Button>
                </Modal.Actions>
            </Modal>
        </>
    )
}

export default SeqrSync
