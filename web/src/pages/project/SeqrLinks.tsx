import * as React from 'react'
import _ from 'lodash'
import { SequenceType, WebApi } from '../../sm-api'
import { Button } from 'semantic-ui-react'

interface SeqrLinksProps {
    project: string
    seqrLinks: Record<string, string>
    syncTypes: SequenceType[]
}

const SeqrLinks: React.FunctionComponent<SeqrLinksProps> = ({ seqrLinks, syncTypes, project }) => {
    const seqrLinkEntries = Object.entries(seqrLinks)
    if (!seqrLinkEntries.length) return <></>

    const syncSeqrProject = (seqType: SequenceType) => {
        new WebApi().syncSeqrProject(seqType, project)
            .then(resp => alert(resp.data))
            .catch(er => alert(er))
    }

    return (
        <>
            <h4> Seqr Links</h4>
            {seqrLinkEntries.map(([key, value]) => (
                <a href={value} className="ui button" key={key} target="_blank" rel="noreferrer">
                    {_.capitalize(key)}
                </a>
            ))}

            {syncTypes.length > 0 && <>
                <h4>Synchronise Seqr</h4>
                {syncTypes.map(st => <Button key={`sync-seqr-${st}`} onClick={() => syncSeqrProject(st)}>{st}</Button>)}
            </>
            }
        </>
    )
}

export default SeqrLinks
