import * as React from 'react'
import _ from 'lodash'

interface SeqrLinksProps {
    seqrLinks: Record<string, string>
}

const SeqrLinks: React.FunctionComponent<SeqrLinksProps> = ({ seqrLinks }) => {
    const seqrLinkEntries = Object.entries(seqrLinks)
    if (!seqrLinkEntries.length) return <></>

    return (
        <>
            <h4> Seqr Links</h4>
            {seqrLinkEntries.map(([key, value]) => (
                <a href={value} className="ui button" key={key} target="_blank" rel="noreferrer">
                    {_.capitalize(key)}
                </a>
            ))}
        </>
    )
}

export default SeqrLinks
