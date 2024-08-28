import AssayLink from '../../shared/components/links/AssayLink'
import FamilyLink from '../../shared/components/links/FamilyLink'
import ParticipantLink from '../../shared/components/links/ParticipantLink'
import ProjectLink from '../../shared/components/links/ProjectLink'
import SampleLink from '../../shared/components/links/SampleLink'
import SequencingGroupLink from '../../shared/components/links/SequencingGroupLink'
import { CommentThreadData } from './commentConfig'

export function CommentEntityLink(props: { comment: CommentThreadData }) {
    const { comment } = props
    switch (comment.entity.__typename) {
        case 'GraphQLAssay':
            return (
                <AssayLink
                    id={comment.entity.assayId}
                    project={comment.entity.sample.project.name}
                />
            )

        case 'GraphQLFamily':
            return <FamilyLink id={comment.entity.familyId} />

        case 'GraphQLParticipant':
            return <ParticipantLink id={comment.entity.participantId} />

        case 'GraphQLProject':
            return <ProjectLink name={comment.entity.projectName} />

        case 'GraphQLSample':
            return <SampleLink id={comment.entity.sampleId} />
        case 'GraphQLSequencingGroup':
            return (
                <SequencingGroupLink
                    sampleId={comment.entity.sample.id}
                    id={comment.entity.sequencingGroupId}
                />
            )
    }
}
