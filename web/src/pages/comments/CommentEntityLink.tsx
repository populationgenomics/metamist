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
                <>
                    on Assay{' '}
                    <AssayLink
                        id={comment.entity.assayId}
                        project={comment.entity.sample.project.name}
                    />{' '}
                    of Sample <SampleLink id={comment.entity.sample.id} />
                </>
            )

        case 'GraphQLFamily':
            return (
                <>
                    on Family{' '}
                    <FamilyLink id={comment.entity.familyId}>
                        {comment.entity.familyId} ({comment.entity.familyExternalId})
                    </FamilyLink>
                </>
            )

        case 'GraphQLParticipant':
            return (
                <>
                    on Participant{' '}
                    <ParticipantLink id={comment.entity.participantId}>
                        {comment.entity.participantId} ({comment.entity.participantExternalId})
                    </ParticipantLink>
                </>
            )

        case 'GraphQLProject':
            return (
                <>
                    on Project <ProjectLink name={comment.entity.projectName} />
                </>
            )

        case 'GraphQLSample':
            return (
                <>
                    on Sample{' '}
                    <SampleLink id={comment.entity.sampleId}>
                        {comment.entity.sampleId} ({comment.entity.sampleExternalId})
                    </SampleLink>
                </>
            )
        case 'GraphQLSequencingGroup':
            return (
                <SequencingGroupLink
                    sampleId={comment.entity.sample.id}
                    id={comment.entity.sequencingGroupId}
                />
            )
    }
}
