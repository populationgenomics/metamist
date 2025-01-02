# pylint: disable=missing-function-docstring, too-many-public-methods, too-many-locals
from typing import Any

from db.python.layers.assay import AssayLayer
from db.python.layers.family import FamilyLayer
from db.python.layers.participant import ParticipantLayer
from db.python.layers.sample import SampleLayer
from db.python.layers.sequencing_group import SequencingGroupLayer
from models.models import PRIMARY_EXTERNAL_ORG, SampleUpsertInternal
from models.models.assay import AssayUpsertInternal
from models.models.participant import ParticipantUpsertInternal
from models.models.sequencing_group import SequencingGroupUpsertInternal
from test.test_participant import get_participant_to_insert
from test.testbase import DbIsolatedTest, run_as_sync

# @TODO would be good to add permissions testing to this, but first need a better
# way of mocking the test user and their permissions to make that possible

COMMENT_VERSION_FRAGMENT = """
    fragment CommentVersionFragment on GraphQLCommentVersion {
        content
        author
        status
        timestamp
    }
"""

COMMENT_FRAGMENT = """
    fragment CommentFragment on GraphQLComment {
        id
        parentId
        author
        content
        createdAt
        updatedAt
        status

        entity {
            ... CommentEntityFragment
        }

        versions {
            ... CommentVersionFragment
        }
    }
"""

COMMENT_ENTITY_FRAGMENT = """
    fragment CommentEntityFragment on GraphQLCommentEntity {
        __typename
        ... on GraphQLProject {
            projectId: id
            projectName: name
        }
        ... on GraphQLSample {
            sampleId: id
            sampleExternalId: externalId
        }
        ... on GraphQLSequencingGroup {
            sequencingGroupId: id
            sample {
                id
            }
        }
        ... on GraphQLAssay {
            assayId: id
            sample {
                id
                project {
                    name
                }
            }
        }
        ... on GraphQLParticipant {
            participantId:id
            participantExternalId: externalId
        }
        ... on GraphQLFamily {
            familyId: id
            familyExternalId: externalId
        }
    }
"""

DISCUSSION_FRAGMENT = """
    fragment DiscussionFragment on GraphQLDiscussion {
        directComments {
            ... CommentFragment

            thread {
                ... CommentFragment
            }
        }

        relatedComments {
            ... CommentFragment

            thread {
                ... CommentFragment
            }
        }
    }
"""


ADD_COMMENT_TO_THREAD = """
    mutation AddCommentToThread($parentId: Int!, $content: String!) {
        comment {
            addCommentToThread(parentId: $parentId, content: $content) {
                ...CommentFragment
            }
        }
    }
"""

UPDATE_COMMENT = """
    mutation UpdateComment($id: Int!, $content: String!) {
        comment {
            updateComment(id: $id, content: $content) {
                ...CommentFragment
                thread {
                    ...CommentFragment
                }
            }
        }
    }
"""

DELETE_COMMENT = """
    mutation DeleteComment($id: Int!) {
        comment {
            deleteComment(id: $id) {
                ...CommentFragment
                thread {
                    ...CommentFragment
                }
            }
        }
    }
"""

RESTORE_COMMENT = """
    mutation RestoreComment($id: Int!) {
        comment {
            restoreComment(id: $id) {
                ...CommentFragment
                thread {
                    ...CommentFragment
                }
            }
        }
    }
"""


SAMPLE_COMMENTS = """
    query SampleComments($sampleId: String!) {
        sample(id:{eq: $sampleId}) {
            id
            discussion {
                ... DiscussionFragment
            }
        }
    }
"""

SAMPLE_ADD_COMMENT = """
    mutation AddCommentToSample($id: String!, $content: String!) {
        sample {
            addComment(id: $id, content: $content) {
                ...CommentFragment
                thread {
                    ...CommentFragment
                }
            }
        }
    }
"""


PROJECT_COMMENTS = """
    query ProjectComments($projectName: String!) {
        project(name: $projectName) {
            id
            name
            discussion {
                ... DiscussionFragment
            }
        }
    }
"""

PROJECT_ADD_COMMENT = """
    mutation AddCommentToProject($id: Int!, $content: String!) {
        project {
            addComment(id: $id, content: $content) {
                ...CommentFragment

                thread {
                    ...CommentFragment
                }
            }
        }
    }
"""


ASSAY_COMMENTS = """
    query AssayComments($assayId: Int!) {
        assay(id: $assayId) {
            id
            discussion {
                ... DiscussionFragment
            }
        }
    }
"""

ASSAY_ADD_COMMENT = """
    mutation AddCommentToAssay($id: Int!, $content: String!) {
        assay {
            addComment(id: $id, content: $content) {
                ...CommentFragment

                thread {
                    ...CommentFragment
                }
            }
        }
    }
"""


PARTICIPANT_COMMENTS = """
    query ParticipantComments($participantId: Int!) {
        participant(id: $participantId) {
            id
            discussion {
                ... DiscussionFragment
            }
        }
    }
"""

PARTICIPANT_ADD_COMMENT = """
    mutation AddCommentToParticipant($id: Int!, $content: String!) {
        participant {
            addComment(id: $id, content: $content) {
                ...CommentFragment

                thread {
                    ...CommentFragment
                }
            }
        }
    }
"""


FAMILY_COMMENTS = """
    query FamilyComments($familyId: Int!) {
        family(familyId: $familyId) {
            id
            discussion {
                ... DiscussionFragment
            }
        }
    }
"""

FAMILY_ADD_COMMENT = """
    mutation AddCommentToFamily($id: Int!, $content: String!) {
        family {
            addComment(id: $id, content: $content) {
                ...CommentFragment

                thread {
                    ...CommentFragment
                }
            }
        }
    }
"""


SEQUENCING_GROUP_COMMENTS = """
    query SequencingGroupComments($sequencingGroupId: String!) {
        sequencingGroups(id:{eq: $sequencingGroupId}) {
            id
            discussion {
                ... DiscussionFragment
            }
        }
    }
"""

SEQUENCING_GROUP_ADD_COMMENT = """
    mutation AddCommentToSequencingGroup($id: String!, $content: String!) {
        sequencingGroup {
            addComment(id: $id, content: $content) {
                ...CommentFragment

                thread {
                    ...CommentFragment
                }
            }
        }
    }
"""


COMMENT_RESULT_FRAGMENTS = f"""
    {COMMENT_VERSION_FRAGMENT}
    {COMMENT_ENTITY_FRAGMENT}
    {COMMENT_FRAGMENT}
"""

DISCUSSION_RESULT_FRAGMENTS = f"""
    {COMMENT_VERSION_FRAGMENT}
    {COMMENT_ENTITY_FRAGMENT}
    {COMMENT_FRAGMENT}
    {DISCUSSION_FRAGMENT}
"""

default_sequencing_meta = {
    'sequencing_type': 'genome',
    'sequencing_platform': 'short-read',
    'sequencing_technology': 'illumina',
}


def comment_uniq(comment: dict[str, Any]):
    return (comment['id'], comment['entity']['__typename'])


class TestComment(DbIsolatedTest):
    """Test commenting functionality"""

    async def add_comment_to_entity(
        self, comment_query: str, entity_id: str | int, content: str
    ) -> dict[str, Any]:
        create_comment_query = f"""
            {COMMENT_RESULT_FRAGMENTS}
            {comment_query}
        """

        create_comment_variables = {
            'id': entity_id,
            'content': content,
        }

        created_comment = await self.run_graphql_query_async(
            create_comment_query, variables=create_comment_variables
        )
        return created_comment

    async def get_discussion_on_entity(
        self, comment_query: str, variables: dict[str, int | str]
    ) -> dict[str, Any]:
        get_discussion_query = f"""
            {DISCUSSION_RESULT_FRAGMENTS}
            {comment_query}
        """

        requested_comment = await self.run_graphql_query_async(
            get_discussion_query, variables=variables
        )
        return requested_comment

    async def add_comment_to_sample(self, sample_id: str, comment: str):
        result = await self.add_comment_to_entity(
            SAMPLE_ADD_COMMENT, sample_id, comment
        )
        return result['sample']['addComment']

    async def get_discussion_on_sample(self, sample_id: str):
        result = await self.get_discussion_on_entity(
            SAMPLE_COMMENTS, {'sampleId': sample_id}
        )
        return result['sample'][0]['discussion']

    async def add_comment_to_project(self, project_id: int, comment: str):
        result = await self.add_comment_to_entity(
            PROJECT_ADD_COMMENT, project_id, comment
        )
        return result['project']['addComment']

    async def get_discussion_on_project(self, project_name: str):
        result = await self.get_discussion_on_entity(
            PROJECT_COMMENTS, {'projectName': project_name}
        )
        return result['project']['discussion']

    async def add_comment_to_assay(self, assay_id: int, comment: str):
        result = await self.add_comment_to_entity(ASSAY_ADD_COMMENT, assay_id, comment)
        return result['assay']['addComment']

    async def get_discussion_on_assay(self, assay_id: int):
        result = await self.get_discussion_on_entity(
            ASSAY_COMMENTS, {'assayId': assay_id}
        )
        return result['assay']['discussion']

    async def add_comment_to_participant(self, participant_id: int, comment: str):
        result = await self.add_comment_to_entity(
            PARTICIPANT_ADD_COMMENT, participant_id, comment
        )
        return result['participant']['addComment']

    async def get_discussion_on_participant(self, participant_id: int):
        result = await self.get_discussion_on_entity(
            PARTICIPANT_COMMENTS, {'participantId': participant_id}
        )
        return result['participant']['discussion']

    async def add_comment_to_family(self, family_id: int, comment: str):
        result = await self.add_comment_to_entity(
            FAMILY_ADD_COMMENT, family_id, comment
        )
        return result['family']['addComment']

    async def get_discussion_on_family(self, family_id: int):
        result = await self.get_discussion_on_entity(
            FAMILY_COMMENTS, {'familyId': family_id}
        )
        return result['family']['discussion']

    async def add_comment_to_sequencing_group(
        self, sequencing_group_id: str, comment: str
    ):
        result = await self.add_comment_to_entity(
            SEQUENCING_GROUP_ADD_COMMENT, sequencing_group_id, comment
        )
        return result['sequencingGroup']['addComment']

    async def get_discussion_on_sequencing_group(self, sequencing_group_id: int):
        result = await self.get_discussion_on_entity(
            SEQUENCING_GROUP_COMMENTS, {'sequencingGroupId': sequencing_group_id}
        )
        return result['sequencingGroups'][0]['discussion']

    async def add_comment_to_thread(
        self, parent_id: int, content: str
    ) -> dict[str, Any]:
        add_comment_to_thread_query = f"""
            {COMMENT_RESULT_FRAGMENTS}
            {ADD_COMMENT_TO_THREAD}
        """

        add_comment_to_thread_variables = {
            'parentId': parent_id,
            'content': content,
        }

        child_comment_result = await self.run_graphql_query_async(
            add_comment_to_thread_query, variables=add_comment_to_thread_variables
        )
        return child_comment_result['comment']['addCommentToThread']

    async def update_comment(self, id_: int, content: str) -> dict[str, Any]:
        update_comment_query = f"""
            {COMMENT_RESULT_FRAGMENTS}
            {UPDATE_COMMENT}
        """

        update_comment_variables = {
            'id': id_,
            'content': content,
        }

        update_comment_result = await self.run_graphql_query_async(
            update_comment_query, variables=update_comment_variables
        )
        return update_comment_result['comment']['updateComment']

    async def delete_comment(self, id_: int) -> dict[str, Any]:
        delete_comment_query = f"""
            {COMMENT_RESULT_FRAGMENTS}
            {DELETE_COMMENT}
        """

        delete_comment_variables = {'id': id_}

        delete_comment_result = await self.run_graphql_query_async(
            delete_comment_query, variables=delete_comment_variables
        )
        return delete_comment_result['comment']['deleteComment']

    async def restore_comment(self, id_: int) -> dict[str, Any]:
        restore_comment_query = f"""
            {COMMENT_RESULT_FRAGMENTS}
            {RESTORE_COMMENT}
        """

        restore_comment_variables = {'id': id_}

        restore_comment_result = await self.run_graphql_query_async(
            restore_comment_query, variables=restore_comment_variables
        )
        return restore_comment_result['comment']['restoreComment']

    # tests run in 'sorted by ascii' order
    @run_as_sync
    async def setUp(self) -> None:
        super().setUp()

        self.slayer = SampleLayer(self.connection)
        self.assaylayer = AssayLayer(self.connection)
        self.player = ParticipantLayer(self.connection)
        self.flayer = FamilyLayer(self.connection)
        self.sglayer = SequencingGroupLayer(self.connection)

    @run_as_sync
    async def test_add_comment_to_sample(self):
        """Test adding a comment to a sample"""

        sample = await self.slayer.upsert_sample(
            SampleUpsertInternal(
                external_ids={PRIMARY_EXTERNAL_ORG: 'Test01'},
                type='blood',
                active=True,
                meta={'meta': 'meta ;)'},
            )
        )

        sample_external = sample.to_external()

        comment_text = 'Sample Test Comment 1234'
        created_comment = await self.add_comment_to_sample(
            sample_external.id, comment_text
        )

        # Ensure that created comment has expected data
        self.assertEqual(created_comment['content'], comment_text)
        self.assertEqual(created_comment['status'], 'active')
        self.assertEqual(created_comment['entity']['sampleId'], sample_external.id)

        discussion = await self.get_discussion_on_sample(sample_external.id)

        # Ensure that comment can be requested after creating it, and that it is the same
        self.assertEqual(created_comment, discussion['directComments'][0])

    @run_as_sync
    async def test_add_comment_to_project(self):
        """Test adding a comment to a project"""

        project = self.connection.all_projects()[0]

        comment_text = 'Project Test Comment 1234'

        created_comment = await self.add_comment_to_project(project.id, comment_text)

        self.assertEqual(created_comment['content'], comment_text)
        self.assertEqual(created_comment['status'], 'active')
        self.assertEqual(created_comment['entity']['projectId'], project.id)

        requested_comment = await self.get_discussion_on_project(project.name)

        # Ensure that comment can be requested after creating it, and that it is the same
        self.assertEqual(
            created_comment,
            requested_comment['directComments'][0],
        )

    @run_as_sync
    async def test_add_comment_to_assay(self):
        """Test adding a comment to an assay"""

        sample = await self.slayer.upsert_sample(
            SampleUpsertInternal(
                external_ids={PRIMARY_EXTERNAL_ORG: 'Test01'},
                type='blood',
                active=True,
                meta={'meta': 'meta ;)'},
            )
        )

        assay = await self.assaylayer.upsert_assay(
            assay=AssayUpsertInternal(
                sample_id=sample.id, meta=default_sequencing_meta, type='sequencing'
            )
        )
        assert assay.id

        comment_text = 'Assay Test Comment 1234'

        created_comment = await self.add_comment_to_assay(assay.id, comment_text)

        self.assertEqual(created_comment['content'], comment_text)
        self.assertEqual(created_comment['status'], 'active')
        self.assertEqual(created_comment['entity']['assayId'], assay.id)

        requested_comment = await self.get_discussion_on_assay(assay.id)

        # Ensure that comment can be requested after creating it, and that it is the same
        self.assertEqual(created_comment, requested_comment['directComments'][0])

    @run_as_sync
    async def test_add_comment_to_participant(self):
        """Test adding a comment to an participant"""

        participant = await self.player.upsert_participant(
            participant=ParticipantUpsertInternal(
                external_ids={PRIMARY_EXTERNAL_ORG: 'p_external_id'},
                meta={'pmeta': 'pvalue'},
                reported_sex=2,
                reported_gender='FEMALE',
                karyotype='XX',
            )
        )
        assert participant.id

        comment_text = 'Participant Test Comment 1234'

        created_comment = await self.add_comment_to_participant(
            participant.id, comment_text
        )

        self.assertEqual(created_comment['content'], comment_text)
        self.assertEqual(created_comment['status'], 'active')
        self.assertEqual(created_comment['entity']['participantId'], participant.id)

        requested_comment = await self.get_discussion_on_participant(participant.id)

        # Ensure that comment can be requested after creating it, and that it is the same
        self.assertEqual(created_comment, requested_comment['directComments'][0])

    @run_as_sync
    async def test_add_comment_to_family(self):
        """Test adding a comment to an family"""

        family = await self.flayer.create_family(
            external_ids={PRIMARY_EXTERNAL_ORG: 'f_external_id'},
        )

        comment_text = 'Family Test Comment 1234'
        created_comment = await self.add_comment_to_family(family, comment_text)

        self.assertEqual(created_comment['content'], comment_text)
        self.assertEqual(created_comment['status'], 'active')
        self.assertEqual(created_comment['entity']['familyId'], family)

        requested_comment = await self.get_discussion_on_family(family)

        # Ensure that comment can be requested after creating it, and that it is the same
        self.assertEqual(created_comment, requested_comment['directComments'][0])

    @run_as_sync
    async def test_add_comment_to_sequencing_group(self):
        """Test adding a comment to an sequencing_group"""

        sample = await self.slayer.upsert_sample(
            SampleUpsertInternal(
                external_ids={PRIMARY_EXTERNAL_ORG: 'Test01'},
                type='blood',
                active=True,
                meta={'meta': 'meta ;)'},
            )
        )

        sequencing_groups = await self.sglayer.upsert_sequencing_groups(
            sequencing_groups=[
                SequencingGroupUpsertInternal(
                    sample_id=sample.id,
                    type='mtseq',
                    technology='short-read',
                    platform='oxford-nanopore',
                    assays=[
                        AssayUpsertInternal(
                            sample_id=sample.id,
                            meta=default_sequencing_meta,
                            type='sequencing',
                        )
                    ],
                )
            ]
        )
        sequencing_group = sequencing_groups[0]
        sequencing_group_id = sequencing_group.to_external().id
        assert sequencing_group_id

        comment_text = 'SequencingGroup Test Comment 1234'

        created_comment = await self.add_comment_to_sequencing_group(
            sequencing_group_id, comment_text
        )

        self.assertEqual(created_comment['content'], comment_text)
        self.assertEqual(created_comment['status'], 'active')
        self.assertEqual(
            created_comment['entity']['sequencingGroupId'],
            sequencing_group_id,
        )

        requested_comment = await self.get_discussion_on_sequencing_group(
            sequencing_group_id
        )

        # Ensure that comment can be requested after creating it, and that it is the same
        self.assertEqual(created_comment, requested_comment['directComments'][0])

    @run_as_sync
    async def test_add_comment_to_thread(self):
        """Test adding a comment to a thread"""

        sample = await self.slayer.upsert_sample(
            SampleUpsertInternal(
                external_ids={PRIMARY_EXTERNAL_ORG: 'Test01'},
                type='blood',
                active=True,
                meta={'meta': 'meta ;)'},
            )
        )

        sample_external = sample.to_external()
        comment_text = 'Sample parent comment'

        parent_comment = await self.add_comment_to_sample(
            sample_external.id, comment_text
        )
        parent_id = parent_comment['id']

        child_comment = await self.add_comment_to_thread(
            parent_id=parent_id, content='Child comment in thread'
        )

        # Ensure that the child comment has the correct parent ID
        self.assertEqual(child_comment['parentId'], parent_id)

        requested_comment = await self.get_discussion_on_sample(sample_external.id)

        parent_comment_with_thread = requested_comment['directComments'][0]
        self.assertEqual(len(parent_comment_with_thread['thread']), 1)
        self.assertEqual(parent_comment_with_thread['thread'][0], child_comment)

    @run_as_sync
    async def test_updating_comment(self):
        """Test updating an existing parent comment"""

        sample = await self.slayer.upsert_sample(
            SampleUpsertInternal(
                external_ids={PRIMARY_EXTERNAL_ORG: 'Test01'},
                type='blood',
                active=True,
                meta={'meta': 'meta ;)'},
            )
        )

        sample_external = sample.to_external()
        initial_parent_comment_text = 'Sample parent comment'

        parent_comment = await self.add_comment_to_sample(
            sample_external.id, initial_parent_comment_text
        )

        initial_child_comment_text = 'Sample child comment'

        child_comment = await self.add_comment_to_thread(
            parent_id=parent_comment['id'], content=initial_child_comment_text
        )

        updated_parent_comment_text = 'Updated Parent Comment Text'
        updated_child_comment_text = 'Updated Child Comment Text'

        # Update child comment first so that we can check that parent comment update
        # contains the updated child comment
        updated_child_comment = await self.update_comment(
            child_comment['id'], updated_child_comment_text
        )

        updated_parent_comment = await self.update_comment(
            parent_comment['id'], updated_parent_comment_text
        )

        sample_discussion = await self.get_discussion_on_sample(sample_external.id)

        self.assertEqual(updated_child_comment['content'], updated_child_comment_text)
        self.assertEqual(updated_parent_comment['content'], updated_parent_comment_text)
        self.assertEqual(
            updated_parent_comment['versions'][0]['content'],
            initial_parent_comment_text,
        )
        self.assertEqual(len(updated_parent_comment['versions']), 1)
        self.assertEqual(len(updated_child_comment['versions']), 1)
        self.assertEqual(
            updated_child_comment['versions'][0]['content'], initial_child_comment_text
        )
        self.assertEqual(
            # The child in the updated parent comment thread won't have a thread of its
            # own, so add it in here, but they otherwise should be identical
            updated_parent_comment['thread'][0] | {'thread': []},
            updated_child_comment,
        )
        self.assertEqual(sample_discussion['directComments'][0], updated_parent_comment)

    @run_as_sync
    async def test_deleting_and_restoring_comment(self):
        """Test deleting a comment and then restoring it"""

        sample = await self.slayer.upsert_sample(
            SampleUpsertInternal(
                external_ids={PRIMARY_EXTERNAL_ORG: 'Test01'},
                type='blood',
                active=True,
                meta={'meta': 'meta ;)'},
            )
        )

        sample_external = sample.to_external()
        comment_text = 'Sample parent comment'

        created_comment = await self.add_comment_to_sample(
            sample_external.id, comment_text
        )
        self.assertEqual(created_comment['status'], 'active')

        deleted_comment = await self.delete_comment(created_comment['id'])

        self.assertEqual(deleted_comment['status'], 'deleted')
        self.assertEqual(len(deleted_comment['versions']), 1)

        restored_comment = await self.restore_comment(created_comment['id'])

        self.assertEqual(restored_comment['status'], 'active')
        self.assertEqual(len(restored_comment['versions']), 2)

    @run_as_sync
    async def test_sample_discussion_related_comments(self):
        """Test getting related comments"""

        family_id = await self.flayer.create_family(
            external_ids={PRIMARY_EXTERNAL_ORG: 'f_external_id'},
        )
        # This will create participant, sample, assay, sequencing group
        participant = await self.player.upsert_participant(get_participant_to_insert())

        sample = participant.samples[0]
        sequencing_group = sample.sequencing_groups[0]
        assay = sequencing_group.assays[0]
        project_id = self.connection.project_id
        assert project_id
        project_name = self.connection.project_id_map[project_id].name

        pat_pid = (
            await self.player.upsert_participant(
                ParticipantUpsertInternal(
                    external_ids={PRIMARY_EXTERNAL_ORG: 'EX01_pat'}, reported_sex=1
                )
            )
        ).id

        mat_pid = (
            await self.player.upsert_participant(
                ParticipantUpsertInternal(
                    external_ids={PRIMARY_EXTERNAL_ORG: 'EX01_mat'}, reported_sex=2
                )
            )
        ).id

        await self.player.add_participant_to_family(
            family_id=family_id,
            participant_id=participant.id,
            paternal_id=pat_pid,
            maternal_id=mat_pid,
            affected=2,
        )

        sample_comment = await self.add_comment_to_sample(
            sample.to_external().id, 'Sample Comment'
        )

        assay_comment = await self.add_comment_to_assay(assay.id, 'Assay Comment')

        family_comment = await self.add_comment_to_family(family_id, 'Family Comment')

        participant_comment = await self.add_comment_to_participant(
            participant.id, 'Participant Comment'
        )

        project_comment = await self.add_comment_to_project(
            self.connection.project_id, 'Project Comment'
        )

        sequencing_group_comment = await self.add_comment_to_sequencing_group(
            sequencing_group.to_external().id, 'Sg Comment'
        )

        all_comments = [
            (sample_comment, 'Sample'),
            (assay_comment, 'Assay'),
            (family_comment, 'Family'),
            (participant_comment, 'Participant'),
            (project_comment, 'Project'),
            (sequencing_group_comment, 'Sequencing Group'),
        ]

        # add some threaded comments to test those
        for comment, entity in all_comments:
            await self.add_comment_to_thread(
                parent_id=comment['id'], content=f'{entity} threaded comment'
            )

        sample_discussion = await self.get_discussion_on_sample(sample.to_external().id)
        assay_discussion = await self.get_discussion_on_assay(assay.id)
        family_discussion = await self.get_discussion_on_family(family_id)
        participant_discussion = await self.get_discussion_on_participant(
            participant.id
        )
        project_discussion = await self.get_discussion_on_project(project_name)
        sequencing_group_discussion = await self.get_discussion_on_sequencing_group(
            sequencing_group.to_external().id
        )

        # Sample relatedness
        sample_related_ids = set(
            comment_uniq(d) for d in sample_discussion['relatedComments']
        )
        self.assertEqual(len(sample_discussion['directComments']), 1)
        self.assertEqual(len(sample_discussion['relatedComments']), 4)
        self.assertIn(comment_uniq(family_comment), sample_related_ids)
        self.assertIn(comment_uniq(assay_comment), sample_related_ids)
        self.assertIn(comment_uniq(participant_comment), sample_related_ids)
        self.assertIn(comment_uniq(sequencing_group_comment), sample_related_ids)
        self.assertNotIn(comment_uniq(project_comment), sample_related_ids)

        # assay relatedness
        assay_related_ids = set(
            comment_uniq(d) for d in assay_discussion['relatedComments']
        )
        self.assertEqual(len(assay_discussion['directComments']), 1)
        self.assertEqual(len(assay_discussion['relatedComments']), 4)
        self.assertIn(comment_uniq(family_comment), assay_related_ids)
        self.assertIn(comment_uniq(sample_comment), assay_related_ids)
        self.assertIn(comment_uniq(participant_comment), assay_related_ids)
        self.assertIn(comment_uniq(sequencing_group_comment), assay_related_ids)
        self.assertNotIn(comment_uniq(project_comment), assay_related_ids)

        # family relatedness
        family_related_ids = set(
            comment_uniq(d) for d in family_discussion['relatedComments']
        )
        self.assertEqual(len(family_discussion['directComments']), 1)
        self.assertEqual(len(family_discussion['relatedComments']), 4)
        self.assertIn(comment_uniq(assay_comment), family_related_ids)
        self.assertIn(comment_uniq(sample_comment), family_related_ids)
        self.assertIn(comment_uniq(participant_comment), family_related_ids)
        self.assertIn(comment_uniq(sequencing_group_comment), family_related_ids)
        self.assertNotIn(comment_uniq(project_comment), family_related_ids)

        # participant relatedness
        participant_related_ids = set(
            comment_uniq(d) for d in participant_discussion['relatedComments']
        )
        self.assertEqual(len(participant_discussion['directComments']), 1)
        self.assertEqual(len(participant_discussion['relatedComments']), 4)
        self.assertIn(comment_uniq(assay_comment), participant_related_ids)
        self.assertIn(comment_uniq(sample_comment), participant_related_ids)
        self.assertIn(comment_uniq(family_comment), participant_related_ids)
        self.assertIn(comment_uniq(sequencing_group_comment), participant_related_ids)
        self.assertNotIn(comment_uniq(project_comment), participant_related_ids)

        # project relatedness
        project_related_ids = set(
            comment_uniq(d) for d in project_discussion['relatedComments']
        )
        self.assertEqual(len(project_discussion['directComments']), 1)
        self.assertEqual(len(project_discussion['relatedComments']), 5)
        self.assertIn(comment_uniq(assay_comment), project_related_ids)
        self.assertIn(comment_uniq(participant_comment), project_related_ids)
        self.assertIn(comment_uniq(sample_comment), project_related_ids)
        self.assertIn(comment_uniq(family_comment), project_related_ids)
        self.assertIn(comment_uniq(sequencing_group_comment), project_related_ids)

        # sg relatedness
        sequencing_group_related_ids = set(
            comment_uniq(d) for d in sequencing_group_discussion['relatedComments']
        )
        self.assertEqual(len(sequencing_group_discussion['directComments']), 1)
        self.assertEqual(len(sequencing_group_discussion['relatedComments']), 4)
        self.assertIn(comment_uniq(assay_comment), sequencing_group_related_ids)
        self.assertIn(comment_uniq(sample_comment), sequencing_group_related_ids)
        self.assertIn(comment_uniq(family_comment), sequencing_group_related_ids)
        self.assertIn(comment_uniq(participant_comment), sequencing_group_related_ids)
        self.assertNotIn(comment_uniq(project_comment), sequencing_group_related_ids)
