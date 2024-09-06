from db.python.layers.assay import AssayLayer
from db.python.layers.family import FamilyLayer
from db.python.layers.participant import ParticipantLayer
from db.python.layers.sample import SampleLayer
from db.python.layers.sequencing_group import SequencingGroupLayer
from models.models import PRIMARY_EXTERNAL_ORG, SampleUpsertInternal
from models.models.assay import AssayUpsertInternal
from models.models.participant import ParticipantUpsertInternal
from models.models.sequencing_group import SequencingGroupUpsertInternal
from test.testbase import DbIsolatedTest, run_as_sync

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


class TestComment(DbIsolatedTest):
    """Test commenting functionality"""

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

        create_comment_query = f"""
            {COMMENT_RESULT_FRAGMENTS}
            {SAMPLE_ADD_COMMENT}
        """

        create_comment_variables = {'id': sample_external.id, 'content': comment_text}
        created_comment = await self.run_graphql_query_async(
            create_comment_query, variables=create_comment_variables
        )

        # Ensure that created comment has expected data
        self.assertEqual(
            created_comment['sample']['addComment']['content'], comment_text
        )
        self.assertEqual(created_comment['sample']['addComment']['status'], 'active')
        self.assertEqual(
            created_comment['sample']['addComment']['entity']['sampleId'],
            sample_external.id,
        )

        get_comment_query = f"""
            {DISCUSSION_RESULT_FRAGMENTS}
            {SAMPLE_COMMENTS}
        """

        get_comment_variables = {'sampleId': sample_external.id}
        requested_comment = await self.run_graphql_query_async(
            get_comment_query, variables=get_comment_variables
        )

        # Ensure that comment can be requested after creating it, and that it is the same
        self.assertEqual(
            created_comment['sample']['addComment'],
            requested_comment['sample'][0]['discussion']['directComments'][0],
        )

    @run_as_sync
    async def test_add_comment_to_project(self):
        """Test adding a comment to a project"""

        project = self.connection.all_projects()[0]

        comment_text = 'Project Test Comment 1234'

        create_comment_query = f"""
            {COMMENT_RESULT_FRAGMENTS}
            {PROJECT_ADD_COMMENT}
        """

        create_comment_variables = {'id': project.id, 'content': comment_text}

        created_comment = await self.run_graphql_query_async(
            create_comment_query, variables=create_comment_variables
        )

        self.assertEqual(
            created_comment['project']['addComment']['content'], comment_text
        )
        self.assertEqual(created_comment['project']['addComment']['status'], 'active')
        self.assertEqual(
            created_comment['project']['addComment']['entity']['projectId'], project.id
        )

        get_comment_query = f"""
            {DISCUSSION_RESULT_FRAGMENTS}
            {PROJECT_COMMENTS}
        """

        get_comment_variables = {'projectName': project.name}
        requested_comment = await self.run_graphql_query_async(
            get_comment_query, variables=get_comment_variables
        )

        # Ensure that comment can be requested after creating it, and that it is the same
        self.assertEqual(
            created_comment['project']['addComment'],
            requested_comment['project']['discussion']['directComments'][0],
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

        comment_text = 'Assay Test Comment 1234'

        create_comment_query = f"""
            {COMMENT_RESULT_FRAGMENTS}
            {ASSAY_ADD_COMMENT}
        """

        create_comment_variables = {'id': assay.id, 'content': comment_text}

        created_comment = await self.run_graphql_query_async(
            create_comment_query, variables=create_comment_variables
        )

        self.assertEqual(
            created_comment['assay']['addComment']['content'], comment_text
        )
        self.assertEqual(created_comment['assay']['addComment']['status'], 'active')
        self.assertEqual(
            created_comment['assay']['addComment']['entity']['assayId'], assay.id
        )

        get_comment_query = f"""
            {DISCUSSION_RESULT_FRAGMENTS}
            {ASSAY_COMMENTS}
        """

        get_comment_variables = {'assayId': assay.id}
        requested_comment = await self.run_graphql_query_async(
            get_comment_query, variables=get_comment_variables
        )

        # Ensure that comment can be requested after creating it, and that it is the same
        self.assertEqual(
            created_comment['assay']['addComment'],
            requested_comment['assay']['discussion']['directComments'][0],
        )

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

        comment_text = 'Participant Test Comment 1234'

        create_comment_query = f"""
            {COMMENT_RESULT_FRAGMENTS}
            {PARTICIPANT_ADD_COMMENT}
        """

        create_comment_variables = {'id': participant.id, 'content': comment_text}

        created_comment = await self.run_graphql_query_async(
            create_comment_query, variables=create_comment_variables
        )

        self.assertEqual(
            created_comment['participant']['addComment']['content'], comment_text
        )
        self.assertEqual(
            created_comment['participant']['addComment']['status'], 'active'
        )
        self.assertEqual(
            created_comment['participant']['addComment']['entity']['participantId'],
            participant.id,
        )

        get_comment_query = f"""
            {DISCUSSION_RESULT_FRAGMENTS}
            {PARTICIPANT_COMMENTS}
        """

        get_comment_variables = {'participantId': participant.id}
        requested_comment = await self.run_graphql_query_async(
            get_comment_query, variables=get_comment_variables
        )

        # Ensure that comment can be requested after creating it, and that it is the same
        self.assertEqual(
            created_comment['participant']['addComment'],
            requested_comment['participant']['discussion']['directComments'][0],
        )

    @run_as_sync
    async def test_add_comment_to_family(self):
        """Test adding a comment to an family"""

        family = await self.flayer.create_family(external_id='f_external_id')

        comment_text = 'Family Test Comment 1234'

        create_comment_query = f"""
            {COMMENT_RESULT_FRAGMENTS}
            {FAMILY_ADD_COMMENT}
        """

        create_comment_variables = {'id': family, 'content': comment_text}

        created_comment = await self.run_graphql_query_async(
            create_comment_query, variables=create_comment_variables
        )

        self.assertEqual(
            created_comment['family']['addComment']['content'], comment_text
        )
        self.assertEqual(created_comment['family']['addComment']['status'], 'active')
        self.assertEqual(
            created_comment['family']['addComment']['entity']['familyId'],
            family,
        )

        get_comment_query = f"""
            {DISCUSSION_RESULT_FRAGMENTS}
            {FAMILY_COMMENTS}
        """

        get_comment_variables = {'familyId': family}
        requested_comment = await self.run_graphql_query_async(
            get_comment_query, variables=get_comment_variables
        )

        # Ensure that comment can be requested after creating it, and that it is the same
        self.assertEqual(
            created_comment['family']['addComment'],
            requested_comment['family']['discussion']['directComments'][0],
        )

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

        comment_text = 'SequencingGroup Test Comment 1234'

        create_comment_query = f"""
            {COMMENT_RESULT_FRAGMENTS}
            {SEQUENCING_GROUP_ADD_COMMENT}
        """

        create_comment_variables = {
            'id': sequencing_group.to_external().id,
            'content': comment_text,
        }

        created_comment = await self.run_graphql_query_async(
            create_comment_query, variables=create_comment_variables
        )

        self.assertEqual(
            created_comment['sequencingGroup']['addComment']['content'], comment_text
        )
        self.assertEqual(
            created_comment['sequencingGroup']['addComment']['status'], 'active'
        )
        self.assertEqual(
            created_comment['sequencingGroup']['addComment']['entity'][
                'sequencingGroupId'
            ],
            sequencing_group.to_external().id,
        )

        get_comment_query = f"""
            {DISCUSSION_RESULT_FRAGMENTS}
            {SEQUENCING_GROUP_COMMENTS}
        """

        get_comment_variables = {'sequencingGroupId': sequencing_group.to_external().id}
        requested_comment = await self.run_graphql_query_async(
            get_comment_query, variables=get_comment_variables
        )

        # Ensure that comment can be requested after creating it, and that it is the same
        self.assertEqual(
            created_comment['sequencingGroup']['addComment'],
            requested_comment['sequencingGroups'][0]['discussion']['directComments'][0],
        )

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

        create_comment_query = f"""
            {COMMENT_RESULT_FRAGMENTS}
            {SAMPLE_ADD_COMMENT}
        """

        create_comment_variables = {'id': sample_external.id, 'content': comment_text}
        parent_comment = await self.run_graphql_query_async(
            create_comment_query, variables=create_comment_variables
        )

        add_comment_to_thread_query = f"""
            {COMMENT_RESULT_FRAGMENTS}
            {ADD_COMMENT_TO_THREAD}
        """
        parent_id = parent_comment['sample']['addComment']['id']

        add_comment_to_thread_variables = {
            'parentId': parent_id,
            'content': 'Child comment in thread',
        }

        child_comment_result = await self.run_graphql_query_async(
            add_comment_to_thread_query, variables=add_comment_to_thread_variables
        )
        child_comment = child_comment_result['comment']['addCommentToThread']

        # Ensure that the child comment has the correct parent ID
        self.assertEqual(child_comment['parentId'], parent_id)

        get_comment_query = f"""
            {DISCUSSION_RESULT_FRAGMENTS}
            {SAMPLE_COMMENTS}
        """

        get_comment_variables = {'sampleId': sample_external.id}
        requested_comment = await self.run_graphql_query_async(
            get_comment_query, variables=get_comment_variables
        )

        parent_comment_with_thread = requested_comment['sample'][0]['discussion'][
            'directComments'
        ][0]

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

        create_comment_query = f"""
            {COMMENT_RESULT_FRAGMENTS}
            {SAMPLE_ADD_COMMENT}
        """

        create_comment_variables = {
            'id': sample_external.id,
            'content': initial_parent_comment_text,
        }
        parent_comment_result = await self.run_graphql_query_async(
            create_comment_query, variables=create_comment_variables
        )

        parent_comment = parent_comment_result['sample']['addComment']

        add_comment_to_thread_query = f"""
            {COMMENT_RESULT_FRAGMENTS}
            {ADD_COMMENT_TO_THREAD}
        """
        initial_child_comment_text = 'Child comment in thread'
        add_comment_to_thread_variables = {
            'parentId': parent_comment['id'],
            'content': initial_child_comment_text,
        }

        child_comment_result = await self.run_graphql_query_async(
            add_comment_to_thread_query, variables=add_comment_to_thread_variables
        )

        child_comment = child_comment_result['comment']['addCommentToThread']

        updated_parent_comment_text = 'Updated Parent Comment Text'
        updated_child_comment_text = 'Updated Child Comment Text'

        update_comment_query = f"""
            {COMMENT_RESULT_FRAGMENTS}
            {UPDATE_COMMENT}
        """
        # Update child comment first so that we can check that parent comment update
        # contains the updated child comment
        update_child_comment_result = await self.run_graphql_query_async(
            update_comment_query,
            variables={
                'id': child_comment['id'],
                'content': updated_child_comment_text,
            },
        )

        updated_child_comment = update_child_comment_result['comment']['updateComment']

        update_parent_comment_result = await self.run_graphql_query_async(
            update_comment_query,
            variables={
                'id': parent_comment['id'],
                'content': updated_parent_comment_text,
            },
        )

        updated_parent_comment = update_parent_comment_result['comment'][
            'updateComment'
        ]

        sample_discussion_query = f"""
            {DISCUSSION_RESULT_FRAGMENTS}
            {SAMPLE_COMMENTS}
        """

        sample_discussion_result = await self.run_graphql_query_async(
            sample_discussion_query, variables={'sampleId': sample_external.id}
        )

        sample_discussion = sample_discussion_result['sample'][0]['discussion']

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

        create_comment_query = f"""
            {COMMENT_RESULT_FRAGMENTS}
            {SAMPLE_ADD_COMMENT}
        """

        create_comment_variables = {'id': sample_external.id, 'content': comment_text}
        create_comment_result = await self.run_graphql_query_async(
            create_comment_query, variables=create_comment_variables
        )

        created_comment = create_comment_result['sample']['addComment']
        self.assertEqual(created_comment['status'], 'active')

        delete_comment_query = f"""
            {COMMENT_RESULT_FRAGMENTS}
            {DELETE_COMMENT}
        """

        deleted_comment_result = await self.run_graphql_query_async(
            delete_comment_query, variables={'id': created_comment['id']}
        )

        deleted_comment = deleted_comment_result['comment']['deleteComment']

        self.assertEqual(deleted_comment['status'], 'deleted')
        self.assertEqual(len(deleted_comment['versions']), 1)

        restore_comment_query = f"""
            {COMMENT_RESULT_FRAGMENTS}
            {RESTORE_COMMENT}
        """

        restore_comment_result = await self.run_graphql_query_async(
            restore_comment_query, variables={'id': created_comment['id']}
        )

        restored_comment = restore_comment_result['comment']['restoreComment']

        self.assertEqual(restored_comment['status'], 'active')
        self.assertEqual(len(restored_comment['versions']), 2)
