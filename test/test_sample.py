import unittest

import pytest

from db.python.connect import Connection
from db.python.filters.generic import GenericFilter
from db.python.filters.sample import SampleFilter
from db.python.filters.sequencing_group import SequencingGroupFilter
from db.python.layers.assay import AssayLayer
from db.python.layers.sample import SampleLayer
from db.python.layers.sequencing_group import SequencingGroupLayer
from models.models import (
    PRIMARY_EXTERNAL_ORG,
    AssayUpsertInternal,
    SampleUpsertInternal,
    SequencingGroupUpsertInternal,
)


class TestSample:
    """Test sample class"""

    @pytest.mark.asyncio
    async def test_add_sample(self, connection_with_project: Connection):
        """Test inserting a sample"""
        project_id = connection_with_project.project_id
        slayer = SampleLayer(connection_with_project)
        sample = await slayer.upsert_sample(
            SampleUpsertInternal(
                external_ids={PRIMARY_EXTERNAL_ORG: 'Test01'},
                type='blood',
                active=True,
                meta={'meta': 'meta ;)'},
            )
        )

        cur = await connection_with_project.pg_connection.execute(
            'SELECT id, type, meta, project FROM sample'
        )
        samples = await cur.fetchall()
        assert len(samples) == 1
        assert sample.id == samples[0]['id']

        mapping = await slayer.get_sample_id_map_by_external_ids(['Test01'], project_id)
        assert {'Test01': sample.id} == mapping

    @pytest.mark.asyncio
    async def test_get_sample(self, connection_with_project: Connection):
        """Test getting formed sample"""
        slayer = SampleLayer(connection_with_project)
        meta_dict = {'meta': 'meta ;)'}
        s = await slayer.upsert_sample(
            SampleUpsertInternal(
                external_ids={PRIMARY_EXTERNAL_ORG: 'Test01'},
                type='blood',
                active=True,
                meta=meta_dict,
            )
        )

        assert s.id is not None
        sample = await slayer.get_by_id(s.id)

        assert sample.type == 'blood'
        assert sample.meta == meta_dict

    @pytest.mark.asyncio
    async def test_query_sample_by_eid(self, connection_with_project: Connection):
        """Test querying samples by an external ID, and check it's returned"""
        slayer = SampleLayer(connection_with_project)
        meta_dict = {'meta': 'meta ;)'}
        ex_ids = {PRIMARY_EXTERNAL_ORG: 'Test01', 'external_org': 'ex01'}
        s = await slayer.upsert_sample(
            SampleUpsertInternal(
                external_ids=ex_ids,
                type='blood',
                active=True,
                meta=meta_dict,
            )
        )

        samples = await slayer.query(
            SampleFilter(external_id=GenericFilter(eq='Test01'))
        )
        assert len(samples) == 1
        assert s.id == samples[0].id
        assert ex_ids == samples[0].external_ids

        samples = await slayer.query(SampleFilter(external_id=GenericFilter(eq='ex01')))
        assert len(samples) == 1
        assert s.id == samples[0].id
        assert ex_ids == samples[0].external_ids

        samples = await slayer.query(SampleFilter(external_id=GenericFilter(eq='ex02')))
        assert len(samples) == 0

    @pytest.mark.asyncio
    @pytest.mark.project_roles(['reader', 'writer'])
    async def test_update_sample(self, connection_with_project: Connection):
        """Test updating a sample"""
        slayer = SampleLayer(connection_with_project)
        meta_dict = {'meta': 'meta ;)'}
        s = await slayer.upsert_sample(
            SampleUpsertInternal(
                external_ids={PRIMARY_EXTERNAL_ORG: 'Test01'},
                type='blood',
                active=True,
                meta=meta_dict,
            )
        )

        new_external_id_dict = {PRIMARY_EXTERNAL_ORG: 'Test02'}
        await slayer.upsert_sample(
            SampleUpsertInternal(id=s.id, external_ids=new_external_id_dict)
        )

        assert s.id is not None
        sample = await slayer.get_by_id(s.id)

        assert new_external_id_dict == sample.external_ids

    @pytest.mark.asyncio
    async def test_nested_samples_and_query(self, connection_with_project: Connection):
        """
        Test inserting a sample with nested samples and querying them
        """
        project_id = connection_with_project.project_id
        slayer = SampleLayer(connection_with_project)

        nested_sample = SampleUpsertInternal(
            external_ids={PRIMARY_EXTERNAL_ORG: 'Test01'},
            type='blood',
            nested_samples=[
                SampleUpsertInternal(
                    external_ids={PRIMARY_EXTERNAL_ORG: 'Test02'},
                    type='blood',
                    nested_samples=[
                        SampleUpsertInternal(
                            external_ids={PRIMARY_EXTERNAL_ORG: 'Test03'},
                            type='blood',
                        )
                    ],
                )
            ],
        )

        inserted = await slayer.upsert_samples([nested_sample])
        assert len(inserted) == 1

        top_sample = inserted[0]
        assert top_sample is not None
        assert top_sample.nested_samples is not None
        assert len(top_sample.nested_samples) == 1

        first_child = top_sample.nested_samples[0]
        assert first_child is not None
        assert first_child.nested_samples is not None
        assert len(first_child.nested_samples) == 1

        children_id = {first_child.id, first_child.nested_samples[0].id}

        # get all
        all_samples = await slayer.query(
            SampleFilter(project=GenericFilter(eq=project_id))
        )
        assert len(all_samples) == 3

        # get only the root
        root_samples = await slayer.query(
            SampleFilter(
                project=GenericFilter(eq=project_id),
                sample_root_id=GenericFilter(isnull=True),
            )
        )
        assert len(root_samples) == 1
        assert root_samples[0].id == top_sample.id
        parentless_samples = await slayer.query(
            SampleFilter(
                project=GenericFilter(eq=project_id),
                sample_parent_id=GenericFilter(isnull=True),
            )
        )
        assert len(parentless_samples) == 1
        assert parentless_samples[0].id == top_sample.id

        # get all children
        children = await slayer.query(
            SampleFilter(
                project=GenericFilter(eq=project_id),
                sample_root_id=GenericFilter(eq=top_sample.id),
            )
        )
        assert len(children) == 2
        assert children_id == {c.id for c in children}

        # get only first child
        first_child_res = await slayer.query(
            SampleFilter(
                project=GenericFilter(eq=project_id),
                sample_parent_id=GenericFilter(eq=top_sample.id),
            )
        )
        assert len(first_child_res) == 1
        assert first_child.id == first_child_res[0].id

    @pytest.mark.asyncio
    async def test_deleting_root_sample(self, connection_with_project: Connection):
        """Test that deleting the root sample cascade deletes the nested samples"""
        project_id = connection_with_project.project_id
        slayer = SampleLayer(connection_with_project)

        nested_sample = SampleUpsertInternal(
            external_ids={PRIMARY_EXTERNAL_ORG: 'Test01'},
            type='blood',
            nested_samples=[
                SampleUpsertInternal(
                    external_ids={PRIMARY_EXTERNAL_ORG: 'Test02'},
                    type='blood',
                    nested_samples=[
                        SampleUpsertInternal(
                            external_ids={PRIMARY_EXTERNAL_ORG: 'Test03'},
                            type='blood',
                        )
                    ],
                )
            ],
        )
        inserted = (await slayer.upsert_samples([nested_sample]))[0]
        pre_delete_samples = await slayer.query(
            SampleFilter(project=GenericFilter(eq=project_id))
        )
        assert len(pre_delete_samples) == 3

        # external ids do not have a cascade delete, so we need to delete them first
        await connection_with_project.pg_connection.execute(
            'DELETE FROM sample_external_id'
        )
        # but nested samples have cascade delete
        await connection_with_project.pg_connection.execute(
            t'DELETE FROM sample WHERE id = {inserted.id}'
        )

        post_delete_samples = await slayer.query(
            SampleFilter(project=GenericFilter(eq=project_id))
        )
        assert len(post_delete_samples) == 0

    @pytest.mark.asyncio
    @pytest.mark.project_roles(['reader', 'writer'])
    async def test_get_history_of_sample(self, connection_with_project: Connection):
        """Test getting the history of a sample"""
        slayer = SampleLayer(connection_with_project)

        s = await slayer.upsert_sample(
            SampleUpsertInternal(
                external_ids={PRIMARY_EXTERNAL_ORG: 'Test01'},
                type='blood',
                active=True,
                meta={'meta': 'meta ;)'},
            )
        )

        s = await slayer.upsert_sample(
            SampleUpsertInternal(
                id=s.id,
                external_ids={PRIMARY_EXTERNAL_ORG: 'Test01'},
                type='blood',
                active=False,
                meta={'meta': 'meta ;)', 'updated': 'yes'},
            )
        )

        assert s.id is not None
        history = await slayer.get_history_of_sample(s.id)

        assert len(history) == 2
        assert history[0].active is True
        assert history[1].active is False
        assert history[0].meta.get('updated') is None
        assert history[0].meta.get('meta') == 'meta ;)'
        assert history[1].meta.get('updated') == 'yes'
        assert history[1].meta.get('meta') == 'meta ;)'

        assert history[0].external_ids == {}
        assert history[1].external_ids == {}

    @pytest.mark.project_roles(['reader', 'writer'])
    async def test_merge_samples(self, connection_with_project: Connection):
        """
        Test that merge_samples(id_keep, id_merge) performs the correct modifications to the database
        """

        project_id = connection_with_project.project_id
        al = AssayLayer(connection_with_project)
        sl = SampleLayer(connection_with_project)
        sgl = SequencingGroupLayer(connection_with_project)

        meta_keep_key = 'meta01'
        meta_keep_value = 'meta :)'
        meta_merge_key = 'meta02'
        meta_merge_value = 'meta :('

        keep_sample = await sl.upsert_sample(
            SampleUpsertInternal(
                external_ids={PRIMARY_EXTERNAL_ORG: 'Test01'},
                type='blood',
                meta={meta_keep_key: meta_keep_value},
                active=True,
                sequencing_groups=[
                    SequencingGroupUpsertInternal(
                        type='genome',
                        technology='short-read',
                        platform='illumina',
                        meta={},
                        sample_id=None,
                        assays=[
                            AssayUpsertInternal(
                                type='sequencing',
                                meta={
                                    'sequencing_type': 'genome',
                                    'sequencing_technology': 'short-read',
                                    'sequencing_platform': 'illumina',
                                },
                            )
                        ],
                    )
                ],
            )
        )

        merge_sample = await sl.upsert_sample(
            SampleUpsertInternal(
                external_ids={PRIMARY_EXTERNAL_ORG: 'Test02'},
                type='blood',
                meta={meta_merge_key: meta_merge_value},
                active=True,
                sequencing_groups=[
                    SequencingGroupUpsertInternal(
                        type='exome',
                        technology='short-read',
                        platform='illumina',
                        meta={},
                        sample_id=None,
                        assays=[
                            AssayUpsertInternal(
                                type='sequencing',
                                meta={
                                    'sequencing_type': 'exome',
                                    'sequencing_technology': 'short-read',
                                    'sequencing_platform': 'illumina',
                                },
                            )
                        ],
                    ),
                ],
            )
        )

        assert keep_sample.id
        assert merge_sample.id

        # Retrieve the created sg ids
        keep_sg = await sgl.query(
            SequencingGroupFilter(
                sample=SequencingGroupFilter.SequencingGroupSampleFilter(
                    id=GenericFilter(eq=keep_sample.id)
                )
            )
        )
        assert keep_sg[0].id
        merge_sg = await sgl.query(
            SequencingGroupFilter(
                sample=SequencingGroupFilter.SequencingGroupSampleFilter(
                    id=GenericFilter(eq=merge_sample.id)
                )
            )
        )
        assert merge_sg[0].id
        # Assert that initially the sgs belong to different samples
        assert keep_sg[0].sample_id == keep_sample.id
        assert merge_sg[0].sample_id == merge_sample.id

        # Assert that initially the assays belong to different samples
        assays = await al.get_assays_for_sequencing_group_ids(
            [keep_sg[0].id, merge_sg[0].id]
        )
        assert assays[keep_sg[0].id][0].sample_id == keep_sample.id
        assert assays[merge_sg[0].id][0].sample_id == merge_sample.id

        samples = await sl.get_samples_by(project_ids=[project_id])
        assert len(samples) == 2

        new_sample = await sl.merge_samples(keep_sample.id, merge_sample.id)

        samples = await sl.get_samples_by(project_ids=[project_id])

        # Check that the "keep" sample is the only one remaining
        assert len(samples) == 1
        assert new_sample.id == keep_sample.id

        # Check that the meta fields have been merged
        assert new_sample.meta[meta_keep_key] == meta_keep_value
        assert new_sample.meta[meta_merge_key] == meta_merge_value

        # Check that the sequencing groups all belong to the same sample now
        new_sample_sgs = await sgl.get_sequencing_groups_by_ids(
            [keep_sg[0].id, merge_sg[0].id]
        )
        assert new_sample_sgs[0].sample_id == keep_sample.id
        assert new_sample_sgs[1].sample_id == keep_sample.id

        # Check that the assays all belong to the same sample now
        assays = await al.get_assays_for_sequencing_group_ids(
            [keep_sg[0].id, merge_sg[0].id]
        )
        assert assays[keep_sg[0].id][0].sample_id == new_sample.id
        assert assays[merge_sg[0].id][0].sample_id == new_sample.id


class TestSampleUnwrapping(unittest.TestCase):
    """Test unwrapping nested samples into an ordered list of rows"""

    def test_nested_sample_unwrapping_basic(self):
        """Basic case, one level, a few sub-samples"""
        sample = SampleUpsertInternal(
            id=1,
            nested_samples=[
                SampleUpsertInternal(id=2),
                SampleUpsertInternal(id=3),
                SampleUpsertInternal(id=4),
            ],
        )

        unwrapped = SampleLayer.unwrap_nested_samples([sample])

        assert len(unwrapped) == 4

        first_row = unwrapped[0]
        assert first_row.root is None
        assert first_row.parent is None
        assert first_row.sample.id == 1

        last_row = unwrapped[-1]
        assert last_row.root is not None
        assert last_row.parent is not None
        assert last_row.root.id == 1
        assert last_row.parent.id == 1
        assert last_row.sample.id == 4

    def test_nested_sample_unwrapping_many_layers(self):
        """
        Multiple levels, but less than the max depth
        I wrote this explicitly so there couldn't be an issue in a codegen
        """
        sample = SampleUpsertInternal(
            id=1,
            nested_samples=[
                SampleUpsertInternal(
                    id=2,
                    nested_samples=[
                        SampleUpsertInternal(
                            id=3,
                            nested_samples=[
                                SampleUpsertInternal(
                                    id=4,
                                    nested_samples=[
                                        SampleUpsertInternal(
                                            id=5,
                                            nested_samples=[
                                                SampleUpsertInternal(
                                                    id=6,
                                                    nested_samples=[
                                                        SampleUpsertInternal(id=7)
                                                    ],
                                                )
                                            ],
                                        )
                                    ],
                                )
                            ],
                        ),
                    ],
                ),
            ],
        )

        unwrapped = SampleLayer.unwrap_nested_samples([sample])

        assert len(unwrapped) == 7

        first_row = unwrapped[0]
        assert (first_row.root, first_row.parent, first_row.sample.id) == (
            None,
            None,
            1,
        )

        last_row = unwrapped[-1]
        assert last_row.root is not None
        assert last_row.parent is not None
        assert (last_row.root.id, last_row.parent.id, last_row.sample.id) == (1, 6, 7)

    def test_nested_sample_unwrapping_overflow(self):
        """
        Test something way too deep, so that the depth protection is triggered
        """
        # at least 20 layers
        root = SampleUpsertInternal(id=1)

        prev = root
        for i in range(2, 21):
            new_sample = SampleUpsertInternal(id=i)
            prev.nested_samples = [new_sample]
            prev = new_sample

        with self.assertRaises(SampleLayer.SampleUnwrapMaxDepthError):
            SampleLayer.unwrap_nested_samples([root])
