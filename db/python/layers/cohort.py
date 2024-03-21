import json

from db.python.connect import Connection
from db.python.layers.base import BaseLayer
from db.python.layers.sequencing_group import SequencingGroupLayer
from db.python.tables.analysis import AnalysisTable
from db.python.tables.cohort import CohortFilter, CohortTable, CohortTemplateFilter
from db.python.tables.project import ProjectId, ProjectPermissionsTable
from db.python.tables.sample import SampleFilter, SampleTable
from db.python.tables.sequencing_group import (
    SequencingGroupFilter,
    SequencingGroupTable,
)
from db.python.utils import GenericFilter, get_logger
from models.models.cohort import Cohort, CohortCriteria, CohortTemplate
from models.utils.cohort_id_format import cohort_id_format
from models.utils.sequencing_group_id_format import (
    sequencing_group_id_format_list,
    sequencing_group_id_transform_to_raw_list,
)

logger = get_logger()


def get_sg_filter(
        projects,
        sg_ids_internal_rich,
        excluded_sgs_internal_rich,
        sg_technology,
        sg_platform,
        sg_type,
        sample_ids
):
    """ Get the sequencing group filter for cohort attributes"""

    # Format inputs for filter
    sg_ids_internal_raw = []
    excluded_sgs_internal_raw = []
    if sg_ids_internal_rich:
        sg_ids_internal_raw = sequencing_group_id_transform_to_raw_list(sg_ids_internal_rich)
    if excluded_sgs_internal_rich:
        excluded_sgs_internal_raw = sequencing_group_id_transform_to_raw_list(excluded_sgs_internal_rich)

    if sg_ids_internal_raw and excluded_sgs_internal_raw:
        sg_id_filter = GenericFilter(in_=sg_ids_internal_raw, nin=excluded_sgs_internal_raw)
    elif sg_ids_internal_raw:
        sg_id_filter = GenericFilter(in_=sg_ids_internal_raw)
    elif excluded_sgs_internal_raw:
        sg_id_filter = GenericFilter(nin=excluded_sgs_internal_raw)
    else:
        sg_id_filter = None

    sg_filter = SequencingGroupFilter(
            project=GenericFilter(in_=projects),
            id=sg_id_filter,
            technology=GenericFilter(in_=sg_technology) if sg_technology else None,
            platform=GenericFilter(in_=sg_platform) if sg_platform else None,
            type=GenericFilter(in_=sg_type) if sg_type else None,
            sample_id=GenericFilter(in_=sample_ids) if sample_ids else None,
        )

    return sg_filter


class CohortLayer(BaseLayer):
    """Layer for cohort logic"""

    def __init__(self, connection: Connection):
        super().__init__(connection)

        self.sampt = SampleTable(connection)
        self.at = AnalysisTable(connection)
        self.ct = CohortTable(connection)
        self.pt = ProjectPermissionsTable(connection)
        self.sgt = SequencingGroupTable(connection)
        self.sglayer = SequencingGroupLayer(self.connection)

    async def query(self, filter_: CohortFilter) -> list[Cohort]:
        """Query Cohorts"""
        cohorts = await self.ct.query(filter_)
        return cohorts

    async def query_cohort_templates(self, filter_: CohortTemplateFilter) -> list[CohortTemplate]:
        """Query CohortTemplates"""
        cohort_templates = await self.ct.query_cohort_templates(filter_)
        return cohort_templates

    async def get_cohort_sequencing_group_ids(self, cohort_id: int) -> list[int]:
        """
        Get the sequencing group IDs for the given cohort.
        """
        return await self.ct.get_cohort_sequencing_group_ids(cohort_id)

    async def create_cohort_template(
            self,
            cohort_template: CohortTemplate,
            project: ProjectId,

    ):
        """
        Create new cohort template
        """

        return await self.ct.create_cohort_template(
            name=cohort_template.name,
            description=cohort_template.description,
            criteria=dict(cohort_template.criteria),
            project=project
        )

    async def create_cohort_from_criteria(
            self,
            project_to_write: ProjectId,
            author: str,
            description: str,
            cohort_name: str,
            dry_run: bool,
            cohort_criteria: CohortCriteria = None,
            template_id: int = None,
    ):
        """
        Create a new cohort from the given parameters. Returns the newly created cohort_id.
        """

        create_cohort_template = True

        # Input validation
        if not cohort_criteria and not template_id:
            raise ValueError('A cohort must have either criteria or be derived from a template')

        # Get template from ID
        template: dict[str, str] = {}
        if template_id:
            template = await self.ct.get_cohort_template(template_id)
            if not template:
                raise ValueError(f'Cohort template with ID {template_id} not found')

        if template and cohort_criteria:
            # TODO: Handle this case. For now, not supported.
            raise ValueError('A cohort cannot have both criteria and be derived from a template')

        # Only provide a template id
        if template and not cohort_criteria:
            create_cohort_template = False
            criteria_dict = json.loads(template['criteria'])
            cohort_criteria = CohortCriteria(**criteria_dict)

        projects_to_pull = await self.pt.get_and_check_access_to_projects_for_names(
            user=self.connection.author, project_names=cohort_criteria.projects, readonly=True
        )
        projects_to_pull = [p.id for p in projects_to_pull]

        # Get sample IDs with sample type
        sample_filter = SampleFilter(
            project=GenericFilter(in_=projects_to_pull),
            type=GenericFilter(in_=cohort_criteria.sample_type) if cohort_criteria.sample_type else None,
        )

        _, samples = await self.sampt.query(sample_filter)

        sg_filter = get_sg_filter(
            projects=projects_to_pull,
            sg_ids_internal_rich=cohort_criteria.sg_ids_internal,
            excluded_sgs_internal_rich=cohort_criteria.excluded_sgs_internal,
            sg_technology=cohort_criteria.sg_technology,
            sg_platform=cohort_criteria.sg_platform,
            sg_type=cohort_criteria.sg_type,
            sample_ids=[s.id for s in samples]
        )

        sgs = await self.sglayer.query(sg_filter)

        rich_ids = sequencing_group_id_format_list([sg.id for sg in sgs])

        if dry_run:
            return {'dry_run': True, 'template_id': template_id or 'CREATE NEW', 'sequencing_group_ids': rich_ids}

        # 2. Create cohort template, if required.
        if create_cohort_template:
            cohort_template = CohortTemplate(
                name=cohort_name,
                description=description,
                criteria=cohort_criteria
            )
            template_id = await self.create_cohort_template(
                cohort_template=cohort_template,
                project=project_to_write
            )

        assert template_id, 'Template ID must be set'

        # 3. Create Cohort
        cohort_id = await self.ct.create_cohort(
            project=project_to_write,
            cohort_name=cohort_name,
            sequencing_group_ids=[sg.id for sg in sgs],
            description=description,
            author=author,
            template_id=template_id,
        )

        return {'cohort_id': cohort_id_format(cohort_id), 'sequencing_group_ids': rich_ids}