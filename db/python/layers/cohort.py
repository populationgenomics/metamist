from db.python.connect import Connection
from db.python.layers.base import BaseLayer
from db.python.layers.sequencing_group import SequencingGroupLayer
from db.python.tables.cohort import CohortFilter, CohortTable, CohortTemplateFilter
from db.python.tables.project import ProjectId, ProjectPermissionsTable
from db.python.tables.sample import SampleFilter, SampleTable
from db.python.tables.sequencing_group import (
    SequencingGroupFilter,
    SequencingGroupTable,
)
from db.python.utils import GenericFilter, get_logger
from models.models.cohort import (
    CohortCriteriaInternal,
    CohortInternal,
    CohortTemplateInternal,
    NewCohortInternal,
)

logger = get_logger()


def get_sg_filter(
    projects: list[ProjectId] | None,
    sg_ids_internal_raw: list[int] | None,
    excluded_sgs_internal_raw: list[int] | None,
    sg_technology: list[str] | None,
    sg_platform: list[str] | None,
    sg_type: list[str] | None,
    sample_ids: list[int] | None,
) -> SequencingGroupFilter:
    """Get the sequencing group filter for cohort attributes"""

    # Format inputs for filter
    if sg_ids_internal_raw and excluded_sgs_internal_raw:
        sg_id_filter = GenericFilter(
            in_=sg_ids_internal_raw, nin=excluded_sgs_internal_raw
        )
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
        self.ct = CohortTable(connection)
        self.pt = ProjectPermissionsTable(connection)
        self.sgt = SequencingGroupTable(connection)
        self.sglayer = SequencingGroupLayer(self.connection)

    async def query(
        self, filter_: CohortFilter, check_project_ids: bool = True
    ) -> list[CohortInternal]:
        """Query Cohorts"""
        cohorts, project_ids = await self.ct.query(filter_)

        if not cohorts:
            return []

        if check_project_ids:
            await self.pt.get_and_check_access_to_projects_for_ids(
                user=self.connection.author,
                project_ids=list(project_ids),
                readonly=True,
            )
        return cohorts

    async def query_cohort_templates(
        self, filter_: CohortTemplateFilter, check_project_ids: bool = True
    ) -> list[CohortTemplateInternal]:
        """Query CohortTemplates"""
        project_ids, cohort_templates = await self.ct.query_cohort_templates(filter_)

        if not cohort_templates:
            return []

        if check_project_ids:
            await self.pt.get_and_check_access_to_projects_for_ids(
                user=self.connection.author,
                project_ids=list(project_ids),
                readonly=True,
            )
        return cohort_templates

    async def get_template_by_cohort_id(self, cohort_id: int) -> CohortTemplateInternal:
        """
        Get the cohort template for a given cohort ID.
        """

        cohort = await self.ct.get_cohort_by_id(cohort_id)

        template_id = cohort.template_id
        if not template_id:
            raise ValueError(f'Cohort with ID {cohort_id} does not have a template')

        template = await self.ct.get_cohort_template(template_id)

        if not template:
            raise ValueError(f'Cohort template with ID {template_id} not found')

        return template

    async def get_cohort_sequencing_group_ids(self, cohort_id: int) -> list[int]:
        """
        Get the sequencing group IDs for the given cohort.
        """
        return await self.ct.get_cohort_sequencing_group_ids(cohort_id)

    async def create_cohort_template(
        self,
        cohort_template: CohortTemplateInternal,
        project: ProjectId,
    ) -> int:
        """
        Create new cohort template
        """

        assert cohort_template.criteria.projects, 'Projects must be set in criteria'
        assert cohort_template.id is None, 'Cohort template ID must be None'

        template_id = await self.ct.create_cohort_template(
            name=cohort_template.name,
            description=cohort_template.description,
            criteria=cohort_template.criteria,
            project=project,
        )

        return template_id

    async def create_cohort_from_criteria(
        self,
        project_to_write: ProjectId,
        description: str,
        cohort_name: str,
        dry_run: bool,
        cohort_criteria: CohortCriteriaInternal | None = None,
        template_id: int | None = None,
    ) -> NewCohortInternal:
        """
        Create a new cohort from the given parameters. Returns the newly created cohort_id.
        """

        create_cohort_template = True

        # Input validation
        if not cohort_criteria and not template_id:
            raise ValueError(
                'A cohort must have either criteria or be derived from a template'
            )

        template: CohortTemplateInternal | None = None
        # Get template from ID
        if template_id:
            template = await self.ct.get_cohort_template(template_id)
            if not template:
                raise ValueError(f'Cohort template with ID {template_id} not found')

        if template and cohort_criteria:
            # TODO: Perhaps handle this case in future. For now, not supported.
            raise ValueError(
                'A cohort cannot have both criteria and be derived from a template'
            )

        # Only provide a template id
        if template and not cohort_criteria:
            create_cohort_template = False
            cohort_criteria = template.criteria

        if not cohort_criteria:
            raise ValueError('Cohort criteria must be set')

        sample_ids: list[int] = []
        if cohort_criteria.sample_type:
            # Get sample IDs with sample type
            sample_filter = SampleFilter(
                project=GenericFilter(in_=cohort_criteria.projects),
                type=(
                    GenericFilter(in_=cohort_criteria.sample_type)
                    if cohort_criteria.sample_type
                    else None
                ),
            )

            _, samples = await self.sampt.query(sample_filter)
            sample_ids = [s.id for s in samples]

        sg_filter = get_sg_filter(
            projects=cohort_criteria.projects,
            sg_ids_internal_raw=cohort_criteria.sg_ids_internal_raw,
            excluded_sgs_internal_raw=cohort_criteria.excluded_sgs_internal_raw,
            sg_technology=cohort_criteria.sg_technology,
            sg_platform=cohort_criteria.sg_platform,
            sg_type=cohort_criteria.sg_type,
            sample_ids=sample_ids,
        )

        sgs = await self.sglayer.query(sg_filter)

        if dry_run:
            sg_ids = [sg.id for sg in sgs if sg.id] if sgs else []

            return NewCohortInternal(
                dry_run=True,
                cohort_id=None,
                sequencing_group_ids=sg_ids,
            )
        # 2. Create cohort template, if required.
        if create_cohort_template:
            cohort_template = CohortTemplateInternal(
                id=None,
                name=cohort_name,
                description=description,
                criteria=cohort_criteria,
            )
            template_id = await self.create_cohort_template(
                cohort_template=cohort_template, project=project_to_write
            )

        if not template_id:
            raise ValueError('Template ID must be set')

        # 3. Create Cohort
        return await self.ct.create_cohort(
            project=project_to_write,
            cohort_name=cohort_name,
            sequencing_group_ids=[sg.id for sg in sgs if sg.id],
            description=description,
            template_id=template_id,
        )
