import os

from api.routes.sample import router as sample_router
from api.routes.imports import router as import_router
from api.routes.analysis import router as analysis_router
from api.routes.assay import router as assay_router
from api.routes.participant import router as participant_router
from api.routes.family import router as family_router
from api.routes.project import router as project_router
from api.routes.web import router as web_router
from api.routes.enum import router as enum_router
from api.routes.sequencing_groups import router as sequencing_groups_router

billing_envs = [
    'SM_GCP_BQ_AGGREG_VIEW',
    'SM_GCP_BQ_AGGREG_RAW',
    'SM_GCP_BQ_AGGREG_EXT_VIEW',
    'SM_GCP_BQ_BUDGET_VIEW',
]

if all((os.environ.get(env) for env in billing_envs)):
    from api.routes.billing import router as billing_router
