from .sample import get_sample_blueprint
from .imports import get_import_blueprint
from .analysis import get_analysis_blueprint

all_blueprints = [get_sample_blueprint, get_import_blueprint, get_analysis_blueprint]
