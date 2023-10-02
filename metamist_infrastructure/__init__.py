"""
Workaround to exclude MetamistInfrastructure from unittest
"""

import sys
if 'unittest' not in sys.modules:
    from metamist_infrastructure.driver import MetamistInfrastructure
