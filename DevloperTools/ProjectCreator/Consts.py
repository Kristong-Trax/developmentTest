__author__ = 'yoava'

MAIN_FILE_NAME = 'Calculations'
GENERATOR_FILE_NAME = 'KPIGenerator'
FETCHER_FILE_NAME = 'Fetcher'
TOOL_BOX_FILE_NAME = 'KPIToolBox'
LOCAL_CALCULATIONS_FILE_NAME = 'LocalCalculations'
PROFILING_SCRIPT_NAME = 'gen_profiling'
DEPENDENCIES_SCRIPT_NAME = 'gen_dependency_graph'
TESTS_SCRIPT_NAME = 'test_unit'
SCENE_TOOLBOX_FILE_NAME = 'KPISceneToolBox'
SCENE_GENERATOR_FILE_NAME = 'KPISceneGenerator'
SCENE_CALCULATIONS_FILE_NAME = 'SceneCalculations'
PLANOGRAM_COMPLIANCE_CALCULATIONS_FILE_NAME = 'PlanogramComplianceCalculation'
PLANOGRAM_FINDER_CALCULATIONS_FILE_NAME = 'PlanogramFinderCalculation'


LOCAL_FILE = """
# from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
# from Trax.Utils.Conf.Configuration import Config
# from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
# from Projects.%(project_capital)s.Calculations import Calculations


# if __name__ == '__main__':
#     LoggerInitializer.init('%(project)s calculations')
#     Config.init()
#     project_name = '%(project)s'
#     data_provider = KEngineDataProvider(project_name)
#     session = ''
#     data_provider.load_session_data(session)
#     output = Output()
#     Calculations(data_provider, output).run_project_calculations()
"""


MAIN_FILE = """
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript

from Projects.%(project_capital)s.%(generator_file_name)s import %(generator_class_name)s

__author__ = '%(author)s'


class Calculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        %(generator_class_name)s(self.data_provider, self.output).main_function()
        self.timer.stop('%(generator_file_name)s.run_project_calculations')



"""

TOOL_BOX = """
from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Cloud.Services.Connector.Keys import DbUsers
from Trax.Data.Projects.Connector import ProjectConnector
# from Trax.Utils.Logging.Logger import Log

from KPIUtils_v2.DB.Common import Common
# from KPIUtils_v2.Calculations.AssortmentCalculations import Assortment
# from KPIUtils_v2.Calculations.AvailabilityCalculations import Availability
# from KPIUtils_v2.Calculations.NumberOfScenesCalculations import NumberOfScenes
# from KPIUtils_v2.Calculations.PositionGraphsCalculations import PositionGraphs
# from KPIUtils_v2.Calculations.SOSCalculations import SOS
# from KPIUtils_v2.Calculations.SequenceCalculations import Sequence
# from KPIUtils_v2.Calculations.SurveyCalculations import Survey

# from KPIUtils_v2.Calculations.CalculationsUtils import GENERALToolBoxCalculations

__author__ = '%(author)s'

KPI_RESULT = 'report.kpi_results'
KPK_RESULT = 'report.kpk_results'
KPS_RESULT = 'report.kps_results'


class %(tool_box_class_name)s:
    LEVEL1 = 1
    LEVEL2 = 2
    LEVEL3 = 3

    def __init__(self, data_provider, output):
        self.output = output
        self.data_provider = data_provider
        self.common = Common(self.data_provider)
        self.project_name = self.data_provider.project_name
        self.session_uid = self.data_provider.session_uid
        self.products = self.data_provider[Data.PRODUCTS]
        self.all_products = self.data_provider[Data.ALL_PRODUCTS]
        self.match_product_in_scene = self.data_provider[Data.MATCHES]
        self.visit_date = self.data_provider[Data.VISIT_DATE]
        self.session_info = self.data_provider[Data.SESSION_INFO]
        self.scene_info = self.data_provider[Data.SCENES_INFO]
        self.store_id = self.data_provider[Data.STORE_FK]
        self.scif = self.data_provider[Data.SCENE_ITEM_FACTS]
        self.rds_conn = ProjectConnector(self.project_name, DbUsers.CalculationEng)
        self.kpi_static_data = self.common.get_kpi_static_data()
        self.kpi_results_queries = []

    def main_calculation(self, *args, **kwargs):
        \"""
        This function calculates the KPI results.
        \"""
        score = 0
        return score
"""

GENERATOR = """
from Trax.Utils.Logging.Logger import Log

from Projects.%(project_capital)s.Utils.%(tool_box_file_name)s import %(tool_box_class_name)s

from KPIUtils_v2.DB.Common import Common

from KPIUtils_v2.Utils.Decorators.Decorators import log_runtime

__author__ = '%(author)s'


class %(generator_class_name)s:

    def __init__(self, data_provider, output):
        self.data_provider = data_provider
        self.output = output
        self.project_name = data_provider.project_name
        self.session_uid = self.data_provider.session_uid
        self.tool_box = %(tool_box_class_name)s(self.data_provider, self.output)
        self.common = Common(data_provider)

    @log_runtime('Total Calculations', log_start=True)
    def main_function(self):
        \"""
        This is the main KPI calculation function.
        It calculates the score for every KPI set and saves it to the DB.
        \"""
        if self.tool_box.scif.empty:
            Log.warning('Scene item facts is empty for this session')
        for kpi_set_fk in self.tool_box.kpi_static_data['kpi_set_fk'].unique().tolist():
            score = self.tool_box.main_calculation(kpi_set_fk=kpi_set_fk)
            self.common.write_to_db_result(kpi_set_fk, self.tool_box.LEVEL1, score)
        self.common.commit_results_data()
"""

PROFILING_SCRIPT = """

#!/usr/bin/env bash

__author__ = '%(author)s'

# this scripts gen an .svg file to see clearly the code flow execution for profiling
# between imports

# HOW TO USE
# ============
# cd to kpi_factory/Projects/<your project>/Profiling
# in terminal : ./gen_profiling.sh


PROJECT_DIR=$PWD

PARENT_DIR="$(dirname "$PROJECT_DIR")"

PROJECT=${PARENT_DIR##*/}


cd .. && cd .. && cd .. && python -m cProfile -o 1.stats ~/dev/kpi_factory/Projects/$PROJECT/Calculations.py -e prod -c ~/dev/theGarage/Trax/Apps/Services/KEngine/k-engine-prod.config
gprof2dot -f pstats 1.stats -o 1.dot
dot -Tsvg -Gdpi=70 -o ${PROJECT}_profiling.svg 1.dot

mv ~/dev/kpi_factory/1.dot ${PROJECT_DIR}/1.dot
mv ~/dev/kpi_factory/1.stats ${PROJECT_DIR}/1.stats
mv ~/dev/kpi_factory/${PROJECT}_profiling.svg ${PROJECT_DIR}/${PROJECT}_profiling.svg

"""


GEN_DEPENDENCY_SCRIPT = """

#!/usr/bin/env bash

__author__ = '%(author)s'

# this scripts gen an .svg file to see clearly the dependencies
# between imports

# HOW TO USE
# ============
# cd to kpi_factory/Projects/<your project>/Profiling
# in terminal : ./gen_dependency_graph.sh

PROJECT_DIR=$PWD


PARENT_DIR="$(dirname "$PROJECT_DIR")"



~/miniconda/envs/garage/bin/sfood ${PARENT_DIR}/ | ~/miniconda/envs/garage/bin/sfood-graph > /tmp/d.dot
dot -Tsvg -Gdpi=70 /tmp/d.dot -o ${PROJECT_DIR}/graph1.svg



export message='"message"'
export severity='"severity"'
export application='"application"'
export environment='"environment"'
export my_user=$(whoami)

curl -X POST https://logs-01.loggly.com/inputs/2cce0ddd-ce82-4f1f-af5d-f72be7fc67ae/tag/python,PS,Install_hooks/ -d "{action: gen dependency graph, $severity: 'info', $application: 'PS_dev_tools' , $environment: 'dev', user:$my_user}"

"""

TEST_SCRIPT = """
from Trax.Utils.Conf.Configuration import Config
from Trax.Utils.Testing.Case import TestCase
from mock import MagicMock, mock
import pandas as pd
from Projects.%(project_capital)s.Utils.%(tool_box_file_name)s import %(tool_box_class_name)s


__author__ = '%(author)s'


class Test%(project_capital)s(TestCase):

    @mock.patch('Projects.%(project_capital)s.Utils.%(tool_box_file_name)s.ProjectConnector')
    def setUp(self, x):
        Config.init('')
        self.data_provider_mock = MagicMock()
        self.data_provider_mock.project_name = '%(project)s'
        self.data_provider_mock.rds_conn = MagicMock()
        self.output = MagicMock()
        self.tool_box = %(tool_box_class_name)s(self.data_provider_mock, MagicMock())

"""

SCENE_TOOLBOX_SCRIPT = """
from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Cloud.Services.Connector.Keys import DbUsers
from Trax.Data.Projects.Connector import ProjectConnector
from Trax.Utils.Logging.Logger import Log

# from KPIUtils_v2.DB.Common import Common
# from KPIUtils_v2.DB.CommonV2 import Common
# from KPIUtils_v2.Calculations.AssortmentCalculations import Assortment
# from KPIUtils_v2.Calculations.AvailabilityCalculations import Availability
# from KPIUtils_v2.Calculations.NumberOfScenesCalculations import NumberOfScenes
# from KPIUtils_v2.Calculations.PositionGraphsCalculations import PositionGraphs
# from KPIUtils_v2.Calculations.SOSCalculations import SOS
# from KPIUtils_v2.Calculations.SequenceCalculations import Sequence
# from KPIUtils_v2.Calculations.SurveyCalculations import Survey

# from KPIUtils_v2.Calculations.CalculationsUtils import GENERALToolBoxCalculations

__author__ = '%(author)s'

KPI_RESULT = 'report.kpi_results'
KPK_RESULT = 'report.kpk_results'
KPS_RESULT = 'report.kps_results'


class %(scene_tool_box_class_name)s:
    LEVEL1 = 1
    LEVEL2 = 2
    LEVEL3 = 3
    
    def __init__(self, data_provider, output, common):
        self.output = output
        self.data_provider = data_provider
        self.common = common
        self.project_name = self.data_provider.project_name
        self.session_uid = self.data_provider.session_uid
        self.products = self.data_provider[Data.PRODUCTS]
        self.templates = self.data_provider[Data.TEMPLATES]
        self.all_products = self.data_provider[Data.ALL_PRODUCTS]
        self.match_product_in_scene = self.data_provider[Data.MATCHES]
        self.visit_date = self.data_provider[Data.VISIT_DATE]
        self.session_info = self.data_provider[Data.SESSION_INFO]
        self.scene_info = self.data_provider[Data.SCENES_INFO]
        self.store_id = self.data_provider[Data.STORE_FK]
        self.store_type = self.data_provider.store_type
        self.rds_conn = ProjectConnector(self.project_name, DbUsers.CalculationEng)
        self.kpi_static_data = self.common.get_kpi_static_data()
        self.kpi_results_queries = []

    def main_function(self):
        score = 0
        return score
"""

SCENE_GENERATOR_SCRIPT = """
from Trax.Utils.Logging.Logger import Log
from KPIUtils_v2.Utils.Decorators.Decorators import log_runtime

from Projects.%(project_capital)s.Utils.%(scene_tool_box_file_name)s import %(scene_tool_box_class_name)s

from KPIUtils_v2.DB.CommonV2 import Common


__author__ = '%(author)s'


class SceneGenerator:

    def __init__(self, data_provider, output=None):
        self.data_provider = data_provider
        self.output = output
        self.project_name = data_provider.project_name
        self.session_uid = self.data_provider.session_uid
        self.common = Common(data_provider)
        self.scene_tool_box = %(scene_tool_box_class_name)s(self.data_provider, self.output, self.common)

    @log_runtime('Total Calculations', log_start=True)
    def scene_score(self):
        if self.scene_tool_box.match_product_in_scene.empty:
            Log.warning('Match product in scene is empty for this scene')
        else:
            self.scene_tool_box.main_function()
            self.common.commit_results_data()
"""

SCENE_CALCULATIONS_SCRIPT = """
from Trax.Apps.Services.KEngine.Handlers.Utils.Scripts import SceneBaseClass
from Projects.%(project_capital)s.%(scene_generator_file_name)s import %(scene_generator_class_name)s

__author__ = '%(author)s'

class SceneCalculations(SceneBaseClass):
    def __init__(self, data_provider):
        super(SceneCalculations, self).__init__(data_provider)
        self.scene_generator =  %(scene_generator_class_name)s(self._data_provider)

    def calculate_kpis(self):
        self.scene_generator.scene_score()

"""


PLANOGRAM_COMPLIANCE_CALCULATIONS_SCRIPT = """
from Trax.Apps.Services.KEngine.Handlers.Utils.Scripts import PlanogramComplianceBaseClass

__author__ = '%(author)s'


class PlanogramComplianceCalculation(PlanogramComplianceBaseClass):
    def __init__(self, data_provider):
        super(PlanogramComplianceCalculation, self).__init__(data_provider)

    def get_compliance(self):
        pass

"""

PLANOGRAM_FINDER_CALCULATIONS_SCRIPT = """
from Trax.Apps.Services.KEngine.Handlers.Utils.Scripts import PlanogramFinderBaseClass

__author__ = '%(author)s'


class PlanogramFinderCalculation(PlanogramFinderBaseClass):
    def __init__(self, data_provider):
        super(PlanogramFinderCalculation, self).__init__(data_provider)

    def get_planogram_id(self):
        pass
"""