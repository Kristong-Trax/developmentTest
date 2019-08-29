__author__ = 'yoava_shivi'


class Const(object):

    MAIN_FILE_NAME = 'Calculations'
    GENERATOR_FILE_NAME = 'KPIGenerator'
    FETCHER_FILE_NAME = 'Fetcher'
    TOOL_BOX_FILE_NAME = 'KPIToolBox'
    LOCAL_CONSTS_FILE_NAME = 'LocalConsts'
    LOCAL_CALCULATIONS_FILE_NAME = 'LocalCalculations'
    PROFILING_SCRIPT_NAME = 'gen_profiling'
    DEPENDENCIES_SCRIPT_NAME = 'gen_dependency_graph'
    TESTS_SCRIPT_NAME = 'test_unit'
    SCENE_TOOLBOX_FILE_NAME = 'KPISceneToolBox'
    SCENE_GENERATOR_FILE_NAME = 'KPISceneGenerator'
    SCENE_CALCULATIONS_FILE_NAME = 'SceneCalculations'
    PLANOGRAM_TOOLBOX_FILE_NAME = 'KPIPlanogramToolBox'
    PLANOGRAM_GENERATOR_FILE_NAME = 'KPIPlanogramGenerator'
    PLANOGRAM_CALCULATIONS_FILE_NAME = 'PlanogramCalculations'
    LIVE_SCENE_TOOLBOX_FILE_NAME = 'LiveSceneToolBox'
    LIVE_SCENE_GENERATOR_FILE_NAME = 'LiveSceneGenerator'
    LIVE_SCENE_CALCULATIONS_FILE_NAME = 'LiveSceneCalculations'
    LIVE_SESSION_TOOLBOX_FILE_NAME = 'LiveSessionToolBox'
    LIVE_SESSION_GENERATOR_FILE_NAME = 'LiveSessionGenerator'
    LIVE_SESSION_CALCULATIONS_FILE_NAME = 'LiveSessionCalculations'
    PLANOGRAM_COMPLIANCE_CALCULATIONS_FILE_NAME = 'PlanogramComplianceCalculation'
    PLANOGRAM_FINDER_CALCULATIONS_FILE_NAME = 'PlanogramFinderCalculation'
    LIVE_SCENE_GENERATOR_CLASS_NAME = 'LiveSceneGenerator'
    LIVE_SESSION_GENERATOR_CLASS_NAME = 'LiveSessionGenerator'
    PLANOGRAM_GENERATOR_CLASS_NAME = 'PlanogramGenerator'
    SCENE_GENERATOR_CLASS_NAME = 'SceneGenerator'
    GENERATOR_CLASS_NAME = 'Generator'
    TOOL_BOX_CLASS_NAME = 'ToolBox'
    PLANOGRAM_TOOL_BOX_CLASS_NAME = 'PlanogramToolBox'
    LIVE_SCENE_TOOL_BOX_CLASS_NAME = 'LiveSceneToolBox'
    MAIN_CLASS_NAME = 'Calculations'
    LIVE_SESSION_TOOL_BOX_CLASS_NAME = 'LiveSessionToolBox'
    SCENE_TOOL_BOX_CLASS_NAME = 'SceneToolBox'


# local scripts:

LOCAL_CALCS = """
# from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
# from Trax.Utils.Conf.Configuration import Config
# from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
# from Projects.%(project_capital)s.Calculations import Calculations


# if __name__ == '__main__':
#     LoggerInitializer.init('%(project)s calculations')
#     Config.init()
#     project_name = '%(project)s'
#     data_provider = KEngineDataProvider(project_name)
#     session_list = ['INSERT_TEST_SESSIONS']
#     for session in session_list:
#         data_provider.load_session_data(session)
#         output = Output()
#         Calculations(data_provider, output).run_project_calculations()
"""

LOCAL_CALCS_WITH_SCENES = """
# import pandas as pd
# from Trax.Algo.Calculations.Core.Constants import Keys, Fields, SCENE_ITEM_FACTS_COLUMNS
# from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
# from Trax.Utils.Conf.Configuration import Config
# from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
# from Projects.%(project_capital)s.Calculations import Calculations
# from Trax.Algo.Calculations.Core.Vanilla.Calculations import SceneVanillaCalculations
# from Trax.Algo.Calculations.Core.Vanilla.Output import VanillaOutput
# from Projects.%(project_capital)s.SceneKpis.SceneCalculations import SceneCalculations
# 
# 
# def save_scene_item_facts_to_data_provider(data_provider, output):
#     scene_item_facts_obj = output.get_facts()
#     if scene_item_facts_obj:
#         scene_item_facts = scene_item_facts_obj[Keys.SCENE_ITEM_FACTS][Keys.SCENE_ITEM_FACTS].fact_df
#     else:
#         scene_item_facts = pd.DataFrame(columns=SCENE_ITEM_FACTS_COLUMNS)
#     scene_item_facts.rename(columns={Fields.PRODUCT_FK: 'item_id', Fields.SCENE_FK: 'scene_id'}, inplace=True)
#     data_provider.set_scene_item_facts(scene_item_facts)
# 
# 
# if __name__ == '__main__':
#     LoggerInitializer.init('%(project)s calculations')
#     Config.init()
#     project_name = '%(project)s'
#     data_provider = KEngineDataProvider(project_name)
#     session_list = {'INSERT_TEST_SESSION': [1, 2, 3, 4],
#                     'INSERT_TEST_SESSION2': []}  # leave empty for all scenes 
#     for session in session_list:
#         scenes = session_list[session]
#         if len(scenes) == 0:
#             data_provider = KEngineDataProvider(project_name)
#             data_provider.load_session_data(session)
#             scif = data_provider['scene_item_facts']
#             scenes = scif['scene_id'].unique().tolist()
#         for scene in scenes:
#             print('scene')
#             data_provider = KEngineDataProvider(project_name)
#             data_provider.load_scene_data(session, scene)
#             output = VanillaOutput()
#             SceneVanillaCalculations(data_provider, output).run_project_calculations()
#             save_scene_item_facts_to_data_provider(data_provider, output)
#             SceneCalculations(data_provider).calculate_kpis()
#         data_provider.load_session_data(session)
#         output = Output()
#         Calculations(data_provider, output).run_project_calculations()
"""

LOCAL_CONSTS = """
class Consts(object):

    pass
"""

# session:

CALCULATIONS = """
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript

from Projects.%(project_capital)s.%(generator_file_name)s import %(generator_class_name)s

__author__ = '%(author)s'


class Calculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        %(generator_class_name)s(self.data_provider, self.output).main_function()
        self.timer.stop('%(generator_file_name)s.run_project_calculations')
"""

GENERATOR = """
from Trax.Utils.Logging.Logger import Log
from KPIUtils_v2.Utils.Decorators.Decorators import log_runtime
from Projects.%(project_capital)s.Utils.%(tool_box_file_name)s import %(tool_box_class_name)s

__author__ = '%(author)s'


class %(generator_class_name)s:

    def __init__(self, data_provider, output):
        self.data_provider = data_provider
        self.output = output
        self.project_name = data_provider.project_name
        self.session_uid = self.data_provider.session_uid
        self.tool_box = %(tool_box_class_name)s(self.data_provider, self.output)

    @log_runtime('Total Calculations', log_start=True)
    def main_function(self):
        \"""
        This is the main KPI calculation function.
        It calculates the score for every KPI set and saves it to the DB.
        \"""
        if self.tool_box.scif.empty:
            Log.warning('Scene item facts is empty for this session')
        self.tool_box.main_calculation()
        self.tool_box.commit_results()
"""

TOOL_BOX = """
from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Utils.Logging.Logger import Log
from KPIUtils_v2.Utils.GlobalScripts.Scripts import GlobalSessionToolBox
# import pandas as pd

from Projects.%(project_capital)s.Data.LocalConsts import Consts

# from KPIUtils_v2.Utils.Consts.DataProvider import 
# from KPIUtils_v2.Utils.Consts.DB import 
# from KPIUtils_v2.Utils.Consts.PS import 
# from KPIUtils_v2.Utils.Consts.GlobalConsts import 
# from KPIUtils_v2.Utils.Consts.Messages import 
# from KPIUtils_v2.Utils.Consts.Custom import 
# from KPIUtils_v2.Utils.Consts.OldDB import 

# from KPIUtils_v2.Calculations.AssortmentCalculations import Assortment
# from KPIUtils_v2.Calculations.AvailabilityCalculations import Availability
# from KPIUtils_v2.Calculations.NumberOfScenesCalculations import NumberOfScenes
# from KPIUtils_v2.Calculations.PositionGraphsCalculations import PositionGraphs
# from KPIUtils_v2.Calculations.SOSCalculations import SOS
# from KPIUtils_v2.Calculations.SequenceCalculations import Sequence
# from KPIUtils_v2.Calculations.SurveyCalculations import Survey

# from KPIUtils_v2.Calculations.CalculationsUtils import GENERALToolBoxCalculations

__author__ = '%(author)s'


class %(tool_box_class_name)s(GlobalSessionToolBox):

    def __init__(self, data_provider, output):
        GlobalSessionToolBox.__init__(self, data_provider, output)

    def main_calculation(self):
        score = 0
        return score
"""

# scene:

SCENE_CALCULATIONS = """
from Trax.Apps.Services.KEngine.Handlers.Utils.Scripts import SceneBaseClass
from Projects.%(project_capital)s.%(scene_generator_file_name)s import %(scene_generator_class_name)s

__author__ = '%(author)s'


class SceneCalculations(SceneBaseClass):
    def __init__(self, data_provider):
        super(SceneCalculations, self).__init__(data_provider)
        self.scene_generator = %(scene_generator_class_name)s(self._data_provider)

    def calculate_kpis(self):
        self.scene_generator.scene_score()

"""

SCENE_GENERATOR_SCRIPT = """
from Trax.Utils.Logging.Logger import Log
from KPIUtils_v2.Utils.Decorators.Decorators import log_runtime

from Projects.%(project_capital)s.Utils.%(scene_tool_box_file_name)s import %(scene_tool_box_class_name)s

__author__ = '%(author)s'


class SceneGenerator:

    def __init__(self, data_provider, output=None):
        self.data_provider = data_provider
        self.output = output
        self.project_name = data_provider.project_name
        self.session_uid = self.data_provider.session_uid
        self.scene_tool_box = %(scene_tool_box_class_name)s(self.data_provider, self.output)

    @log_runtime('Total Calculations', log_start=True)
    def scene_score(self):
        if self.scene_tool_box.match_product_in_scene.empty:
            Log.warning('Match product in scene is empty for this scene')
        self.scene_tool_box.main_function()
        self.scene_tool_box.commit_results()
"""

SCENE_TOOLBOX_SCRIPT = """
from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Utils.Logging.Logger import Log
from KPIUtils_v2.Utils.GlobalScripts.Scripts import GlobalSceneToolBox
# import pandas as pd

from Projects.%(project_capital)s.Data.LocalConsts import Consts

# from KPIUtils_v2.Utils.Consts.DataProvider import  
# from KPIUtils_v2.Utils.Consts.DB import 
# from KPIUtils_v2.Utils.Consts.PS import 
# from KPIUtils_v2.Utils.Consts.GlobalConsts import 
# from KPIUtils_v2.Utils.Consts.Messages import 
# from KPIUtils_v2.Utils.Consts.Custom import 
# from KPIUtils_v2.Utils.Consts.OldDB import 

# from KPIUtils_v2.Calculations.AssortmentCalculations import Assortment
# from KPIUtils_v2.Calculations.AvailabilityCalculations import Availability
# from KPIUtils_v2.Calculations.NumberOfScenesCalculations import NumberOfScenes
# from KPIUtils_v2.Calculations.PositionGraphsCalculations import PositionGraphs
# from KPIUtils_v2.Calculations.SOSCalculations import SOS
# from KPIUtils_v2.Calculations.SequenceCalculations import Sequence
# from KPIUtils_v2.Calculations.SurveyCalculations import Survey

# from KPIUtils_v2.Calculations.CalculationsUtils import GENERALToolBoxCalculations

__author__ = '%(author)s'


class %(scene_tool_box_class_name)s(GlobalSceneToolBox):

    def __init__(self, data_provider, output):
        GlobalSceneToolBox.__init__(self, data_provider, output)

    def main_function(self):
        score = 0
        return score
"""

# test and profiling:

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
# import pandas as pd
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

# planogram:

PLANOGRAM_CALCULATIONS_SCRIPT = """
from Trax.Apps.Services.KEngine.Handlers.Utils.Scripts import PlanogramBaseClass
from Projects.%(project_capital)s.%(planogram_generator_file_name)s import %(planogram_generator_class_name)s

__author__ = '%(author)s'


class PlanogramCalculations(PlanogramBaseClass):
    def __init__(self, data_provider):
        super(PlanogramCalculations, self).__init__(data_provider)
        self.planogram_generator = %(planogram_generator_class_name)s(self._data_provider)

    def calculate_planogram(self):
        self.planogram_generator.planogram_score()
"""

PLANOGRAM_GENERATOR_SCRIPT = """
from Trax.Utils.Logging.Logger import Log
from KPIUtils_v2.Utils.Decorators.Decorators import log_runtime

from Projects.%(project_capital)s.Utils.%(planogram_tool_box_file_name)s import %(planogram_tool_box_class_name)s

from KPIUtils_v2.DB.CommonV2 import Common


__author__ = '%(author)s'


class PlanogramGenerator:

    def __init__(self, data_provider, output=None):
        self.data_provider = data_provider
        self.output = output
        self.project_name = data_provider.project_name
        self.session_uid = self.data_provider.session_uid
        self.common = Common(data_provider)
        self.scene_tool_box = %(planogram_tool_box_class_name)s(self.data_provider, self.output, self.common)

    @log_runtime('Total Calculations', log_start=True)
    def planogram_score(self):
        if self.scene_tool_box.match_product_in_scene.empty:
            Log.warning('Match product in scene is empty for this scene')
        else:
            self.scene_tool_box.main_function()
            self.common.commit_results_data()
"""

PLANOGRAM_TOOLBOX_SCRIPT = """
from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Cloud.Services.Connector.Keys import DbUsers
from KPIUtils_v2.DB.PsProjectConnector import PSProjectConnector
# from Trax.Utils.Logging.Logger import Log
import pandas as pd
import os

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


class %(planogram_tool_box_class_name)s:

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
        self.planogram_item_facts = self.data_provider['planogram_item_facts']


    def main_function(self):
        score = 0
        return score
"""

# planogram status tags and finder:

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

# live scene:

LIVE_SCENE_CALCULATIONS_SCRIPT = """
from Trax.Apps.Services.KEngine.Handlers.Utils.Scripts import LiveSceneBaseClass
from Projects.%(project_capital)s.%(live_scene_generator_file_name)s import %(live_scene_generator_class_name)s

__author__ = '%(author)s'


class LiveSceneCalculations(LiveSceneBaseClass):
    def __init__(self, data_provider):
        super(LiveSceneCalculations, self).__init__(data_provider)
        self.live_scene_generator = %(live_scene_generator_class_name)s(self._data_provider)

    def calculate_scene_live_kpi(self):
        self.live_scene_generator.live_scene_score()

"""

LIVE_SCENE_GENERATOR_SCRIPT = """
from Trax.Utils.Logging.Logger import Log
from KPIUtils_v2.Utils.Decorators.Decorators import log_runtime

from Projects.%(project_capital)s.Utils.%(live_scene_tool_box_file_name)s import %(live_scene_tool_box_class_name)s

from KPIUtils_v2.DB.LiveCommon import LiveCommon


__author__ = '%(author)s'


class LiveSceneGenerator:

    def __init__(self, data_provider):
        self.data_provider = data_provider
        self.scene_uid = self.data_provider.level_uid
        self.common = LiveCommon(data_provider)
        self.live_scene_tool_box = %(live_scene_tool_box_class_name)s(self.data_provider, self.common)

    @log_runtime('Total Calculations', log_start=True)
    def live_scene_score(self):
        self.live_scene_tool_box.main_function()
"""

LIVE_SCENE_TOOLBOX_SCRIPT = """
from Trax.Utils.Logging.Logger import Log

__author__ = '%(author)s'


class %(live_scene_tool_box_class_name)s:

    def __init__(self, data_provider, common):
        self.data_provider = data_provider
        self.live_common = common
        self.project_name = self.data_provider.project_name
        self.scene_uid = self.data_provider.level_uid

    def main_function(self):
        score = 0
        return score
"""

# live session:

LIVE_SESSION_CALCULATIONS_SCRIPT = """
from Trax.Apps.Services.KEngine.Handlers.Utils.Scripts import LiveSessionBaseClass
from Projects.%(project_capital)s.%(live_session_generator_file_name)s import %(live_session_generator_class_name)s

__author__ = '%(author)s'


class LiveSessionCalculations(LiveSessionBaseClass):
    def __init__(self, data_provider):
        super(LiveSessionCalculations, self).__init__(data_provider)
        self.live_session_generator = %(live_session_generator_class_name)s(self._data_provider)

    def calculate_session_live_kpi(self):
        self.live_session_generator.live_session_score()

"""

LIVE_SESSION_GENERATOR_SCRIPT = """
from Trax.Utils.Logging.Logger import Log
from KPIUtils_v2.Utils.Decorators.Decorators import log_runtime

from Projects.%(project_capital)s.Utils.%(live_session_tool_box_file_name)s import %(live_session_tool_box_class_name)s

from KPIUtils_v2.DB.LiveCommon import LiveCommon


__author__ = '%(author)s'


class LiveSessionGenerator:

    def __init__(self, data_provider):
        self.data_provider = data_provider
        self.session_uid = self.data_provider.level_uid
        self.common = LiveCommon(data_provider)
        self.live_session_tool_box = %(live_session_tool_box_class_name)s(self.data_provider, self.common)

    @log_runtime('Total Calculations', log_start=True)
    def live_session_score(self):
        self.live_session_tool_box.main_function()
"""

LIVE_SESSION_TOOLBOX_SCRIPT = """
from Trax.Utils.Logging.Logger import Log


__author__ = '%(author)s'


class %(live_session_tool_box_class_name)s:

    def __init__(self, data_provider, common):
        self.data_provider = data_provider
        self.live_common = common
        self.project_name = self.data_provider.project_name
        self.session_uid = self.data_provider.level_uid

    def main_function(self):
        score = 0
        return score
"""

# functions:


def get_project_name_and_directory_name(project):
    project_name = project.lower().replace('_', '-')
    project_capital = project_name.upper().replace('-', '_')
    return project_name, project_capital
