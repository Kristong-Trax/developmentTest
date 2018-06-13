
import os
import shutil
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Trax.Utils.Logging.Logger import Log

__author__ = 'yoava'

MAIN_FILE_NAME = 'Calculations'
GENERATOR_FILE_NAME = 'KPIGenerator'
FETCHER_FILE_NAME = 'Fetcher'
TOOL_BOX_FILE_NAME = 'KPIToolBox'
LOCAL_CALCULATIONS_FILE_NAME = 'LocalCalculations'
PROFILING_SCRIPT_NAME = 'gen_profiling'

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

# Author: Ilan P

# this scripts gen an .svg file to see clearly the code flow execution for profiling
# between imports

# HOW TO USE
# ============
# just provide the project name which you want to create a execution graph . example " ./gen_profiling.sh CCBR_SAND"
# the svg file will be in the folder project folder DO NOT PUSH IT TO THE GIT


dir=$PWD

parentdir="$(dirname "$dir")"

PROJECT=${parentdir##*/}


cd .. && cd .. && cd .. && python -m cProfile -o 1.stats ~/dev/kpi_factory/Projects/$PROJECT/Calculations.py -e prod -c ~/dev/theGarage/Trax/Apps/Services/KEngine/k-engine-prod.config
gprof2dot -f pstats 1.stats -o 1.dot
dot -Tsvg -Gdpi=70 -o ${PROJECT}_profiling.svg 1.dot

mv ~/dev/kpi_factory/1.dot $dir/1.dot
mv ~/dev/kpi_factory/1.stats $dir/1.stats
mv ~/dev/kpi_factory/${PROJECT}_profiling.svg $dir/${PROJECT}_profiling.svg

"""


class CreateKPIProject:

    def __init__(self, project_name):
        self.project = project_name.lower().replace('_', '-')
        self.project_capital = self.project.upper().replace('-', '_')
        self.project_short = self.project_capital.split('_')[0]
        self.author = os.environ.get('USER', '')
        self.project_path = \
            '{}/Projects/{}/'.format(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), self.project_capital)
        self.create_project_directory()

    def create_project_directory(self):
        if os.path.exists(self.project_path):
            shutil.rmtree(self.project_path)
        os.mkdir(self.project_path)
        with open(self.project_path + '__init__.py', 'wb') as f:
            f.write('')

    def create_new_project(self):
        files_to_create = {'': [(MAIN_FILE_NAME, MAIN_FILE),
                                (LOCAL_CALCULATIONS_FILE_NAME, LOCAL_FILE),
                                (GENERATOR_FILE_NAME, GENERATOR)],
                           'Utils': [(TOOL_BOX_FILE_NAME, TOOL_BOX),
                                     ],
                           'Profiling': [(PROFILING_SCRIPT_NAME, PROFILING_SCRIPT)]}

        formatting_dict = {'author': self.author,
                           'project': self.project,
                           'project_capital': self.project_capital,
                           'generator_file_name': GENERATOR_FILE_NAME,
                           'generator_class_name': 'Generator',
                           'tool_box_file_name': TOOL_BOX_FILE_NAME,
                           'tool_box_class_name': '{}ToolBox'.format(self.project_short),
                           'main_file_name': MAIN_FILE_NAME,
                           'main_class_name': '{}Calculations'.format(self.project_short)
                           }
        for directory in files_to_create.keys():
            if directory:
                directory_path = self.project_path + directory + '/'
                os.mkdir(directory_path)
                with open(directory_path + '__init__.py', 'wb') as f:
                    f.write('')
            else:
                directory_path = self.project_path
            for file_name, file_content in files_to_create[directory]:
                if directory == 'Profiling':
                    with open(directory_path + file_name + '.sh', 'wb') as f:
                        f.write(file_content)
                else:
                    with open(directory_path + file_name + '.py', 'wb') as f:
                        f.write(file_content % formatting_dict)

        data_directory = os.path.join(self.project_path, 'Data')
        if not os.path.exists(data_directory):
            os.makedirs(data_directory)


if __name__ == '__main__':

    LoggerInitializer.init('Creating new project')
    project = 'test1'
    Log.info("project name : " + project)
    new = CreateKPIProject(project)
    new.create_new_project()
    Log.info("project : " + project + " was created successfully")
