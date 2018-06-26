from Trax.Cloud.Services.Connector.Keys import DbUsers
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Trax.Data.Projects.Connector import ProjectConnector
from Trax.Utils.Conf.Configuration import Config

import os
import pandas as pd
import shutil

from Trax.Utils.Logging.Logger import Log

__author__ = 'yoava'


class SeedCreator:
    def __init__(self, project):
        self.project = project
        self.rds_conn = ProjectConnector(project, DbUsers.CalculationEng)
        self.user = os.environ.get('USER')
        self.exporter_outputs_dir = os.path.join('/home', self.user, 'Documents', 'exporter')
        if not os.path.exists(self.exporter_outputs_dir):
            os.makedirs(self.exporter_outputs_dir)
        self.output_dir = os.path.join(self.exporter_outputs_dir, project)
        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir)
        self.rds_name = self.rds_conn.project_params['rds_name']
        self.seed_name = '{}_seed.sql.gz'.format(project)
        self.export_dir = os.path.join('/home', self.user, 'dev', 'traxdatabase', 'traxExport')

    def get_top_sessions(self):
        Log.info('Fetching 3 sessions')
        query = """
        select session_uid from probedata.session where status = 
        'Completed' order by visit_date, number_of_scenes desc limit 3;
        """
        sessions_df = pd.read_sql(query, self.rds_conn.db)
        Log.info(str(sessions_df.values))
        return sessions_df.session_uid.values

    def activate_exporter(self):
        os.chdir(self.export_dir)
        sessions = self.get_top_sessions()
        Log.info('Activating exporter')
        export_command = """./traxExportIntuition.sh {0} {1} session_uid {2},{3},{4}""".\
            format(self.rds_name, self.output_dir, sessions[0], sessions[1], sessions[2])
        os.system(export_command)
        os.chdir(self.output_dir)
        os.rename('dump.sql.gz', self.seed_name)
        shutil.copy2(os.path.join(self.output_dir, self.seed_name),
                     os.path.join('/home', self.user, 'dev', 'kpi_factory', 'Tests', 'Data', self.seed_name))
        Log.info('Done')


class SanityTestsCreator:

    def __init__(self, project):
        self.project = project.lower().replace('_', '-')
        self.project_capital = self.project.upper().replace('-', '_')
        self.user = os.environ.get('USER')
        self.project_short = self.project_capital.split('_')[0]
        self.main_class_name = '{}Calculations'.format(self.project_short)

    def get_test_class(self):
        test_class = """
        
import os
from Trax.Data.Projects.Connector import ProjectConnector
from Trax.Data.Testing.SeedNew import Seeder
import MySQLdb
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider
from Trax.Cloud.Services.Connector.Keys import DbUsers
from Trax.Data.Testing.TestProjects import TestProjectsNames
from Trax.Utils.Testing.Case import MockingTestCase

from Tests.Data.test_data_project_sanity import ProjectsSanityData

__author__ = '%(author)s'

class TestKEngineOutOfTheBox(MockingTestCase):

    @property
    def import_path(self):
        return 'Trax.Apps.Services.KEngine.Handlers.SessionHandler'
    
    @property
    def config_file_path(self):
        return os.path.join(os.path.dirname(os.path.realpath(__file__)), 'k-engine-test.config')
    
    seeder = Seeder()
    
    def _assert_kpi_results_filled(self):
        connector = ProjectConnector(TestProjectsNames().TEST_PROJECT_1, DbUsers.Docker)
        cursor = connector.db.cursor(MySQLdb.cursors.DictCursor)
        cursor.execute('''
        SELECT * FROM report.kpi_results
        ''')
        kpi_results = cursor.fetchall()
        self.assertNotEquals(len(kpi_results), 0)
        connector.disconnect_rds()
    
    
    @seeder.seed(["ccru_seed"], ProjectsSanityData())
    def test_ccru_sanity(self):
        project_name = ProjectsSanityData.project_name
        data_provider = KEngineDataProvider(project_name)
        sessions = ['8DD169D2-EFE1-4B5F-8DA7-A805EADA17B7']
        for session in sessions:
            data_provider.load_session_data(session)
            output = Output()
            %{main_class_name}s(data_provider, output).run_project_calculations()
            self._assert_kpi_results_filled()
        """

        formatting_dict = {'author': self.user,
                           'project': self.project,
                           'project_capital': self.project_capital,
                           'generator_file_name': 'KPIGenerator',
                           'generator_class_name': 'Generator',
                           'tool_box_file_name': 'KPIToolBox',
                           'tool_box_class_name': '{}ToolBox'.format(self.project_short),
                           'main_file_name': self.main_class_name,
                           'main_class_name': '{}Calculations'.format(self.project_short)
                           }

        test_path = ('/home/yoava/dev/kpi_factory/Tests/{0}'.format(self.project))
        with open(test_path + '.py', 'wb') as f:
            f.write(test_class % formatting_dict)

if __name__ == '__main__':
    LoggerInitializer.init('')
    Config.init()
    # creator = SeedCreator('inbevtradmx')
    # creator.activate_exporter()
    # creator.rds_conn.disconnect_rds()
    sanity = SanityTestsCreator('ccru')
    sanity.get_test_class()
