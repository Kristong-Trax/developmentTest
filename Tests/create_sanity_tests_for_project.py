from Trax.Cloud.Services.Connector.Keys import DbUsers
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Trax.Data.Projects.Connector import ProjectConnector
from Trax.Utils.Conf.Configuration import Config
from Trax.Utils.Logging.Logger import Log

import os
import pandas as pd
import shutil

__author__ = 'yoava'

"""
this module creates dump file and sanity test classes for a specific project.
all you have to do is to insert the project name and run it 
"""

TOP_SESSIONS = []


class SeedCreator:
    """
    this class creates seed file
    """
    def __init__(self, project):
        self.project = project
        self.rds_conn = ProjectConnector(project, DbUsers.CalculationEng)
        self.user = os.environ.get('USER')
        self.exporter_outputs_dir = os.path.join('/home', self.user, 'exporter')
        if not os.path.exists(self.exporter_outputs_dir):
            os.makedirs(self.exporter_outputs_dir)
        self.output_dir = os.path.join(self.exporter_outputs_dir, project)
        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir)
        self.rds_name = self.rds_conn.project_params['rds_name']
        self.seed_name = '{}_seed.sql.gz'.format(project.replace('-', '_'))
        self.export_dir = os.path.join('/home', self.user, 'dev', 'traxdatabase', 'traxExport')

    def get_top_sessions(self):
        """
        this method gets top sessions from the project db to test
        :return: list of sessions
        """
        Log.info('Fetching 3 sessions')
        query = """
        SELECT s.session_uid, s.pk FROM probedata.session s
        join reporting.scene_item_facts r on r.session_id = s.pk
        WHERE
            status = 'Completed' 
            and r.session_id is not null  
            ORDER BY s.visit_date , s.number_of_scenes DESC
            LIMIT 1;
        """
        sessions_df = pd.read_sql(query, self.rds_conn.db)
        Log.info(str(sessions_df.values))
        for session in sessions_df.session_uid.values:
            TOP_SESSIONS.append(session)
        return sessions_df.session_uid.values

    def get_specific_session(self, session_uid):
        TOP_SESSIONS.append(session_uid)
        return [session_uid]

    def activate_exporter(self, specific_session=None):
        """
        this method build a dump file with traxExporter from the given sessions
        :return: None
        """
        os.chdir(self.export_dir)
        if specific_session is not None:
            sessions = self.get_specific_session(specific_session)
        else:
            sessions = self.get_top_sessions()
        Log.info('Activating exporter')
        export_command = """./traxExportIntuition.sh {0} {1} session_uid {2}""". \
            format(self.rds_name, self.output_dir, sessions[0])
        os.system(export_command)
        os.chdir(self.output_dir)
        os.rename('dump.sql.gz', self.seed_name)
        shutil.copy2(os.path.join(self.output_dir, self.seed_name),
                     os.path.join('/home', self.user, 'dev', 'kpi_factory', 'Tests', 'Data', 'Seeds', self.seed_name))
        Log.info('Done')


class SanityTestsCreator:
    """
    this class creates the sanity tests class
    """
    TEST_CLASS = """
import os
import MySQLdb

from Trax.Data.Projects.Connector import ProjectConnector
from Trax.Data.Testing.SeedNew import Seeder
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Cloud.Services.Connector.Keys import DbUsers
from Trax.Data.Testing.TestProjects import TestProjectsNames
from Trax.Utils.Testing.Case import MockingTestCase

from Tests.Data.TestData.test_data_%(project)s_sanity import ProjectsSanityData
from Projects.%(project_capital)s.Calculations import %(main_class_name)s
from Trax.Apps.Core.Testing.BaseCase import TestMockingFunctionalCase


__author__ = '%(author)s'


class TestKEngineOutOfTheBox(TestMockingFunctionalCase):

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
    
    @seeder.seed(["%(seed)s"], ProjectsSanityData())
    def test_%(project)s_sanity(self):
        project_name = ProjectsSanityData.project_name
        data_provider = KEngineDataProvider(project_name)
        sessions = ['%(session_0)s']
        for session in sessions:
            data_provider.load_session_data(session)
            output = Output()
            %(main_class_name)s(data_provider, output).run_project_calculations()
            self._assert_kpi_results_filled()
"""

    def __init__(self, project, session_list):
        self.project = project.lower().replace('-', '_')
        self.project_capital = self.project.upper().replace('-', '_')
        self.user = os.environ.get('USER')
        self.project_short = self.project_capital.split('_')[0]
        self.main_class_name = '{}Calculations'.format(self.project_capital)
        self.session_list = session_list

    def create_test_class(self):
        """
        this method create sanity test class
        :return: None
        """
        formatting_dict = {'author': self.user,
                           'main_class_name': self.main_class_name,
                           'project_capital': self.project_capital,
                           'seed': '{}_seed'.format(self.project.replace('-','_')),
                           'project': self.project,
                           'session_0': self.session_list[0],
                           # 'session_1': self.session_list[1],
                           # 'session_2': self.session_list[2]
                           }

        test_path = ('/home/{0}/dev/kpi_factory/Tests/test_functional_{1}_sanity'.format(self.user, self.project))
        with open(test_path + '.py', 'wb') as f:
            f.write(SanityTestsCreator.TEST_CLASS % formatting_dict)


class CreateTestDataProjectSanity:
    """
    this classs creates project sanity data class
    """
    def __init__(self, project):
        self.project = project
        self.user = os.environ.get('USER')

    def create_data_class(self):
        """
        this method creates the data class
        :return:  None
        """
        seed_data = """DATA_TYPE: BaseSeedData.MYSQL,
                        FILES_RELATIVE_PATH: ['Data/Seeds/{}_seed.sql.gz'],
                        PROJECT_NAME: project_name
                """.format(self.project.replace('-', '_'))
        seed_data = '{' + seed_data

        seed_data = seed_data + '        }'

        data_class_content = """
from Trax.Data.Testing.Resources import BaseSeedData, DATA_TYPE, FILES_RELATIVE_PATH
from Trax.Data.Testing.TestProjects import TestProjectsNames
from Trax.Utils.Environments.DockerGlobal import PROJECT_NAME

__author__ = '{0}'


class ProjectsSanityData(BaseSeedData):
    project_name = TestProjectsNames().TEST_PROJECT_1
    {1}_seed = {2} 
""".format(self.user, self.project.replace('-', '_'), seed_data)

        data_class_path = \
            ('/home/{0}/dev/kpi_factory/Tests/Data/TestData/test_data_{1}_sanity'.format(self.user,
                                                                                self.project.replace('-', '_')))

        with open(data_class_path + '.py', 'wb') as f:
            f.write(data_class_content)


if __name__ == '__main__':
    LoggerInitializer.init('')
    Config.init()
    project_to_test = 'ccbza'
    creator = SeedCreator(project_to_test)
    creator.activate_exporter()
    creator.rds_conn.disconnect_rds()
    data_class = CreateTestDataProjectSanity(project_to_test)
    data_class.create_data_class()
    sanity = SanityTestsCreator(project_to_test, TOP_SESSIONS)
    sanity.create_test_class()
