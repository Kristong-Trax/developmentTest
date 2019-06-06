from Trax.Cloud.Services.Connector.Keys import DbUsers
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from KPIUtils_v2.DB.PsProjectConnector import PSProjectConnector
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


class SeedCreator:
    TOP_SESSIONS = []

    """
    this class creates seed file
    """
    def __init__(self, project):
        self.project = project
        self.rds_conn = PSProjectConnector(project, DbUsers.CalculationEng)
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

    def _set_top_sessions_from_db(self, number_of_sessions):
        """
        this method gets top sessions from the project db to test
        :return: list of sessions
        """
        Log.info('Fetching {} sessions'.format(number_of_sessions))
        query = """
        SELECT s.session_uid, s.pk FROM probedata.session s
        join reporting.scene_item_facts r on r.session_id = s.pk
        WHERE
            status = 'Completed' 
            and r.session_id is not null  
            ORDER BY s.visit_date , s.number_of_scenes DESC
            LIMIT {};
        """.format(number_of_sessions)
        sessions_df = pd.read_sql(query, self.rds_conn.db)
        for session in sessions_df.session_uid.values:
            self.TOP_SESSIONS.append(session)

    def _set_top_sessions_from_specific_sessions(self, specific_sessions):
        for session_uid in specific_sessions:
            self.TOP_SESSIONS.append(session_uid)

    def build_export_command(self):
        export_command = """./traxExportIntuition.sh {0} {1} session_uid """.format(self.rds_name, self.output_dir)
        for session_uid in self.TOP_SESSIONS:
            export_command = export_command.__add__(session_uid)
            if session_uid != self.TOP_SESSIONS[len(self.TOP_SESSIONS) - 1]:
                export_command += ','
        return export_command

    def activate_exporter(self, number_of_sessions=1, specific_sessions=None):
        """
        this method build a dump file with traxExporter from the given sessions
        :param number_of_sessions: number of sessions in cae we want to fetch them from db
        :param specific_sessions: list of sessions if we already know the sessions to test
        :return: None
        """
        os.chdir(self.export_dir)
        if specific_sessions is not None:
            self._set_top_sessions_from_specific_sessions(specific_sessions)
        else:
            self._set_top_sessions_from_db(number_of_sessions)
        Log.info('Activating exporter')
        export_command = self.build_export_command()
        Log.info(export_command)
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

from KPIUtils_v2.DB.PsProjectConnector import PSProjectConnector
from Trax.Data.Testing.SeedNew import Seeder
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Cloud.Services.Connector.Keys import DbUsers
from Trax.Data.Testing.TestProjects import TestProjectsNames
from Trax.Utils.Testing.Case import MockingTestCase

from Tests.Data.TestData.test_data_%(project)s_sanity import ProjectsSanityData
from Projects.%(project_capital)s.Calculations import %(main_class_name)s
from Trax.Apps.Core.Testing.BaseCase import TestFunctionalCase


__author__ = '%(author)s'


class TestKEngineOutOfTheBox(TestFunctionalCase):

    @property
    def import_path(self):
        return 'Trax.Apps.Services.KEngine.Handlers.SessionHandler'
    
    @property
    def config_file_path(self):
        return os.path.join(os.path.dirname(os.path.realpath(__file__)), 'k-engine-test.config')
    
    seeder = Seeder()
    
    def _assert_kpi_results_filled(self):
        connector = PSProjectConnector(TestProjectsNames().TEST_PROJECT_1, DbUsers.Docker)
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
        sessions = [%(sessions)s]
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
                           'seed': '{}_seed'.format(self.project.replace('-', '_')),
                           'project': self.project,
                           'sessions': self._get_session_list_as_str(),
                           }

        test_path = ('/home/{0}/dev/kpi_factory/Tests/test_functional_{1}_sanity'.format(self.user, self.project))
        with open(test_path + '.py', 'wb') as f:
            f.write(SanityTestsCreator.TEST_CLASS % formatting_dict)

    def _get_session_list_as_str(self):
        sessions_str = ""
        for session_uid in self.session_list:
            sessions_str = sessions_str.__add__("'{}'".format(session_uid))
            if session_uid != self.session_list[len(self.session_list) - 1]:
                sessions_str += ', '
        return sessions_str


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
                                                                                         self.project.
                                                                                         replace('-', '_')))

        with open(data_class_path + '.py', 'wb') as f:
            f.write(data_class_content)


if __name__ == '__main__':
    LoggerInitializer.init('')
    Config.init()
    project_to_test = 'gskau'
    creator = SeedCreator(project_to_test)
    creator.activate_exporter(specific_sessions=
                              ['C804124E-7223-446A-A084-A143E45D94D8'])
    creator.rds_conn.disconnect_rds()
    data_class = CreateTestDataProjectSanity(project_to_test)
    data_class.create_data_class()
    sanity = SanityTestsCreator(project_to_test, creator.TOP_SESSIONS)
    sanity.create_test_class()
