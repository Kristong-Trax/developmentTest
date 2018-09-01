
import os
import MySQLdb

from Trax.Data.Projects.Connector import ProjectConnector
from Trax.Data.Testing.SeedNew import Seeder
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Cloud.Services.Connector.Keys import DbUsers
from Trax.Data.Testing.TestProjects import TestProjectsNames
from Trax.Utils.Testing.Case import MockingTestCase
from mock import patch

from Tests.Data.Templates.paneflorar_sand.MPA import mpa
from Tests.Data.Templates.paneflorar_sand.NewProducts import products
from Tests.Data.TestData.test_data_penaflorar_sand_sanity import ProjectsSanityData
from Projects.PENAFLORAR_SAND.Calculations import PENAFLORAR_SANDDIAGEOARCalculations
from Trax.Apps.Core.Testing.BaseCase import TestMockingFunctionalCase


__author__ = 'yoava'


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

    @patch('Projects.PENAFLORAR_SAND.Utils.ToolBox.PENAFLORAR_SANDDIAGEOToolBox.get_latest_directory_date_from_cloud',
           return_value='2018-02-20')
    @patch('Projects.PENAFLORAR_SAND.Utils.ToolBox.PENAFLORAR_SANDDIAGEOToolBox.save_latest_templates')
    @patch('Projects.PENAFLORAR_SAND.Utils.ToolBox.PENAFLORAR_SANDDIAGEOToolBox.download_template',
           return_value=mpa)
    @patch('Projects.PENAFLORAR_SAND.Utils.ToolBox.PENAFLORAR_SANDDIAGEOToolBox.download_template',
           return_value=products)
    @seeder.seed(["penaflorar_sand_seed"], ProjectsSanityData())
    def test_penaflorar_sand_sanity(self, x, y, json, json2):
        project_name = ProjectsSanityData.project_name
        data_provider = KEngineDataProvider(project_name)
        sessions = ['cb1f5608-488a-4eb3-bb50-68298b255390']
        for session in sessions:
            data_provider.load_session_data(session)
            output = Output()
            PENAFLORAR_SANDDIAGEOARCalculations(data_provider, output).run_project_calculations()
            self._assert_kpi_results_filled()
