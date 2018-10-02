
import os
import MySQLdb

from Trax.Data.Projects.Connector import ProjectConnector
from Trax.Data.Testing.SeedNew import Seeder
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Cloud.Services.Connector.Keys import DbUsers
from Trax.Data.Testing.TestProjects import TestProjectsNames
from Trax.Utils.Testing.Case import MockingTestCase
from mock import patch

from Tests.Data.Templates.diageouk.LocalMPA import LocalMPA
from Tests.Data.Templates.diageouk.MPA import MPA
from Tests.Data.Templates.diageouk.NewProducts import Products
from Tests.Data.Templates.diageouk.POSM import POSM
from Tests.Data.Templates.diageouk.RelativePosition import Position
from Tests.Data.TestData.test_data_diageouk_sand_sanity import ProjectsSanityData
from Projects.DIAGEOUK_SAND.Calculations import DIAGEOUK_SANDCalculations
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

    @patch('Projects.DIAGEOUK_SAND.Utils.ToolBox.DIAGEOUK_SANDDIAGEOToolBox.get_latest_directory_date_from_cloud',
           return_value='2018-05-18')
    @patch('Projects.DIAGEOUK_SAND.Utils.ToolBox.DIAGEOUK_SANDDIAGEOToolBox.save_latest_templates')
    @patch('Projects.DIAGEOUK_SAND.Utils.ToolBox.DIAGEOUK_SANDDIAGEOToolBox.download_template',
           return_value=LocalMPA)
    @patch('Projects.DIAGEOUK_SAND.Utils.ToolBox.DIAGEOUK_SANDDIAGEOToolBox.download_template',
           return_value=MPA)
    @patch('Projects.DIAGEOUK_SAND.Utils.ToolBox.DIAGEOUK_SANDDIAGEOToolBox.download_template',
           return_value=Products)
    @patch('Projects.DIAGEOUK_SAND.Utils.ToolBox.DIAGEOUK_SANDDIAGEOToolBox.download_template',
           return_value=POSM)
    @patch('Projects.DIAGEOUK_SAND.Utils.ToolBox.DIAGEOUK_SANDDIAGEOToolBox.download_template',
           return_value=Position)
    @seeder.seed(["diageouk_sand_seed"], ProjectsSanityData())
    def test_diageouk_sand_sanity(self, x, y, json, json2, json3, json4, json5):
        project_name = ProjectsSanityData.project_name
        data_provider = KEngineDataProvider(project_name)
        sessions = ['DA5DB45A-98A7-45AA-8497-F853C32CD0E7']
        for session in sessions:
            data_provider.load_session_data(session)
            output = Output()
            DIAGEOUK_SANDCalculations(data_provider, output).run_project_calculations()
            self._assert_kpi_results_filled()
