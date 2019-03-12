
import os
import MySQLdb

from KPIUtils_v2.DB.PsProjectConnector import PSProjectConnector
from Trax.Data.Testing.SeedNew import Seeder
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Cloud.Services.Connector.Keys import DbUsers
from Trax.Data.Testing.TestProjects import TestProjectsNames
from Trax.Utils.Testing.Case import MockingTestCase
from mock import patch

from Tests.Data.Templates.heinikencn.Availability import availability
from Tests.Data.TestData.test_data_heinekencn_sanity import ProjectsSanityData
from Projects.HEINEKENCN.Calculations import HEINEKENCNCalculations
from Trax.Apps.Core.Testing.BaseCase import TestMockingFunctionalCase

from Tests.TestUtils import remove_cache_and_storage

__author__ = 'yoava'


class TestKEngineOutOfTheBox(TestMockingFunctionalCase):
    
    def set_up(self):
        super(TestKEngineOutOfTheBox, self).set_up()
        remove_cache_and_storage()
    
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

    @patch('Projects.HEINEKENCN.Utils.ToolBox.HandleTemplate.get_latest_directory_date_from_cloud',
           return_value='2018-05-18')
    @patch('Projects.HEINEKENCN.Utils.ToolBox.HandleTemplate.save_latest_templates')
    @patch('Projects.HEINEKENCN.Utils.ToolBox.HandleTemplate.download_template',
           return_value=availability)
    @seeder.seed(["heinekencn_seed"], ProjectsSanityData())
    def test_heinekencn_sanity(self, x, y, json):
        project_name = ProjectsSanityData.project_name
        data_provider = KEngineDataProvider(project_name)
        sessions = ['f7c3b29d-a5fc-457e-be65-0bae8b7f7202']
        for session in sessions:
            data_provider.load_session_data(session)
            output = Output()
            HEINEKENCNCalculations(data_provider, output).run_project_calculations()
            self._assert_kpi_results_filled()
