
import os
import MySQLdb

from KPIUtils_v2.DB.PsProjectConnector import PSProjectConnector
from Trax.Data.Testing.SeedNew import Seeder
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Cloud.Services.Connector.Keys import DbUsers
from Trax.Data.Testing.TestProjects import TestProjectsNames

from Tests.Data.TestData.test_data_diageotw_sanity import ProjectsSanityData
from Projects.DIAGEOTW.Calculations import DIAGEOTWCalculations
from Trax.Apps.Core.Testing.BaseCase import TestFunctionalCase

from mock import patch
from Tests.Data.Templates.diageomx.MPA import mpa
from Tests.Data.Templates.diageomx.NewProducts import products
from Tests.Data.Templates.diageomx.POSM import posm
from Tests.Data.Templates.diageomx.RelativePosition import position
from Tests.TestUtils import remove_cache_and_storage

__author__ = 'avrahama'


class TestKEngineOutOfTheBox(TestFunctionalCase):

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
        SELECT * FROM report.kpi_level_2_results
        ''')
        kpi_results = cursor.fetchall()
        self.assertNotEquals(len(kpi_results), 0)
        connector.disconnect_rds()

    @patch('KPIUtils.DIAGEO.ToolBox.DIAGEOToolBox.get_latest_directory_date_from_cloud',
           return_value='2018-05-18')
    @patch('KPIUtils.DIAGEO.ToolBox.DIAGEOToolBox.save_latest_templates')
    @patch('KPIUtils.DIAGEO.ToolBox.DIAGEOToolBox.download_template',
           return_value=mpa)
    @patch('KPIUtils.DIAGEO.ToolBox.DIAGEOToolBox.download_template',
           return_value=products)
    @patch('KPIUtils.DIAGEO.ToolBox.DIAGEOToolBox.download_template',
           return_value=position)
    @patch('KPIUtils.DIAGEO.ToolBox.DIAGEOToolBox.download_template',
           return_value=posm)

    @seeder.seed(["mongodb_products_and_brands_seed", "diageotw_seed"], ProjectsSanityData())
    def test_diageotw_sanity(self, x, y, json, json2, json3, json4):
        project_name = ProjectsSanityData.project_name
        data_provider = KEngineDataProvider(project_name)
        sessions = ['E9C9D024-5CD2-46F1-A759-2E527207B161']
        for session in sessions:
            data_provider.load_session_data(session)
            output = Output()
            DIAGEOTWCalculations(data_provider, output).run_project_calculations()
            self._assert_kpi_results_filled()
