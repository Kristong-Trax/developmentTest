
import os
import MySQLdb

from KPIUtils_v2.DB.PsProjectConnector import PSProjectConnector
from Trax.Data.Testing.SeedNew import Seeder
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Cloud.Services.Connector.Keys import DbUsers
from Trax.Data.Testing.TestProjects import TestProjectsNames

from Tests.Data.TestData.test_data_penaflorar_sanity import ProjectsSanityData
from Projects.PENAFLORAR.Calculations import PENAFLORARCalculations
from Trax.Apps.Core.Testing.BaseCase import TestFunctionalCase
from Tests.TestUtils import remove_cache_and_storage

__author__ = 'avrahama'


class TestKEngineOutOfTheBox(TestFunctionalCase):

    def set_up(self):
        super(TestKEngineOutOfTheBox, self).set_up()
        self.mock_object('save_latest_templates', path='KPIUtils.DIAGEO.ToolBox.DIAGEOToolBox')
        self.mock_object('save_level2_and_level3', path='Projects.PENAFLORAR.Utils.KPIToolBox.PENAFLORARToolBox')
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
    
    @seeder.seed(["mongodb_products_and_brands_seed", "penaflorar_seed"], ProjectsSanityData())
    def test_penaflorar_sanity(self):
        project_name = ProjectsSanityData.project_name
        data_provider = KEngineDataProvider(project_name)
        sessions = ['0949f439-185e-48c1-bfc1-8a1230413ef2']
        for session in sessions:
            data_provider.load_session_data(session)
            output = Output()
            PENAFLORARCalculations(data_provider, output).run_project_calculations()
            self._assert_kpi_results_filled()
