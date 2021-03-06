
import os

from KPIUtils_v2.DB.PsProjectConnector import PSProjectConnector
from Trax.Data.Testing.SeedNew import Seeder
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Cloud.Services.Connector.Keys import DbUsers
from Trax.Data.Testing.TestProjects import TestProjectsNames

from Tests.Data.TestData.test_data_sanofikh_sanity import ProjectsSanityData
from Projects.SANOFIKH.Calculations import SANOFIKHCalculations
from Trax.Apps.Core.Testing.BaseCase import TestFunctionalCase
from Trax.Utils.Testing.Case import skip

__author__ = 'jasmineg'


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
        cursor = connector.db.cursor()
        cursor.execute('''
        SELECT * FROM report.kpi_level_2_results
        ''')
        kpi_results = cursor.fetchall()
        self.assertNotEquals(len(kpi_results), 0)
        connector.disconnect_rds()

    @skip('sup')
    @seeder.seed(["sanofikh_seed"], ProjectsSanityData())
    def test_sanofikh_sanity(self):
        project_name = ProjectsSanityData.project_name
        data_provider = KEngineDataProvider(project_name)
        sessions = ['9DA101ED-9A63-49AE-89E8-0098AC1E36D5']
        for session in sessions:
            data_provider.load_session_data(session)
            output = Output()
            SANOFIKHCalculations(data_provider, output).run_project_calculations()
            self._assert_kpi_results_filled()
