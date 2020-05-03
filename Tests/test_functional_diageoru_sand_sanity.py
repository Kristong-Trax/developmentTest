
import os

from KPIUtils_v2.DB.PsProjectConnector import PSProjectConnector
from Trax.Data.Testing.SeedNew import Seeder
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Cloud.Services.Connector.Keys import DbUsers
from Trax.Data.Testing.TestProjects import TestProjectsNames
from Trax.Utils.Testing.Case import skip
from mock import patch

from Tests.Data.TestData.test_data_diageoru_sand_sanity import ProjectsSanityData
from Projects.DIAGEORU_SAND.Calculations import DIAGEORUCalculations
from Trax.Apps.Core.Testing.BaseCase import TestFunctionalCase

from Tests.TestUtils import remove_cache_and_storage

__author__ = 'yoava'


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
        cursor = connector.db.cursor()
        cursor.execute('''
        SELECT * FROM report.kpi_results
        ''')
        kpi_results = cursor.fetchall()
        self.assertNotEquals(len(kpi_results), 0)
        connector.disconnect_rds()

    # @skip('temp')
    @skip('Test failed in garage')
    @patch('KPIUtils.DIAGEO.ToolBox.DIAGEOToolBox.get_latest_directory_date_from_cloud',
           return_value='2018-05-15')
    @patch('KPIUtils.DIAGEO.ToolBox.DIAGEOToolBox.save_latest_templates')
    @seeder.seed(["diageoru_sand_seed"], ProjectsSanityData())
    def test_diageoru_sand_sanity(self, x, y):
        project_name = ProjectsSanityData.project_name
        data_provider = KEngineDataProvider(project_name)
        sessions = ['d788328d-3814-42ae-ae6e-6c22a44a8780']
        for session in sessions:
            data_provider.load_session_data(session)
            output = Output()
            DIAGEORUCalculations(data_provider, output).run_project_calculations()
            self._assert_kpi_results_filled()
