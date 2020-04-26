import os
from KPIUtils_v2.DB.PsProjectConnector import PSProjectConnector
from Trax.Data.Testing.SeedNew import Seeder
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Cloud.Services.Connector.Keys import DbUsers
from Trax.Data.Testing.TestProjects import TestProjectsNames
from Tests.Data.TestData.test_data_diageoco_sanity import ProjectsSanityData
from Projects.DIAGEOCO.Calculations import DIAGEOCOCalculations
from Trax.Apps.Core.Testing.BaseCase import TestFunctionalCase
from Trax.Utils.Testing.Case import skip
from Tests.TestUtils import remove_cache_and_storage

__author__ = 'avrahama'


class TestKEngineOutOfTheBox(TestFunctionalCase):

    def set_up(self):
        super(TestKEngineOutOfTheBox, self).set_up()
        self.mock_object('save_latest_templates',
                         path='KPIUtils.GlobalProjects.DIAGEO.Utils.TemplatesUtil.TemplateHandler')
        self.mock_object('download_template',
                         path='KPIUtils.GlobalProjects.DIAGEO.Utils.TemplatesUtil.TemplateHandler')
        remove_cache_and_storage()

    @property
    def import_path(self):
        return 'Trax.Apps.Services.KEngine.Handlers.SessionHandler'

    @property
    def config_file_path(self):
        return os.path.join(os.path.dirname(os.path.realpath(__file__)), 'k-engine-test.config')

    seeder = Seeder()

    def _assert_kpi_results_filled(self):
        """tests if get results after running the KPIs"""
        connector = PSProjectConnector(TestProjectsNames().TEST_PROJECT_1, DbUsers.Docker)
        cursor = connector.db.cursor()
        cursor.execute('''
        SELECT * FROM report.kpi_level_2_results
        ''')
        kpi_results = cursor.fetchall()
        self.assertNotEquals(len(kpi_results), 0)
        connector.disconnect_rds()

    # @seeder.seed(["mongodb_products_and_brands_seed", "diageoco_seed"], ProjectsSanityData())
    @skip('Will be restored soon')
    def test_diageoco_sanity(self):
        project_name = ProjectsSanityData.project_name
        data_provider = KEngineDataProvider(project_name)
        sessions = ['31fed918-37f0-449e-903b-be8ea233c80d']
        for session in sessions:
            data_provider.load_session_data(session)
            output = Output()
            DIAGEOCOCalculations(data_provider, output).run_project_calculations()
            self._assert_kpi_results_filled()
