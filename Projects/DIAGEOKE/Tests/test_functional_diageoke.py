import pandas as pd
import MySQLdb
from KPIUtils_v2.DB.PsProjectConnector import PSProjectConnector
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Apps.Core.Testing.BaseCase import TestFunctionalCase
from Trax.Cloud.Services.Connector.Keys import DbUsers
from Trax.Data.Testing.SeedNew import Seeder
from Trax.Data.Testing.TestProjects import TestProjectsNames
from Projects.DIAGEOKE.Calculations import DIAGEOKECalculations
from Projects.DIAGEOKE.Tests.Data.test_data_diageoke import ProjectsSanityData
from Projects.DIAGEOKE.Utils.KPIToolBox import DIAGEOKEToolBox
from Tests.TestUtils import remove_cache_and_storage

__author__ = 'yoava'


class TestDiageoke(TestFunctionalCase):
    seeder = Seeder()

    @seeder.seed(["mongodb_products_and_brands_seed", "diageoke_seed"], ProjectsSanityData())
    def set_up(self):
        super(TestDiageoke, self).set_up()
        self.project_name = ProjectsSanityData.project_name
        self.output = Output()
        self.mock_object('save_latest_templates', path='KPIUtils.DIAGEO.ToolBox.DIAGEOToolBox')
        self.session_uid = '08e4dbd4-9270-4352-a68b-ca27e7853de6'
        self.data_provider = KEngineDataProvider(self.project_name)
        self.data_provider.load_session_data(self.session_uid)
        remove_cache_and_storage()

    @property
    def import_path(self):
        return 'Projects.DIAGEOKE.Utils.KPIToolBox'

    def _assert_kpi_results_filled(self):
        connector = PSProjectConnector(TestProjectsNames().TEST_PROJECT_1, DbUsers.Docker)
        cursor = connector.db.cursor(MySQLdb.cursors.DictCursor)
        cursor.execute('''
       SELECT * FROM report.kpi_results
       ''')
        kpi_results = cursor.fetchall()
        self.assertNotEquals(len(kpi_results), 0)
        connector.disconnect_rds()

    @seeder.seed(["mongodb_products_and_brands_seed", "diageoke_seed"], ProjectsSanityData())
    def test_diageoke_sanity(self):
        """
        basic sanity test, check if we write results after runing the KPIs
        """
        DIAGEOKECalculations(self.data_provider, self.output).run_project_calculations()
        self._assert_kpi_results_filled()

    @seeder.seed(["mongodb_products_and_brands_seed", "diageoke_seed"], ProjectsSanityData())
    def test_get_match_display(self):
        """
        test the resulte type
        """
        tool_box = DIAGEOKEToolBox(self.data_provider, self.output)
        result = tool_box.get_match_display()
        expected_result = pd.DataFrame
        self.assertIsInstance(result, expected_result)

    @seeder.seed(["mongodb_products_and_brands_seed", "diageoke_seed"], ProjectsSanityData())
    def test_get_kpi_static_data(self):
        """
        test the resulte type
        """
        tool_box = DIAGEOKEToolBox(self.data_provider, self.output)
        result = tool_box.get_kpi_static_data()
        expected_result = pd.DataFrame
        self.assertIsInstance(result, expected_result)

    @seeder.seed(["mongodb_products_and_brands_seed", "diageoke_seed"], ProjectsSanityData())
    def test_main_calculation(self):
        """
        test that the resulte is None
        """
        tool_box = DIAGEOKEToolBox(self.data_provider, self.output)
        set_names = tool_box.kpi_static_data['kpi_set_name'].unique().tolist()
        tool_box.kpi_static_data = tool_box.get_kpi_static_data()
        result = tool_box.main_calculation(set_names)
        self.assertIsNone(result, "Did Not returned None")
