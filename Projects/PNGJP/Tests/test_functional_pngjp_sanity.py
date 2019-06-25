import os
import MySQLdb

from KPIUtils_v2.DB.PsProjectConnector import PSProjectConnector
from Trax.Data.Testing.SeedNew import Seeder
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Cloud.Services.Connector.Keys import DbUsers
from Trax.Data.Testing.TestProjects import TestProjectsNames

from Projects.PNGJP.Tests.Data.test_data_pngjp_sanity import ProjectsSanityData
from Projects.PNGJP.Calculations import PNGJPCalculations
from Trax.Apps.Core.Testing.BaseCase import TestFunctionalCase

from Tests.TestUtils import remove_cache_and_storage
import pandas as pd

__author__ = 'avrahama'


class TestKEngineOutOfTheBox(TestFunctionalCase):

    def set_up(self):
        super(TestKEngineOutOfTheBox, self).set_up()
        remove_cache_and_storage()
        self.test_results_aginst = pd.read_csv('Data/Pngcn_results_E14412B2-BEF5-4380-B5D0-D3E23674C32B.csv')
        self.filtered_results = self.test_results_aginst[~self.test_results_aginst['sum_result'].isnull()]

    @staticmethod
    def test_if_2_df_are_equal(df1, df2):
        return len(df1.marge(df2)) == len(df1)

    @property
    def import_path(self):
        return 'Trax.Apps.Services.KEngine.Handlers.SessionHandler'

    @property
    def config_file_path(self):
        return os.path.join(os.path.dirname(os.path.realpath(__file__)), 'k-engine-test.config')

    seeder = Seeder()

    # def _assert_kpi_results_filled(self):
    #     connector = PSProjectConnector(TestProjectsNames().TEST_PROJECT_1, DbUsers.Docker)
    #     cursor = connector.db.cursor(MySQLdb.cursors.DictCursor)
    #     cursor.execute('''
    #     SELECT * FROM report.kpi_results
    #     ''')
    #     kpi_results = cursor.fetchall()
    #     self.assertNotEquals(len(kpi_results), 0)
    #     connector.disconnect_rds()
    #
    @seeder.seed(["mongodb_products_and_brands_seed", "pngjp_seed"], ProjectsSanityData())
    def test_pngjp_sanity(self):
        project_name = ProjectsSanityData.project_name
        data_provider = KEngineDataProvider(project_name)
        sessions = ['E14412B2-BEF5-4380-B5D0-D3E23674C32B']
        for session in sessions:
            data_provider.load_session_data(session)
            output = Output()
            PNGJPCalculations(data_provider, output).run_project_calculations()
            # self._assert_kpi_results_filled()

    @seeder.seed(["mongodb_products_and_brands_seed", "pngjp_seed"], ProjectsSanityData())
    def test_pngjp_sanity_Adjacency_KPI_124_131(self):
        # load the session and get results for the KPIs
        project_name = ProjectsSanityData.project_name
        data_provider = KEngineDataProvider(project_name)
        session = 'E14412B2-BEF5-4380-B5D0-D3E23674C32B'
        data_provider.load_session_data(session)
        output = Output()
        PNGJPCalculations(data_provider, output).run_project_calculations()

        # get results from seed, and test it
        connector = PSProjectConnector(TestProjectsNames().TEST_PROJECT_1, DbUsers.Docker)
        cursor = connector.db.cursor(MySQLdb.cursors.DictCursor)
        cursor.execute('''
                SELECT * FROM report.kpi_results
                ''')
        self.kpi_results = cursor.fetchall()
