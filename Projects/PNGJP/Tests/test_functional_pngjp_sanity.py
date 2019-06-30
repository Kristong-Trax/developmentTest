import os
import MySQLdb

from KPIUtils_v2.DB.PsProjectConnector import PSProjectConnector
from Trax.Data.Testing.SeedNew import Seeder
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Cloud.Services.Connector.Keys import DbUsers
from Trax.Data.Testing.TestProjects import TestProjectsNames

from Projects.PNGJP.Tests.Data.test_data_pngjp_sanity import ProjectsSanityData
from Projects.PNGJP.Calculations import PNGJPCalculations
from Trax.Apps.Core.Testing.BaseCase import TestFunctionalCase, patch

from Tests.TestUtils import remove_cache_and_storage
import pandas as pd

__author__ = 'avrahama'


class TestKEngineOutOfTheBox(TestFunctionalCase):
    seeder = Seeder()

    @seeder.seed(["mongodb_products_and_brands_seed", "pngjp_seed"], ProjectsSanityData())
    @patch('PNGJPToolBox.TEMPLATE_PATH',
           'Projects/PNGJP/Tests/Data/Template.xlsx')
    @patch('PNGJPToolBox.GOLDEN_ZONE_PATH',
           'Projects/PNGJP/Tests/Data/TemplateQualitative.xlsx')
    def set_up(self):
        super(TestKEngineOutOfTheBox, self).set_up()

        # get expected results DB from file
        self.test_results_against = pd.read_csv('Data/Pngcn_results_E14412B2-BEF5-4380-B5D0-D3E23674C32B.csv')
        self.kpi_expected_results_df = self.test_results_against[~self.test_results_against['sum_result'].isnull()]

        # load the session and save the results in the seed results for the KPIs and save the toolbox
        self.toolbox = self.save_kpi_results_in_seed()

        # get results from seed
        self.kpi_actual_results_df = self.get_kpi_actual_results_from_seed()

        # mock template
        remove_cache_and_storage()

    @property
    def import_path(self):
        return 'Trax.Apps.Services.KEngine.Handlers.SessionHandler'

    @property
    def config_file_path(self):
        return os.path.join(os.path.dirname(os.path.realpath(__file__)), 'k-engine-test.config')

    @staticmethod
    @seeder.seed(["mongodb_products_and_brands_seed", "pngjp_seed"], ProjectsSanityData())
    def save_kpi_results_in_seed():
        """
        load the session and save the results in the seed results for the KPIs
        """
        project_name = ProjectsSanityData.project_name
        data_provider = KEngineDataProvider(project_name)
        session = 'E14412B2-BEF5-4380-B5D0-D3E23674C32B'
        data_provider.load_session_data(session)
        output = Output()
        toolbox = PNGJPCalculations(data_provider, output).run_project_calculations()
        return toolbox

    @staticmethod
    @seeder.seed(["mongodb_products_and_brands_seed", "pngjp_seed"], ProjectsSanityData())
    def get_kpi_actual_results_from_seed():
        """get results from seed"""
        connector = PSProjectConnector(TestProjectsNames().TEST_PROJECT_1, DbUsers.Docker)
        cursor = connector.db.cursor(MySQLdb.cursors.DictCursor)
        cursor.execute('''
                                SELECT * FROM report.kpi_results
                                ''')
        temp = cursor.fetchall()
        # save results to df
        df = pd.DataFrame(temp)
        # filter unneeded columns
        df_filtered = df[['kps_name', 'kpi_fk', 'result']]
        # copy kpi_fk in-order to count the fks
        df_filtered['kpi_fk_count'] = df['kpi_fk']
        # convert string result to float
        df_filtered['result'] = pd.to_numeric(df_filtered['result'])
        # sum results and count fks
        df_calculated = df_filtered.groupby('kpi_fk').agg(
            {'kpi_fk_count': 'count', 'result': 'sum'}).reset_index().rename(
            columns={'result': 'results sum'})
        df_calculated = df_calculated[~(df_calculated['results sum'] == 0.0)]
        return df_calculated

    @staticmethod
    def test_if_2_df_are_equal(df1, df2):
        return len(df1.marge(df2)) == len(df1)

    @seeder.seed(["mongodb_products_and_brands_seed", "pngjp_seed"], ProjectsSanityData())
    def test_pngjp_sanity(self):
        pass
