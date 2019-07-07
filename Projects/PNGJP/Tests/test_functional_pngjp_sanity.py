import os
import MySQLdb
import time

from KPIUtils_v2.DB.PsProjectConnector import PSProjectConnector
from Trax.Data.Testing.SeedNew import Seeder
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Cloud.Services.Connector.Keys import DbUsers
from Trax.Data.Testing.TestProjects import TestProjectsNames

from Projects.PNGJP.Tests.Data.test_data_pngjp_sanity_per_kpi import ProjectsSanityData
from Projects.PNGJP.Calculations import PNGJPCalculations
from Trax.Apps.Core.Testing.BaseCase import TestFunctionalCase, MagicMock

from Tests.TestUtils import remove_cache_and_storage
import pandas as pd

__author__ = 'avrahama'


class TestPngjpSanityPerKPI(TestFunctionalCase):
    seeder = Seeder()

    @seeder.seed(["mongodb_products_and_brands_seed", "pngjp_seed"], ProjectsSanityData())
    def set_up(self):
        super(TestPngjpSanityPerKPI, self).set_up()

        # mock templates
        self.get_template_path_mock = MagicMock('get_template_path',
                                                path='Projects.PNGJP.Utils.KPIToolBox.PNGJPToolBox',
                                                side_effect=[
                                                    os.path.join(os.path.dirname(os.path.realpath(__file__)), 'Data',
                                                                 'Template.xlsx'),
                                                    os.path.join(os.path.dirname(os.path.realpath(__file__)), 'Data',
                                                                 'TemplateQualitative.xlsx')
                                                ])
        # get expected results DB from file
        self.test_results_against = pd.read_csv('Data/Pngcn_results_E14412B2-BEF5-4380-B5D0-D3E23674C32B.csv')
        self.kpi_expected_results_df = self.test_results_against[~self.test_results_against['sum_result'].isnull()]

        # load seed data
        self.save_kpi_results_in_seed()

        # get actual results from seed
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
        start_time = time.time()
        data_provider.load_session_data(session)
        elapsed_time = time.time() - start_time
        print elapsed_time

    @staticmethod
    @seeder.seed(["mongodb_products_and_brands_seed", "pngjp_seed"], ProjectsSanityData())
    def get_kpi_actual_results_from_seed():
        """get results from seed"""
        connector = PSProjectConnector(TestProjectsNames().TEST_PROJECT_1, DbUsers.Docker)
        cursor = connector.db.cursor(MySQLdb.cursors.DictCursor)
        cursor.execute('''SELECT * FROM report.kpi_results''')
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

    @seeder.seed(["mongodb_products_and_brands_seed", "pngjp_seed"], ProjectsSanityData())
    def test_SKU_Distribution_Raw_Data(self):
        kpi_name = 'SKU Distribution Raw Data'
        kpi_fk = 1
        expected_result = \
            self.kpi_expected_results_df[self.kpi_expected_results_df.kpi_fk == kpi_fk]['sum_result'].values[0]
        actual_result = \
            self.kpi_actual_results_df[self.kpi_actual_results_df['kpi_fk'] == kpi_fk]['results sum'].values[0]
        self.assertAlmostEqual(expected_result, actual_result, msg='KPI {} results are not as expected'.format(kpi_name))

    @seeder.seed(["mongodb_products_and_brands_seed", "pngjp_seed"], ProjectsSanityData())
    def test_SKU_Distribution_Primary(self):
        kpi_name = 'SKU Distribution (Primary)'
        kpi_fk = 2
        expected_result = \
            self.kpi_expected_results_df[self.kpi_expected_results_df.kpi_fk == kpi_fk]['sum_result'].values[0]
        actual_result = \
            self.kpi_actual_results_df[self.kpi_actual_results_df['kpi_fk'] == kpi_fk]['results sum'].values[0]
        self.assertAlmostEqual(expected_result, actual_result, msg='KPI {} results are not as expected'.format(kpi_name))

    @seeder.seed(["mongodb_products_and_brands_seed", "pngjp_seed"], ProjectsSanityData())
    def test_SKU_OOS_Primary(self):
        kpi_name = 'SKU OOS (Primary)'
        kpi_fk = 3
        expected_result = \
            self.kpi_expected_results_df[self.kpi_expected_results_df.kpi_fk == kpi_fk]['sum_result'].values[0]
        actual_result = \
            self.kpi_actual_results_df[self.kpi_actual_results_df['kpi_fk'] == kpi_fk]['results sum'].values[0]
        self.assertAlmostEqual(expected_result, actual_result, msg='KPI {} results are not as expected'.format(kpi_name))

    @seeder.seed(["mongodb_products_and_brands_seed", "pngjp_seed"], ProjectsSanityData())
    def test_Linear_Shelf_Space_by_Manufacturer(self):
        kpi_name = 'Linear Shelf Space (by Manufacturer)'
        kpi_fk = 6
        expected_result = \
            self.kpi_expected_results_df[self.kpi_expected_results_df.kpi_fk == kpi_fk]['sum_result'].values[0]
        actual_result = \
            self.kpi_actual_results_df[self.kpi_actual_results_df['kpi_fk'] == kpi_fk]['results sum'].values[0]
        self.assertAlmostEqual(expected_result, actual_result, msg='KPI {} results are not as expected'.format(kpi_name))

    @seeder.seed(["mongodb_products_and_brands_seed", "pngjp_seed"], ProjectsSanityData())
    def test_Linear_Shelf_Space_Primary(self):
        kpi_name = 'Linear Shelf Space (Primary)'
        kpi_fk = 7
        expected_result = \
            self.kpi_expected_results_df[self.kpi_expected_results_df.kpi_fk == kpi_fk]['sum_result'].values[0]
        actual_result = \
            self.kpi_actual_results_df[self.kpi_actual_results_df['kpi_fk'] == kpi_fk]['results sum'].values[0]
        self.assertAlmostEqual(expected_result, actual_result, msg='KPI {} results are not as expected'.format(kpi_name))

    @seeder.seed(["mongodb_products_and_brands_seed", "pngjp_seed"], ProjectsSanityData())
    def test_Number_of_Facings_Primary_by_Manufacturer(self):
        kpi_name = 'Number of Facings (Primary) (by Manufacturer)'
        kpi_fk = 10
        expected_result = \
            self.kpi_expected_results_df[self.kpi_expected_results_df.kpi_fk == kpi_fk]['sum_result'].values[0]
        actual_result = \
            self.kpi_actual_results_df[self.kpi_actual_results_df['kpi_fk'] == kpi_fk]['results sum'].values[0]
        self.assertAlmostEqual(expected_result, actual_result, msg='KPI {} results are not as expected'.format(kpi_name))

    @seeder.seed(["mongodb_products_and_brands_seed", "pngjp_seed"], ProjectsSanityData())
    def test_Number_of_Facings_Primary(self):
        kpi_name = 'Number of Facings (Primary)'
        kpi_fk = 11
        expected_result = \
            self.kpi_expected_results_df[self.kpi_expected_results_df.kpi_fk == kpi_fk]['sum_result'].values[0]
        actual_result = \
            self.kpi_actual_results_df[self.kpi_actual_results_df['kpi_fk'] == kpi_fk]['results sum'].values[0]
        self.assertAlmostEqual(expected_result, actual_result, msg='KPI {} results are not as expected'.format(kpi_name))

    @seeder.seed(["mongodb_products_and_brands_seed", "pngjp_seed"], ProjectsSanityData())
    def test_Number_of_Facings_Primary_Raw_Data(self):
        kpi_name = 'Number of Facings (Primary) Raw Data'
        kpi_fk = 12
        expected_result = \
            self.kpi_expected_results_df[self.kpi_expected_results_df.kpi_fk == kpi_fk]['sum_result'].values[0]
        actual_result = \
            self.kpi_actual_results_df[self.kpi_actual_results_df['kpi_fk'] == kpi_fk]['results sum'].values[0]
        self.assertAlmostEqual(expected_result, actual_result, msg='KPI {} results are not as expected'.format(kpi_name))

    @seeder.seed(["mongodb_products_and_brands_seed", "pngjp_seed"], ProjectsSanityData())
    def test_Share_of_Shelf_Facings_by_Empty_Space(self):
        kpi_name = 'Share of Shelf Facings by Empty Space'
        kpi_fk = 13
        expected_result = \
            self.kpi_expected_results_df[self.kpi_expected_results_df.kpi_fk == kpi_fk]['sum_result'].values[0]
        actual_result = \
            self.kpi_actual_results_df[self.kpi_actual_results_df['kpi_fk'] == kpi_fk]['results sum'].values[0]
        self.assertAlmostEqual(expected_result, actual_result, msg='KPI {} results are not as expected'.format(kpi_name))

    @seeder.seed(["mongodb_products_and_brands_seed", "pngjp_seed"], ProjectsSanityData())
    def test_Golden_Zone_Compliance(self):
        kpi_name = 'Golden Zone Compliance'
        kpi_fk = 14
        expected_result = \
            self.kpi_expected_results_df[self.kpi_expected_results_df.kpi_fk == kpi_fk]['sum_result'].values[0]
        actual_result = \
            self.kpi_actual_results_df[self.kpi_actual_results_df['kpi_fk'] == kpi_fk]['results sum'].values[0]
        self.assertAlmostEqual(expected_result, actual_result, msg='KPI {} results are not as expected'.format(kpi_name))

    @seeder.seed(["mongodb_products_and_brands_seed", "pngjp_seed"], ProjectsSanityData())
    def test_Display_Raw_Data(self):
        kpi_name = 'Display Raw Data'
        kpi_fk = 16
        expected_result = \
            self.kpi_expected_results_df[self.kpi_expected_results_df.kpi_fk == kpi_fk]['sum_result'].values[0]
        actual_result = \
            self.kpi_actual_results_df[self.kpi_actual_results_df['kpi_fk'] == kpi_fk]['results sum'].values[0]
        self.assertAlmostEqual(expected_result, actual_result, msg='KPI {} results are not as expected'.format(kpi_name))

    @seeder.seed(["mongodb_products_and_brands_seed", "pngjp_seed"], ProjectsSanityData())
    def test_Number_of_Display_by_Brand(self):
        kpi_name = 'Number of Display (by Brand)'
        kpi_fk = 17
        expected_result = \
            self.kpi_expected_results_df[self.kpi_expected_results_df.kpi_fk == kpi_fk]['sum_result'].values[0]
        actual_result = \
            self.kpi_actual_results_df[self.kpi_actual_results_df['kpi_fk'] == kpi_fk]['results sum'].values[0]
        self.assertAlmostEqual(expected_result, actual_result, msg='KPI {} results are not as expected'.format(kpi_name))

    @seeder.seed(["mongodb_products_and_brands_seed", "pngjp_seed"], ProjectsSanityData())
    def test_Size_of_Display_Raw_Data(self):
        kpi_name = 'Size of Display Raw Data'
        kpi_fk = 21
        expected_result = \
            self.kpi_expected_results_df[self.kpi_expected_results_df.kpi_fk == kpi_fk]['sum_result'].values[0]
        actual_result = \
            self.kpi_actual_results_df[self.kpi_actual_results_df['kpi_fk'] == kpi_fk]['results sum'].values[0]
        self.assertAlmostEqual(expected_result, actual_result, msg='KPI {} results are not as expected'.format(kpi_name))

    @seeder.seed(["mongodb_products_and_brands_seed", "pngjp_seed"], ProjectsSanityData())
    def test_Size_of_Display_by_Manufacturer(self):
        kpi_name = 'Size of Display (by Manufacturer)'
        kpi_fk = 22
        expected_result = \
            self.kpi_expected_results_df[self.kpi_expected_results_df.kpi_fk == kpi_fk]['sum_result'].values[0]
        actual_result = \
            self.kpi_actual_results_df[self.kpi_actual_results_df['kpi_fk'] == kpi_fk]['results sum'].values[0]
        self.assertAlmostEqual(expected_result, actual_result, msg='KPI {} results are not as expected'.format(kpi_name))

    @seeder.seed(["mongodb_products_and_brands_seed", "pngjp_seed"], ProjectsSanityData())
    def test_Number_of_Facings_by_Brand(self):
        kpi_name = 'Number of Facings (by Brand)'
        kpi_fk = 23
        expected_result = \
            self.kpi_expected_results_df[self.kpi_expected_results_df.kpi_fk == kpi_fk]['sum_result'].values[0]
        actual_result = \
            self.kpi_actual_results_df[self.kpi_actual_results_df['kpi_fk'] == kpi_fk]['results sum'].values[0]
        self.assertAlmostEqual(expected_result, actual_result, msg='KPI {} results are not as expected'.format(kpi_name))

    @seeder.seed(["mongodb_products_and_brands_seed", "pngjp_seed"], ProjectsSanityData())
    def test_SKU_on_Display_Raw_Data(self):
        kpi_name = 'SKU on Display Raw Data'
        kpi_fk = 24
        expected_result = \
            self.kpi_expected_results_df[self.kpi_expected_results_df.kpi_fk == kpi_fk]['sum_result'].values[0]
        actual_result = \
            self.kpi_actual_results_df[self.kpi_actual_results_df['kpi_fk'] == kpi_fk]['results sum'].values[0]
        self.assertAlmostEqual(expected_result, actual_result, msg='KPI {} results are not as expected'.format(kpi_name))

    @seeder.seed(["mongodb_products_and_brands_seed", "pngjp_seed"], ProjectsSanityData())
    def test_SKU_on_Display_by_Category(self):
        kpi_name = 'SKU on Display (by Category)'
        kpi_fk = 25
        expected_result = \
            self.kpi_expected_results_df[self.kpi_expected_results_df.kpi_fk == kpi_fk]['sum_result'].values[0]
        actual_result = \
            self.kpi_actual_results_df[self.kpi_actual_results_df['kpi_fk'] == kpi_fk]['results sum'].values[0]
        self.assertAlmostEqual(expected_result, actual_result, msg='KPI {} results are not as expected'.format(kpi_name))

    @seeder.seed(["mongodb_products_and_brands_seed", "pngjp_seed"], ProjectsSanityData())
    def test_PSKU_Distribution_Raw_Data(self):
        kpi_name = 'PSKU Distribution Raw Data'
        kpi_fk = 27
        expected_result = \
            self.kpi_expected_results_df[self.kpi_expected_results_df.kpi_fk == kpi_fk]['sum_result'].values[0]
        actual_result = \
            self.kpi_actual_results_df[self.kpi_actual_results_df['kpi_fk'] == kpi_fk]['results sum'].values[0]
        self.assertAlmostEqual(expected_result, actual_result, msg='KPI {} results are not as expected'.format(kpi_name))

    @seeder.seed(["mongodb_products_and_brands_seed", "pngjp_seed"], ProjectsSanityData())
    def test_PSKU_Presence(self):
        kpi_name = 'PSKU Presence'
        kpi_fk = 28
        expected_result = \
            self.kpi_expected_results_df[self.kpi_expected_results_df.kpi_fk == kpi_fk]['sum_result'].values[0]
        actual_result = \
            self.kpi_actual_results_df[self.kpi_actual_results_df['kpi_fk'] == kpi_fk]['results sum'].values[0]
        self.assertAlmostEqual(expected_result, actual_result, msg='KPI {} results are not as expected'.format(kpi_name))

    @seeder.seed(["mongodb_products_and_brands_seed", "pngjp_seed"], ProjectsSanityData())
    def test_Number_of_Display_by_Category(self):
        kpi_name = 'Number of Display (by Category)'
        kpi_fk = 68
        expected_result = \
            self.kpi_expected_results_df[self.kpi_expected_results_df.kpi_fk == kpi_fk]['sum_result'].values[0]
        actual_result = \
            self.kpi_actual_results_df[self.kpi_actual_results_df['kpi_fk'] == kpi_fk]['results sum'].values[0]
        self.assertAlmostEqual(expected_result, actual_result, msg='KPI {} results are not as expected'.format(kpi_name))

    @seeder.seed(["mongodb_products_and_brands_seed", "pngjp_seed"], ProjectsSanityData())
    def test_Adjacency124(self):
        kpi_name = 'Adjacency124'
        kpi_fk = 124
        expected_result = \
            self.kpi_expected_results_df[self.kpi_expected_results_df.kpi_fk == kpi_fk]['sum_result'].values[0]
        actual_result = \
            self.kpi_actual_results_df[self.kpi_actual_results_df['kpi_fk'] == kpi_fk]['results sum'].values[0]
        self.assertAlmostEqual(expected_result, actual_result, msg='KPI {} results are not as expected'.format(kpi_name))

    @seeder.seed(["mongodb_products_and_brands_seed", "pngjp_seed"], ProjectsSanityData())
    def test_Adjacency125(self):
        kpi_name = 'Adjacency125'
        kpi_fk = 125
        expected_result = \
            self.kpi_expected_results_df[self.kpi_expected_results_df.kpi_fk == kpi_fk]['sum_result'].values[0]
        actual_result = \
            self.kpi_actual_results_df[self.kpi_actual_results_df['kpi_fk'] == kpi_fk]['results sum'].values[0]
        self.assertAlmostEqual(expected_result, actual_result, msg='KPI {} results are not as expected'.format(kpi_name))

    @seeder.seed(["mongodb_products_and_brands_seed", "pngjp_seed"], ProjectsSanityData())
    def test_Adjacency126(self):
        kpi_name = 'Adjacency126'
        kpi_fk = 126
        expected_result = \
            self.kpi_expected_results_df[self.kpi_expected_results_df.kpi_fk == kpi_fk]['sum_result'].values[0]
        actual_result = \
            self.kpi_actual_results_df[self.kpi_actual_results_df['kpi_fk'] == kpi_fk]['results sum'].values[0]
        self.assertAlmostEqual(expected_result, actual_result, msg='KPI {} results are not as expected'.format(kpi_name))

    @seeder.seed(["mongodb_products_and_brands_seed", "pngjp_seed"], ProjectsSanityData())
    def test_Adjacency127(self):
        kpi_name = 'Adjacency127'
        kpi_fk = 127
        expected_result = \
            self.kpi_expected_results_df[self.kpi_expected_results_df.kpi_fk == kpi_fk]['sum_result'].values[0]
        actual_result = \
            self.kpi_actual_results_df[self.kpi_actual_results_df['kpi_fk'] == kpi_fk]['results sum'].values[0]
        self.assertAlmostEqual(expected_result, actual_result, msg='KPI {} results are not as expected'.format(kpi_name))

    @seeder.seed(["mongodb_products_and_brands_seed", "pngjp_seed"], ProjectsSanityData())
    def test_Adjacency128(self):
        kpi_name = 'Adjacency128'
        kpi_fk = 128
        expected_result = \
            self.kpi_expected_results_df[self.kpi_expected_results_df.kpi_fk == kpi_fk]['sum_result'].values[0]
        actual_result = \
            self.kpi_actual_results_df[self.kpi_actual_results_df['kpi_fk'] == kpi_fk]['results sum'].values[0]
        self.assertAlmostEqual(expected_result, actual_result, msg='KPI {} results are not as expected'.format(kpi_name))

    @seeder.seed(["mongodb_products_and_brands_seed", "pngjp_seed"], ProjectsSanityData())
    def test_Adjacency129(self):
        kpi_name = 'Adjacency129'
        kpi_fk = 129
        expected_result = \
            self.kpi_expected_results_df[self.kpi_expected_results_df.kpi_fk == kpi_fk]['sum_result'].values[0]
        actual_result = \
            self.kpi_actual_results_df[self.kpi_actual_results_df['kpi_fk'] == kpi_fk]['results sum'].values[0]
        self.assertAlmostEqual(expected_result, actual_result, msg='KPI {} results are not as expected'.format(kpi_name))

    @seeder.seed(["mongodb_products_and_brands_seed", "pngjp_seed"], ProjectsSanityData())
    def test_Adjacency130(self):
        kpi_name = 'Adjacency130'
        kpi_fk = 130
        expected_result = \
            self.kpi_expected_results_df[self.kpi_expected_results_df.kpi_fk == kpi_fk]['sum_result'].values[0]
        actual_result = \
            self.kpi_actual_results_df[self.kpi_actual_results_df['kpi_fk'] == kpi_fk]['results sum'].values[0]
        self.assertAlmostEqual(expected_result, actual_result, msg='KPI {} results are not as expected'.format(kpi_name))

    @seeder.seed(["mongodb_products_and_brands_seed", "pngjp_seed"], ProjectsSanityData())
    def test_Adjacency131(self):
        kpi_name = 'Adjacency131'
        kpi_fk = 131
        expected_result = \
            self.kpi_expected_results_df[self.kpi_expected_results_df.kpi_fk == kpi_fk]['sum_result'].values[0]
        actual_result = self.kpi_actual_results_df[self.kpi_actual_results_df.kpi_fk == kpi_fk]['results sum'].values[0]
        self.assertAlmostEqual(expected_result, actual_result, msg='KPI {} results are not as expected'.format(kpi_name))
