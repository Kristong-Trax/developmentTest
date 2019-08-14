# coding=utf-8
from Projects.TNUVAILV2.Tests.Data.TestData.test_data_tnuvailv2 import TnuvailV2SanityData
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider
from Projects.TNUVAILV2.Tests.Data.MockDataFrames import TnuvaMocks
from Trax.Apps.Core.Testing.BaseCase import TestFunctionalCase
from Projects.TNUVAILV2.Utils.KPIToolBox import TNUVAILToolBox
from Projects.TNUVAILV2.Utils.Consts import Consts
from Trax.Data.Testing.SeedNew import Seeder
from mock import MagicMock


__author__ = 'idanr'


class TestTnuvaV2(TestFunctionalCase):
    base_calculation_path = 'KPIUtils_v2.Calculations.BaseCalculations'
    seeder = Seeder()

    @property
    def import_path(self):
        return 'Projects.TNUVAILV2.Utils.KPIToolBox'

    def set_up(self):
        super(TestTnuvaV2, self).set_up()
        self.data_provider_mock = self._initiate_date_provider()
        self.output = MagicMock()
        self.mock_common()
        self.mock_assortment()
        self.mock_ps_data_provider()
        self.mock_project_connector()
        self.tool_box = TNUVAILToolBox(self.data_provider_mock,  self.output)

    def mock_assortment(self):
        self.mock_object('Assortment')
        self.mock_util_functions()

    def mock_util_functions(self):
        self.lvl3_assortment = self.mock_object('TNUVAILToolBox._prepare_data_for_assortment_calculation')
        self.lvl3_assortment.return_value = TnuvaMocks.lvl3_results()
        self.kpi_result_type = self.mock_object('DBHandler.get_kpi_result_value',
                                                path='Projects.TNUVAILV2.Utils.DataBaseHandler')
        self.kpi_result_type.return_value = TnuvaMocks.get_result_value()

    def mock_project_connector(self):
        self.mock_object('ProjectConnector', path='KPIUtils_v2.DB.PsProjectConnector')
        self.mock_object('PSProjectConnector', path='Projects.TNUVAILV2.Utils.DataBaseHandler')
        self.mock_object('PSProjectConnector', path=TestTnuvaV2.base_calculation_path)

    def mock_common(self):
        self.mock_object('Common')
        self.mock_object('Common', path=TestTnuvaV2.base_calculation_path)

    def mock_ps_data_provider(self):
        self.mock_object('PsDataProvider', path=TestTnuvaV2.base_calculation_path)

    @seeder.seed(["mongodb_products_and_brands_seed", "tnuvailv2_sand_seed"], TnuvailV2SanityData())
    def _initiate_date_provider(self):
        data_provider = KEngineDataProvider(TnuvailV2SanityData.project_name)
        session = '236c1577-0ecb-4bf9-88b9-c9e87ab17c58'
        data_provider.load_session_data(session)
        return data_provider

    def test_general_sos_calculation(self):
        test_case_1 = ((11, 11), {Consts.MANUFACTURER_FK: self.tool_box.own_manufacturer_fk})
        test_case_2 = ((0, 11), {Consts.MANUFACTURER_FK: 999})
        test_case_3 = ((1, 11), {Consts.CATEGORY_FK: 252})
        test_cases = [test_case_1, test_case_2, test_case_3]
        for expected_res, test_case in test_cases:
            self.assertEqual(expected_res, self.tool_box._general_sos_calculation(self.tool_box.scif, **test_case))

    def test_filtered_scif_by_policy(self):
        tirat_tsvi_scif = self.tool_box._get_filtered_scif_for_sos_calculations(Consts.TIRAT_TSVI_POLICY)
        dairy_scif = self.tool_box._get_filtered_scif_for_sos_calculations(Consts.MILKY_POLICY)
        # Validate products
        products_in_dairy_scif = [15628, 15631, 15639, 15829, 15881]
        products_in_tirat_tsvi_scif = [15603, 15613, 15854, 15874, 15884, 15915]
        self.assertEqual(len(products_in_dairy_scif), len(
            set(dairy_scif.product_fk.unique().tolist()).intersection(products_in_dairy_scif)))
        self.assertEqual(len(products_in_tirat_tsvi_scif), len(
            set(tirat_tsvi_scif.product_fk.unique().tolist()).intersection(products_in_tirat_tsvi_scif)))
        # Validate scenes
        self.assertEqual(40, dairy_scif.scene_fk.values[0])
        self.assertEqual(38, tirat_tsvi_scif.scene_fk.values[0])

    def test_category_manufacturer_sos(self):
        mock_scif = TnuvaMocks.get_mock_scif_1(self.tool_box.own_manufacturer_fk)
        expected_own_manu_in_cat_res = {1: 2, 2: 2, 3: 5, 4: 0}
        cat_1_res = self.tool_box._calculate_manufacturer_of_out_category_sos(mock_scif, 1)
        cat_2_res = self.tool_box._calculate_manufacturer_of_out_category_sos(mock_scif, 2)
        cat_3_res = self.tool_box._calculate_manufacturer_of_out_category_sos(mock_scif, 3)
        cat_4_res = self.tool_box._calculate_manufacturer_of_out_category_sos(mock_scif, 4)
        category_results = [cat_1_res, cat_2_res, cat_3_res, cat_4_res]
        for cat_res in category_results:
            is_own_manufacturer_in_res = False
            for res in cat_res:
                cat_fk = res[Consts.CATEGORY_FK]
                if res[Consts.MANUFACTURER_FK] == self.tool_box.own_manufacturer_fk:
                    self.assertEqual(expected_own_manu_in_cat_res[cat_fk], res[Consts.NUMERATOR_RESULT])
                    is_own_manufacturer_in_res = True
            self.assertTrue(is_own_manufacturer_in_res, msg='There must be Tnuva manufacturer in category results!')

    def test_assortment(self):
        lvl3_assortment = self.lvl3_assortment.return_value
        tirat_tsvi_data = self.tool_box._get_relevant_assortment_data(lvl3_assortment, Consts.TIRAT_TSVI_POLICY)
        dairy_data = self.tool_box._get_relevant_assortment_data(lvl3_assortment, Consts.MILKY_POLICY)
        # Compare expected product
        dairy_expected_product = [15628, 15603, 15613, 15829, 15639, 15631, 15567, 16080, 15602, 15588]
        tirat_tsvi_product = [15915, 15884, 15854, 15881, 15874, 15852, 15879]
        self.compare_assortment_product_and_expected(tirat_tsvi_data, tirat_tsvi_product)
        self.compare_assortment_product_and_expected(dairy_data, dairy_expected_product)
        # Compare expected store results
        store_ass_res_dairy = 4, 10
        store_oos_res_tsvi = 3, 7
        self.compare_store_results(dairy_data, store_ass_res_dairy, True)
        self.compare_store_results(tirat_tsvi_data, store_oos_res_tsvi, False)
        # Compare expected category results
        dairy_expected_cat = {(252, 810): 1, (260, 810): 0, (262, 810): 2}
        tsvi_expected_cat = {(260, 810): 4}
        self.compare_category_results(dairy_data, dairy_expected_cat, True)
        self.compare_category_results(tirat_tsvi_data, tsvi_expected_cat, True)

    def compare_assortment_product_and_expected(self, lvl3_data_relevant, expected_product):
        self.assertEqual(set(expected_product), set(lvl3_data_relevant.product_fk.unique().tolist()))

    def compare_store_results(self, lvl3_data, expected_results, is_dist):
        store_res = self.tool_box._calculate_store_level_assortment(lvl3_data, is_dist)
        num_res, den_res = store_res[0][Consts.NUMERATOR_RESULT], store_res[0][Consts.DENOMINATOR_RESULT]
        self.assertEqual((num_res, den_res), expected_results)

    def compare_category_results(self, lvl3_data, expected_res, is_dist):
        cat_results = self.tool_box._calculate_category_level_assortment(lvl3_data, is_distribution=is_dist)
        for res in cat_results:
            cat_fk, manufacturer_fk = res[Consts.CATEGORY_FK], res[Consts.DENOMINATOR_ID]
            if (cat_fk, manufacturer_fk) in expected_res.keys():
                self.assertEqual(expected_res[(cat_fk, manufacturer_fk)], res[Consts.NUMERATOR_RESULT])
