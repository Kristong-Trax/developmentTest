# coding=utf-8
from Trax.Apps.Core.Testing.BaseCase import TestFunctionalCase, TestUnitCase
# from Trax.Data.Testing.SeedNew import Seeder
# from Trax.Utils.Testing.Case import TestUnitCase
from mock import MagicMock, PropertyMock
from Projects.TNUVAILV2.Tests.data_test_unit_tnuvailv2 import DataTestUnitTnuva
from Projects.TNUVAILV2.Utils.KPIToolBox import TNUVAILToolBox
from Trax.Algo.Calculations.Core.DataProvider import Data
import pandas as pd
# import numpy as np
from pandas.util.testing import assert_frame_equal

__author__ = 'natalyak'


def get_test_case_template_all_tests():
    test_case_xls_object = pd.ExcelFile(DataTestUnitTnuva.test_case_1)
    return test_case_xls_object


def get_scif_matches_stich_groups(xls_file):
    scif_test_case = pd.read_excel(xls_file, sheetname='scif')
    matches_test_case = pd.read_excel(xls_file, sheetname='matches')
    all_products = pd.read_excel(xls_file, sheetname='all_products')
    return scif_test_case, matches_test_case, all_products


class TestTnuvailv2(TestFunctionalCase):

    TEST_CASE = get_test_case_template_all_tests()
    SCIF, MATCHES, ALL_PRODUCTS = get_scif_matches_stich_groups(TEST_CASE)

    @property
    def import_path(self):
        return 'Projects.TNUVAILV2.Utils.KPIToolBox'

    def set_up(self):
        super(TestTnuvailv2, self).set_up()
        self.mock_data_provider()
        self.data_provider_mock.project_name = 'Test_Project_1'
        self.data_provider_mock.rds_conn = MagicMock()
        self.mock_db_users()
        # self.project_connector_mock = self.mock_project_connector()
        self.mock_common_project_connector_mock = self.mock_common_project_connector()
        self.static_kpi_mock = self.mock_static_kpi()
        self.mock_all_products()
        self.session_info_mock = self.mock_session_info()
        self.output = MagicMock()
        self.session_info_mock = self.mock_session_info()
        self.kpi_result_values_mock = self.mock_kpi_result_value_table()
        self.mock_various_project_connectors()
        self.mock_position_graph()
        self.mock_lvl3_ass_base_df()
        self.mock_own_manufacturer_property()
        self.mock_store_id()

    def mock_all_products(self):
        self.data_provider_data_mock[Data.ALL_PRODUCTS] = self.ALL_PRODUCTS

    def mock_lvl3_ass_base_df(self):
        ass_res = self.mock_object('Assortment.get_lvl3_relevant_ass',
                                   path='KPIUtils_v2.Calculations.AssortmentCalculations')
        ass_res.return_value = DataTestUnitTnuva.assortment_store
        return ass_res.return_value

    def mock_position_graph(self):
        self.mock_object('PositionGraphs', path='KPIUtils_v2.Calculations.AssortmentCalculations')

    def mock_kpi_result_value_table(self):
        kpi_result_value = self.mock_object('DBHandler.get_kpi_result_value',
                                            path='Projects.TNUVAILV2.Utils.DataBaseHandler')
        kpi_result_value.return_value = DataTestUnitTnuva.kpi_results_values_table
        return kpi_result_value.return_value

    def mock_data_provider(self):
        self.data_provider_mock = MagicMock()
        # return self._data_provider
        self.data_provider_data_mock = {}

        def get_item(key):
            return self.data_provider_data_mock[key] if key in self.data_provider_data_mock else MagicMock()

        self.data_provider_mock.__getitem__.side_effect = get_item

        def _set_scene_item_facts_for_mock(df):
            self.data_provider_data_mock[Data.SCENE_ITEM_FACTS] = df
            p = PropertyMock(return_value=df)
            type(self.data_provider_mock).scene_item_facts = p

        self.data_provider_mock._set_scene_item_facts = _set_scene_item_facts_for_mock

    def mock_db_users(self):
        self.mock_object('DbUsers', path='KPIUtils_v2.DB.CommonV2')
        self.mock_object('DbUsers', path='Projects.TNUVAILV2.Utils.DataBaseHandler')
        self.mock_object('DbUsers', path='KPIUtils_v2.Calculations.BaseCalculations')
        self.mock_object('DbUsers', path='KPIUtils_v2.GlobalDataProvider.PSAssortmentProvider')

    def mock_various_project_connectors(self):
        self.mock_object('PSProjectConnector', path='KPIUtils_v2.GlobalDataProvider.PSAssortmentProvider')
        self.mock_object('PSProjectConnector', path='KPIUtils_v2.DB.PsProjectConnector')
        self.mock_object('PSProjectConnector', path='KPIUtils_v2.Calculations.BaseCalculations')
        self.mock_object('PSProjectConnector', path='KPIUtils_v2.GlobalDataProvider.PsDataProvider')
        self.mock_object('PSProjectConnector', path='Projects.TNUVAILV2.Utils.DataBaseHandler')

    # def mock_project_connector(self):
    #     return self.mock_object('PSProjectConnector')

    def mock_common_project_connector(self):
        return self.mock_object('PSProjectConnector', path='KPIUtils_v2.DB.CommonV2')

    def mock_session_info(self):
        return self.mock_object('SessionInfo', path='Trax.Algo.Calculations.Core.Shortcuts')

    def mock_static_kpi(self):
        static_kpi = self.mock_object('Common.get_kpi_static_data', path='KPIUtils_v2.DB.CommonV2')
        static_kpi.return_value = DataTestUnitTnuva.kpi_static_data
        return static_kpi.return_value

    def mock_scene_item_facts(self, data):
        self.data_provider_data_mock['scene_item_facts'] = data.where(data.notnull(), None)

    def mock_match_product_in_scene(self, data):
        self.data_provider_data_mock['matches'] = data.where(data.notnull(), None)

    def create_scif_matches_stitch_groups_data_mocks(self, scenes_list):
        scif_test_case = self.SCIF
        matches_test_case = self.MATCHES
        scif_scene = scif_test_case[scif_test_case['scene_fk'].isin(scenes_list)]
        matches_scene = matches_test_case[matches_test_case['scene_fk'].isin(scenes_list)]
        self.mock_scene_item_facts(scif_scene)
        self.mock_scif_in_data_provider_as_property(scif_scene)

        self.mock_match_product_in_scene(matches_scene)
        return matches_scene, scif_scene

    def mock_get_last_session_oos_results(self, data):
        last_ses_results = self.mock_object('DBHandler.get_last_session_oos_results',
                                            path='Projects.TNUVAILV2.Utils.DataBaseHandler')
        last_ses_results.return_value = data

    def mock_get_oos_reasons_for_session(self, data):
        kpi_result_value = self.mock_object('DBHandler.get_oos_reasons_for_session',
                                            path='Projects.TNUVAILV2.Utils.DataBaseHandler')
        kpi_result_value.return_value = data

    # def mock_project_name_property(self):
    #     p = PropertyMock(return_value=self.data_provider.project_name)
    #     type(self.data_provider_mock).project_name = p

    def mock_session_info_property(self, data):
        p = PropertyMock(return_value=data)
        type(self.data_provider_mock).session_info = p

    def mock_own_manufacturer_property(self):
        p = PropertyMock(return_value=DataTestUnitTnuva.own_manuf_property)
        type(self.data_provider_mock).own_manufacturer = p

    def mock_scif_in_data_provider_as_property(self, data):
        p = PropertyMock(return_value=data)
        type(self.data_provider_mock).scene_item_facts = p

    def mock_store_id(self):
        self.data_provider_data_mock[Data.STORE_FK] = 1

    # def test_whatever(self):
    #     matches, scene = self.create_scif_matches_stitch_groups_data_mocks([1])
    #     self.mock_get_last_session_oos_results(DataTestUnitTnuva.previous_results_empty)
    #     self.mock_get_oos_reasons_for_session(DataTestUnitTnuva.oos_exclude_res_empty)
    #     self.mock_session_info_property(DataTestUnitTnuva.session_info_new)
    #     tool_box = TNUVAILToolBox(self.data_provider_mock, self.output)
    #     print tool_box.kpi_result_types

    def test_get_relevant_assortment_instance_does_not_change_scif_and_data_provider_if_session_new(self):
        matches, scene = self.create_scif_matches_stitch_groups_data_mocks([1])
        self.mock_get_last_session_oos_results(DataTestUnitTnuva.previous_results_empty)
        self.mock_get_oos_reasons_for_session(DataTestUnitTnuva.oos_exclude_res_1)
        self.mock_session_info_property(DataTestUnitTnuva.session_info_new)
        tool_box = TNUVAILToolBox(self.data_provider_mock, self.output)
        self.assertEquals(len(tool_box.scif), 13)
        self.assertEquals(len(tool_box.data_provider[Data.SCENE_ITEM_FACTS]), 13)
        assortment = tool_box.get_relevant_assortment_instance(tool_box.assortment)
        self.assertEquals(len(assortment.data_provider[Data.SCENE_ITEM_FACTS]), 13)
        self.assertEquals(len(tool_box.scif), 13)

    def test_get_relevant_assortment_instance_changes_scif_and_data_provider_if_session_completed_and_att3_eq_scene(self):
        matches, scene = self.create_scif_matches_stitch_groups_data_mocks([1])
        self.mock_get_last_session_oos_results(DataTestUnitTnuva.previous_results_empty)
        self.mock_get_oos_reasons_for_session(DataTestUnitTnuva.oos_exclude_res_1)
        self.mock_session_info_property(DataTestUnitTnuva.session_info_completed)
        tool_box = TNUVAILToolBox(self.data_provider_mock, self.output)
        self.assertEquals(len(tool_box.scif), 13)
        self.assertEquals(len(tool_box.data_provider[Data.SCENE_ITEM_FACTS]), 13)
        assortment = tool_box.get_relevant_assortment_instance(tool_box.assortment)
        self.assertEquals(len(assortment.data_provider[Data.SCENE_ITEM_FACTS]), 14)
        self.assertEquals(len(tool_box.scif), 14)

    def test_get_relevant_assortment_instance_does_not_change_scif_and_data_provider_if_session_completed_and_att3_not_eq_scene(self):
        matches, scene = self.create_scif_matches_stitch_groups_data_mocks([1])
        self.mock_get_last_session_oos_results(DataTestUnitTnuva.previous_results_empty)
        self.mock_get_oos_reasons_for_session(DataTestUnitTnuva.oos_exclude_res_2)
        self.mock_session_info_property(DataTestUnitTnuva.session_info_completed)
        tool_box = TNUVAILToolBox(self.data_provider_mock, self.output)
        self.assertEquals(len(tool_box.scif), 13)
        self.assertEquals(len(tool_box.data_provider[Data.SCENE_ITEM_FACTS]), 13)
        assortment = tool_box.get_relevant_assortment_instance(tool_box.assortment)
        self.assertEquals(len(assortment.data_provider[Data.SCENE_ITEM_FACTS]), 13)
        self.assertEquals(len(tool_box.scif), 13)

    def test_get_relevant_assortment_instance_appends_rows_to_scif_and_data_provider_accordingly_based_on_oos_res(self):
        matches, scene = self.create_scif_matches_stitch_groups_data_mocks([1, 2])
        self.mock_get_last_session_oos_results(DataTestUnitTnuva.previous_results_empty)
        self.mock_get_oos_reasons_for_session(DataTestUnitTnuva.oos_exclude_res_3)
        self.mock_session_info_property(DataTestUnitTnuva.session_info_completed)
        tool_box = TNUVAILToolBox(self.data_provider_mock, self.output)
        self.assertEquals(len(tool_box.scif), 18)
        self.assertEquals(len(tool_box.data_provider[Data.SCENE_ITEM_FACTS]), 18)
        assortment = tool_box.get_relevant_assortment_instance(tool_box.assortment)
        self.assertEquals(len(assortment.data_provider[Data.SCENE_ITEM_FACTS]), 20)
        self.assertEquals(len(tool_box.scif), 20)

        expected_results = list()
        expected_results.append({'product_fk': 6, 'scene_fk': 1, 'facings': 1, 'template_fk': 2})
        expected_results.append({'product_fk': 11, 'scene_fk': 2, 'facings': 1, 'template_fk': 1})
        test_result_list = []
        for expected_result in expected_results:
            test_result_list.append(self.check_results(tool_box.scif, expected_result) == 1)
        self.assertTrue(all(test_result_list))

        test_result_list = []
        for expected_result in expected_results:
            test_result_list.append(self.check_results(assortment.data_provider[Data.SCENE_ITEM_FACTS],
                                                       expected_result) == 1)
        self.assertTrue(all(test_result_list))

    # Test session has only milk shelf. Only SOS with respect to milk products is expected to be calculated
    # Empties are ignored. Stack is not included.
    def test_calculate_facings_sos_calculates_kpis_only_for_policies_existing_in_store(self):
        matches, scene = self.create_scif_matches_stitch_groups_data_mocks([3])
        self.mock_get_last_session_oos_results(DataTestUnitTnuva.previous_results_empty)
        self.mock_get_oos_reasons_for_session(DataTestUnitTnuva.oos_exclude_res_1)
        self.mock_session_info_property(DataTestUnitTnuva.session_info_new)
        tool_box = TNUVAILToolBox(self.data_provider_mock, self.output)
        tool_box.common_v2.write_to_db_result = MagicMock()
        tool_box._calculate_facings_sos()

        results_df = self.build_results_df(tool_box)
        self.assertEquals(len(results_df[results_df['fk'] == 1997]), 1)
        self.assertEquals(len(results_df[results_df['fk'] == 1998]), 3)
        self.assertEquals(len(results_df[results_df['fk'] == 1999]), 4)
        self.assertEquals(len(results_df[results_df['fk'] == 2000]), 0)
        self.assertEquals(len(results_df[results_df['fk'] == 2001]), 0)
        self.assertEquals(len(results_df[results_df['fk'] == 2002]), 0)

        expected_list = []
        expected_list.append({'fk': 1997, 'numerator_id': 810, 'numerator_result': 3,
                              'denominator_result': 8, 'score': round(3.0/8 * 100, 2),
                              'result': round(3.0/8 * 100, 2)})
        expected_list.append({'fk': 1998, 'numerator_id': 262, 'score': 2.0/5 * 100, 'denominator_result': 5,
                              'numerator_result': 2, 'denominator_id': 810, 'result': 2.0/5 * 100})
        expected_list.append({'fk': 1998, 'numerator_id': 252, 'score': 0, 'denominator_result': 2,
                              'numerator_result': 0, 'denominator_id': 810, 'result': 0})
        expected_list.append({'fk': 1998, 'numerator_id': 269, 'score': 100, 'denominator_result': 1,
                              'numerator_result': 1, 'denominator_id': 810, 'result': 100})

        expected_list.append({'fk': 1999, 'numerator_id': 810, 'score': 2.0 / 5 * 100, 'denominator_result': 5,
                              'numerator_result': 2, 'denominator_id': 262, 'result': 2.0 / 5 * 100})
        expected_list.append({'fk': 1999, 'numerator_id': 2, 'score': 3.0 / 5 * 100, 'denominator_result': 5,
                              'numerator_result': 3, 'denominator_id': 262, 'result': 3.0 / 5 * 100})
        expected_list.append({'fk': 1999, 'numerator_id': 2, 'score': 100, 'denominator_result': 2,
                              'numerator_result': 2, 'denominator_id': 252, 'result': 100})
        expected_list.append({'fk': 1999, 'numerator_id': 810, 'score': 100, 'denominator_result': 1,
                              'numerator_result': 1, 'denominator_id': 269, 'result': 100})

        test_result_list = []
        for expected_result in expected_list:
            test_result_list.append(self.check_results(results_df, expected_result) == 1)
        self.assertTrue(all(test_result_list))

    def test_calculate_assortment_no_kpi_results_if_assortment_list_empty(self):
        matches, scene = self.create_scif_matches_stitch_groups_data_mocks([1])
        ass_res = self.mock_object('Assortment.get_lvl3_relevant_ass',
                                   path='KPIUtils_v2.Calculations.AssortmentCalculations')
        ass_res.return_value = pd.DataFrame()
        self.mock_get_last_session_oos_results(DataTestUnitTnuva.previous_results_empty)
        self.mock_get_oos_reasons_for_session(DataTestUnitTnuva.oos_exclude_res_empty)
        self.mock_session_info_property(DataTestUnitTnuva.session_info_new)
        tool_box = TNUVAILToolBox(self.data_provider_mock, self.output)
        tool_box._calculate_assortment()
        self.assertEquals(len(tool_box.common_v2.kpi_results), 0)


    # discuss:
    # SKU lvl ass kpi: denominator is zero if product not in store
    # all ass kpis have actually denom_id as store_id - does not match DB
    def test_calculate_assortment_if_only_dairy_shelves_in_store(self):
        matches, scene = self.create_scif_matches_stitch_groups_data_mocks([1])
        self.mock_get_last_session_oos_results(DataTestUnitTnuva.previous_results_no_session)
        self.mock_get_oos_reasons_for_session(DataTestUnitTnuva.oos_exclude_res_empty)
        self.mock_session_info_property(DataTestUnitTnuva.session_info_new)
        tool_box = TNUVAILToolBox(self.data_provider_mock, self.output)
        tool_box.common_v2.write_to_db_result = MagicMock()
        tool_box._calculate_assortment()
        results_df = self.build_results_df(tool_box)
        # x = results_df[['fk', 'numerator_id', 'numerator_result', 'denominator_id', 'denominator_result', 'result',
        #                  'score']]
        # distribution part
        self.assertEquals(len(results_df[results_df['fk'] == 2006]), 0)
        self.assertEquals(len(results_df[results_df['fk'] == 2007]), 0)
        self.assertEquals(len(results_df[results_df['fk'] == 2008]), 0)
        self.assertEquals(len(results_df[results_df['fk'] == 2003]), 1)
        self.assertEquals(len(results_df[results_df['fk'] == 2004]), 5)
        self.assertEquals(len(results_df[results_df['fk'] == 2005]), 6)
        # oos part
        self.assertEquals(len(results_df[results_df['fk'] == 2009]), 1)
        self.assertEquals(len(results_df[results_df['fk'] == 2010]), 4)
        self.assertEquals(len(results_df[results_df['fk'] == 2011]), 4)

        self.assertEquals(len(results_df[results_df['fk'] == 2016]), 1)

        expected_list = []
        # distribution part
        expected_list.append({'fk': 2003, 'numerator_id': 810, 'numerator_result': 5,
                              'denominator_result': 6, 'score': round(5.0 / 6 * 100, 2),
                              'result': round(5.0 / 6 * 100, 2)})
        expected_list.append({'fk': 2004, 'numerator_id': 272, 'score': 100, 'denominator_result': 1,
                              'numerator_result': 1, 'result': 100})
        expected_list.append({'fk': 2004, 'numerator_id': 262, 'score': 50, 'denominator_result': 2,
                              'numerator_result': 1, 'result': 50})
        expected_list.append({'fk': 2004, 'numerator_id': 252, 'score': 100, 'denominator_result': 1,
                              'numerator_result': 1, 'result': 100})
        expected_list.append({'fk': 2004, 'numerator_id': 264, 'score': 100, 'denominator_result': 1,
                              'numerator_result': 1, 'result': 100})
        expected_list.append({'fk': 2004, 'numerator_id': 254, 'score': 100, 'denominator_result': 1,
                              'numerator_result': 1, 'result': 100})

        expected_list.append({'fk': 2005, 'numerator_id': 1, 'score': 100, 'denominator_result': 1,
                              'numerator_result': 1, 'result': 2})
        expected_list.append({'fk': 2005, 'numerator_id': 2, 'score': 100, 'denominator_result': 1,
                              'numerator_result': 1, 'result': 2})
        expected_list.append({'fk': 2005, 'numerator_id': 3, 'score': 100, 'denominator_result': 1,
                              'numerator_result': 1, 'result': 2})
        expected_list.append({'fk': 2005, 'numerator_id': 4, 'score': 100, 'denominator_result': 1,
                              'numerator_result': 1, 'result': 2})
        expected_list.append({'fk': 2005, 'numerator_id': 5, 'score': 100, 'denominator_result': 1,
                              'numerator_result': 1, 'result': 2})
        expected_list.append({'fk': 2005, 'numerator_id': 6, 'score': 0, 'denominator_result': 0,
                              'numerator_result': 0, 'result': 1}) # discuss re denom result
        # oos part - takes only products that are obligatory (denom res)
        expected_list.append({'fk': 2009, 'numerator_id': 810, 'numerator_result': 0,
                              'denominator_result': 4, 'score': 0, 'result': 0})
        expected_list.append({'fk': 2016, 'numerator_id': 810, 'numerator_result': 0,
                              'denominator_result': 4, 'score': 0, 'result': 0})

        test_result_list = []
        for expected_result in expected_list:
            test_result_list.append(self.check_results(results_df, expected_result) == 1)
        self.assertTrue(all(test_result_list))

    def test_calculate_assortment_if_only_meat_shelves_in_store(self):
        matches, scene = self.create_scif_matches_stitch_groups_data_mocks([2])
        self.mock_get_last_session_oos_results(DataTestUnitTnuva.previous_results_no_session)
        self.mock_get_oos_reasons_for_session(DataTestUnitTnuva.oos_exclude_res_empty)
        self.mock_session_info_property(DataTestUnitTnuva.session_info_new)
        tool_box = TNUVAILToolBox(self.data_provider_mock, self.output)
        tool_box.common_v2.write_to_db_result = MagicMock()
        tool_box._calculate_assortment()
        results_df = self.build_results_df(tool_box)
        # distribution part
        self.assertEquals(len(results_df[results_df['fk'] == 2006]), 1)
        self.assertEquals(len(results_df[results_df['fk'] == 2007]), 1)
        self.assertEquals(len(results_df[results_df['fk'] == 2008]), 5)

        self.assertEquals(len(results_df[results_df['fk'] == 2003]), 0)
        self.assertEquals(len(results_df[results_df['fk'] == 2004]), 0)
        self.assertEquals(len(results_df[results_df['fk'] == 2005]), 0)
        # oos part
        self.assertEquals(len(results_df[results_df['fk'] == 2012]), 1)
        self.assertEquals(len(results_df[results_df['fk'] == 2013]), 1)
        self.assertEquals(len(results_df[results_df['fk'] == 2015]), 5)

        self.assertEquals(len(results_df[results_df['fk'] == 2016]), 1)

        expected_list = []
        # distribution part
        expected_list.append({'fk': 2006, 'numerator_id': 810, 'numerator_result': 3,
                              'denominator_result': 5, 'score': round(3.0 / 5 * 100, 2),
                              'result': round(3.0 / 5 * 100, 2)})
        expected_list.append({'fk': 2007, 'numerator_id': 260, 'score': round(3.0 / 5 * 100, 2),
                              'denominator_result': 5, 'numerator_result': 3, 'result': round(3.0 / 5 * 100, 2)})

        expected_list.append({'fk': 2008, 'numerator_id': 7, 'score': 100, 'denominator_result': 1,
                              'numerator_result': 1, 'result': 2})
        expected_list.append({'fk': 2008, 'numerator_id': 8, 'score': 100, 'denominator_result': 1,
                              'numerator_result': 1, 'result': 2})
        expected_list.append({'fk': 2008, 'numerator_id': 9, 'score': 100, 'denominator_result': 1,
                              'numerator_result': 1, 'result': 2})
        expected_list.append({'fk': 2008, 'numerator_id': 10, 'score': 0, 'denominator_result': 0,
                              'numerator_result': 0, 'result': 1})
        expected_list.append({'fk': 2008, 'numerator_id': 11, 'score': 0, 'denominator_result': 0,
                              'numerator_result': 0, 'result': 1})

        # oos part - takes only products that are obligatory (denom res)
        expected_list.append({'fk': 2012, 'numerator_id': 810, 'numerator_result': 2,
                              'denominator_result': 5, 'score': 2.0/5 * 100, 'result': 2.0/5 * 100})
        expected_list.append({'fk': 2016, 'numerator_id': 810, 'numerator_result': 2,
                              'denominator_result': 5, 'score': 0, 'result': 2.0 / 5 * 100})

        test_result_list = []
        for expected_result in expected_list:
            test_result_list.append(self.check_results(results_df, expected_result) == 1)
        self.assertTrue(all(test_result_list))

    def test_calculate_assortment_both_scene_types_in_session_check_totals(self):
        matches, scene = self.create_scif_matches_stitch_groups_data_mocks([2, 3])
        self.mock_get_last_session_oos_results(DataTestUnitTnuva.previous_results_no_session)
        self.mock_get_oos_reasons_for_session(DataTestUnitTnuva.oos_exclude_res_empty)
        self.mock_session_info_property(DataTestUnitTnuva.session_info_new)
        tool_box = TNUVAILToolBox(self.data_provider_mock, self.output)
        tool_box.common_v2.write_to_db_result = MagicMock()
        tool_box._calculate_assortment()
        results_df = self.build_results_df(tool_box)
        x = results_df[['fk', 'numerator_id', 'numerator_result', 'denominator_id', 'denominator_result', 'result',
                         'score']]
        self.assertEquals(len(results_df[results_df['fk'] == 2006]), 1)
        self.assertEquals(len(results_df[results_df['fk'] == 2003]), 1)

        self.assertEquals(len(results_df[results_df['fk'] == 2009]), 1)
        self.assertEquals(len(results_df[results_df['fk'] == 2012]), 1)
        self.assertEquals(len(results_df[results_df['fk'] == 2016]), 1)

        expected_list = []
        expected_list.append({'fk': 2006, 'numerator_id': 810, 'numerator_result': 3,
                              'denominator_result': 5, 'score': round(3.0 / 5 * 100, 2),
                              'result': round(3.0 / 5 * 100, 2)})
        expected_list.append({'fk': 2003, 'numerator_id': 810, 'numerator_result': 2,
                              'denominator_result': 6, 'score': round(2.0 / 6 * 100, 2),
                              'result': round(2.0 / 6 * 100, 2)})

        expected_list.append({'fk': 2009, 'numerator_id': 810, 'numerator_result': 3,
                              'denominator_result': 4, 'score': 3.0 / 4 * 100, 'result': 3.0 / 4 * 100})
        expected_list.append({'fk': 2012, 'numerator_id': 810, 'numerator_result': 2,
                              'denominator_result': 5, 'score': 2.0 / 5 * 100, 'result': 2.0 / 5 * 100})
        expected_list.append({'fk': 2016, 'numerator_id': 810, 'numerator_result': 5,
                              'denominator_result': 9, 'score': 0, 'result': round(5.0 / 9 * 100, 2)})

        test_result_list = []
        for expected_result in expected_list:
            test_result_list.append(self.check_results(results_df, expected_result) == 1)
        self.assertTrue(all(test_result_list))

    # def test_meat_included(self): # when we change the code
    #     matches, scene = self.create_scif_matches_stitch_groups_data_mocks([2])
    #     self.mock_get_last_session_oos_results(DataTestUnitTnuva.previous_results_empty)
    #     self.mock_get_oos_reasons_for_session(DataTestUnitTnuva.oos_exclude_res_1)
    #     self.mock_session_info_property(DataTestUnitTnuva.session_info_new)
    #     tool_box = TNUVAILToolBox(self.data_provider_mock, self.output)
    #     tool_box.common_v2.write_to_db_result = MagicMock()
    #     tool_box._calculate_facings_sos()

    @staticmethod
    def check_results(results_df, expected_results_dict):
        column = []
        expression = []
        condition = []
        for key, value in expected_results_dict.items():
            column.append(key)
            expression.append('==')
            condition.append(value)
        query = ' & '.join('{} {} {}'.format(i, j, k) for i, j, k in zip(column, expression, condition))
        filtered_df = results_df.query(query)
        return len(filtered_df)

    @staticmethod
    def build_results_df(tool_box):
        results_list = [res[2] for res in tool_box.common_v2.write_to_db_result.mock_calls]
        results_df = pd.DataFrame.from_records(results_list)
        return results_df

    # @staticmethod
    # def build_results_df_and_hierarchies_dict_new_tables(tool_box, *keys_for_hierarchy):
    #     results_list = [res[2] for res in tool_box.common.write_to_db_result.mock_calls]
    #     results_df = pd.DataFrame.from_records(results_list)
    #
    #     hierarchies_dict = {}
    #     for res in results_list:
    #         h_keys = [res.get(k) for k in keys_for_hierarchy]
    #         h_keys = tuple(filter(lambda x: x == x and x is not None, h_keys))
    #         # h_keys = tuple([res.get(k) for k in keys_for_hierarchy])
    #         hierarchies_dict.update({h_keys: res.get('identifier_parent')})
    #
    #     return results_df, hierarchies_dict

