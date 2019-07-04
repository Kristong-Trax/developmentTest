# -*- coding: utf-8 -*-
import random

from Trax.Utils.Testing.Case import TestUnitCase
from mock import MagicMock
from Projects.PNGCN_PROD.SceneKpis.KPISceneToolBox import PngcnSceneKpis
import pandas as pd
import numpy

from Tests.TestUtils import remove_cache_and_storage

__author__ = 'avrahama'


class TestPngcn(TestUnitCase):
    @property
    def import_path(self):
        return 'Projects.PNGCN_PROD.SceneKpis.KPISceneToolBox.PngcnSceneKpis'

    def set_up(self):
        super(TestPngcn, self).set_up()
        remove_cache_and_storage()
        # mock PSProjectConnector
        self.ProjectConnector_mock = self.mock_object(
            'ProjectConnector', path='KPIUtils_v2.DB.PsProjectConnector')
        self.PSProjectConnector = self.mock_object('PSProjectConnector',
                                                   path='KPIUtils_v2.DB.PsProjectConnector')

        # mock 'Common' object used in toolbox
        self.common_mock = self.mock_object(
            'Common.get_kpi_fk_by_kpi_name', path='KPIUtils_v2.DB.CommonV2')
        self.SessionInfo_mock = self.mock_object(
            'SessionInfo', path='Trax.Algo.Calculations.Core.Shortcuts')
        self.common_mock.return_value = 3

        # get the relevant DFs
        matches = pd.read_csv('Data/matches.csv')
        scif = pd.read_csv('Data/scif.csv')
        all_products = pd.read_csv('Data/all_products.csv')
        session_info = pd.read_csv('Data/session_info.csv')

        # create a dict of data_provider object relevant attributes
        my_dict = {'matches': matches,
                   'scene_item_facts': scif,
                   'all_products': all_products,
                   'session_info': session_info,
                   'store_fk': session_info['store_fk'].iloc[0],
                   'visit_date': session_info['visit_date'].iloc[0],
                   'session_and_store_info': pd.DataFrame({'values': [4, 67, 8, 2]})}

        # decode manufacturer_name (to work around get_png_manufacturer_fk method)
        my_dict['all_products']['manufacturer_name'] = my_dict['all_products']['manufacturer_name'].str.decode(
            'utf8')

        # mock 'data provider' object giving to the toolbox
        self.data_provider_mock = MagicMock()

        # making data_provider_mock behave like a dict
        self.data_provider_mock.__getitem__.side_effect = my_dict.__getitem__
        self.data_provider_mock.__iter__.side_effect = my_dict.__iter__

    def test_insert_data_into_custom_scif(self):
        """
            test type delete qury type.
        """
        scene_tool_box = PngcnSceneKpis(self.ProjectConnector_mock,
                                        self.common_mock, 16588190,
                                        self.data_provider_mock)
        scene_tool_box.data_provider.session_id = 'ebebc629-6b82-4be8-a872-0caa248ea248'
        new_scif = pd.read_csv('Data/new_scif.csv')
        scene_tool_box.common.execute_custom_query()
        scene_tool_box.insert_data_into_custom_scif(new_scif)
        delete_query = scene_tool_box.common.execute_custom_query.mock_calls[1][1][0]
        insert_query = scene_tool_box.common.execute_custom_query.mock_calls[2][1][0]
        self.assertIsInstance(delete_query, str)
        self.assertIsInstance(insert_query, str)

    def test_calculate_result_sanity(self):
        """
            test that function returns zero for denominator=0
        """
        scene_tool_box = PngcnSceneKpis(self.ProjectConnector_mock,
                                        self.common_mock, 16588190,
                                        self.data_provider_mock)
        # test that function returns zero for denominator=0
        numerator, denominator = 12, 0
        result = scene_tool_box.calculate_result(numerator, denominator)
        expected_result = 0
        self.assertEqual(expected_result, result)

    def test_calculate_result_type(self):
        """
            test that the result is float
        """
        scene_tool_box = PngcnSceneKpis(self.ProjectConnector_mock,
                                        self.common_mock, 16588190,
                                        self.data_provider_mock)
        # test that the result is float
        numerator, denominator = 12, 15
        result = type(scene_tool_box.calculate_result(numerator, denominator))
        expected_result = float
        self.assertEqual(expected_result, result)

    def test_get_png_manufacturer_fk(self):
        """
            test the result type
        """
        scene_tool_box = PngcnSceneKpis(self.ProjectConnector_mock,
                                        self.common_mock, 16588190,
                                        self.data_provider_mock)

        result = type(scene_tool_box.get_png_manufacturer_fk())
        expected_result = numpy.int64
        self.assertEqual(expected_result, result)

    def test__get_display_size_of_product_in_scene(self):
        """
            test that the result is a DF
        """
        scene_tool_box = PngcnSceneKpis(self.ProjectConnector_mock,
                                        self.common_mock, 16588190,
                                        self.data_provider_mock)
        self.assertIsInstance(scene_tool_box._get_display_size_of_product_in_scene(),
                              type(pd.DataFrame()))

    def test_calculate_display_size_empty_df(self):
        """
            test that we don't return any thing when the used df is empty
        """
        scene_tool_box = PngcnSceneKpis(self.ProjectConnector_mock,
                                        self.common_mock, 16588190,
                                        self.data_provider_mock)
        scene_tool_box.mock_df_products_size = \
            self.mock_object('_get_display_size_of_product_in_scene',
                             path='Projects.PNGCN_PROD.SceneKpis.KPISceneToolBox.PngcnSceneKpis')
        # test that we don't return any thing when the used df is empty
        scene_tool_box.mock_df_products_size.return_value = pd.DataFrame({})
        result = scene_tool_box.calculate_display_size()
        expected_result = None
        self.assertEqual(expected_result, result)

    def test_calculate_display_size_correct_results_type(self):
        """
            test that the type of the numerator and denominator is float
        """
        scene_tool_box = PngcnSceneKpis(self.ProjectConnector_mock,
                                        self.common_mock, 16588190,
                                        self.data_provider_mock)
        mock_df_products_size = self.mock_object('_get_display_size_of_product_in_scene',
                                                 path='Projects.PNGCN_PROD.SceneKpis.KPISceneToolBox.PngcnSceneKpis')
        # test that we don't return any thing when the used df is empty
        mock_df_products_size.return_value = pd.DataFrame(
            [{'item_id': 2, 'scene_id': 3, 'product_size': 0.25}])
        # test that we write the correct results to DB
        data_scif = [{u'scene_id': 16588190, u'item_id': 123, u'manufacturer_fk': 4, u'rlv_sos_sc': 1, u'status': 1},
                     {u'scene_id': 16588190, u'item_id': 125,
                      u'manufacturer_fk': 3, u'rlv_sos_sc': 1, u'status': 1},
                     {u'scene_id': 16588190, u'item_id': 136, u'manufacturer_fk': 3, u'rlv_sos_sc': 1, u'status': 1}]
        scene_tool_box.scif = pd.DataFrame(data_scif)
        data_df_products_size = [{'item_id': 123, 'scene_id': 16588190, 'product_size': 1.245},
                                 {'item_id': 124, 'scene_id': 16588190, 'product_size': 0.285},
                                 {'item_id': 125, 'scene_id': 16588190, 'product_size': 1.225},
                                 {'item_id': 126, 'scene_id': 16588190, 'product_size': 0.232},
                                 {'item_id': 136, 'scene_id': 16588190, 'product_size': 0}]
        mock_df_products_size.return_value = pd.DataFrame(data_df_products_size)
        scene_tool_box.common.write_to_db_result = MagicMock()
        scene_tool_box.calculate_display_size()
        kpi_results = scene_tool_box.common.write_to_db_result.mock_calls[0][2]
        if kpi_results:
            numerator = kpi_results['numerator_result']
            denominator = kpi_results['denominator_result']
            # test that the type of the numerator and denominator is float
            self.assertIsInstance(denominator, float)
            self.assertIsInstance(numerator, float)

    def test_calculate_display_size_correct_results_length(self):
        """
            test result length
        """
        scene_tool_box = PngcnSceneKpis(self.ProjectConnector_mock,
                                        self.common_mock, 16588190,
                                        self.data_provider_mock)
        mock_df_products_size = self.mock_object('_get_display_size_of_product_in_scene',
                                                 path='Projects.PNGCN_PROD.SceneKpis.KPISceneToolBox.PngcnSceneKpis')

        # test that we don't return any thing when the used df is empty
        mock_df_products_size.return_value = pd.DataFrame(
            [{'item_id': 2, 'scene_id': 3, 'product_size': 0.25}])

        # test that we write the correct results to DB
        data_scif = [{u'scene_id': 16588190, u'item_id': 123, u'manufacturer_fk': 4, u'rlv_sos_sc': 1, u'status': 1},
                     {u'scene_id': 16588190, u'item_id': 125,
                      u'manufacturer_fk': 3, u'rlv_sos_sc': 1, u'status': 1},
                     {u'scene_id': 16588190, u'item_id': 136, u'manufacturer_fk': 3, u'rlv_sos_sc': 1, u'status': 1}]
        scene_tool_box.scif = pd.DataFrame(data_scif)
        data_df_products_size = [{'item_id': 123, 'scene_id': 16588190, 'product_size': 1.245},
                                 {'item_id': 124, 'scene_id': 16588190, 'product_size': 0.285},
                                 {'item_id': 125, 'scene_id': 16588190, 'product_size': 1.225},
                                 {'item_id': 126, 'scene_id': 16588190, 'product_size': 0.232},
                                 {'item_id': 136, 'scene_id': 16588190, 'product_size': 0}]
        mock_df_products_size.return_value = pd.DataFrame(data_df_products_size)
        scene_tool_box.common.write_to_db_result = MagicMock()
        scene_tool_box.calculate_display_size()
        kpi_results = scene_tool_box.common.write_to_db_result.mock_calls[0][2]
        if kpi_results:
            # test that we write 8 fields to DB
            self.assertEqual(len(kpi_results), 8, 'expects to write 8 parameters to db')

    def test_calculate_display_size_correct_results_sanity(self):
        """
            test if the numerator is greater then denominator (if the subgroup is greater then containing group)
        """
        scene_tool_box = PngcnSceneKpis(self.ProjectConnector_mock,
                                        self.common_mock, 16588190,
                                        self.data_provider_mock)
        mock_df_products_size = self.mock_object('_get_display_size_of_product_in_scene',
                                                 path='Projects.PNGCN_PROD.SceneKpis.KPISceneToolBox.PngcnSceneKpis')

        # test that we write the correct results to DB
        data_scif = [{u'scene_id': 16588190, u'item_id': 123, u'manufacturer_fk': 4, u'rlv_sos_sc': 1, u'status': 1},
                     {u'scene_id': 16588190, u'item_id': 125,
                      u'manufacturer_fk': 3, u'rlv_sos_sc': 1, u'status': 1},
                     {u'scene_id': 16588190, u'item_id': 136, u'manufacturer_fk': 3, u'rlv_sos_sc': 1, u'status': 1}]
        scene_tool_box.scif = pd.DataFrame(data_scif)
        data_df_products_size = [{'item_id': 123, 'scene_id': 16588190, 'product_size': 1.245},
                                 {'item_id': 124, 'scene_id': 16588190, 'product_size': 0.285},
                                 {'item_id': 125, 'scene_id': 16588190, 'product_size': 1.225},
                                 {'item_id': 126, 'scene_id': 16588190, 'product_size': 0.232},
                                 {'item_id': 136, 'scene_id': 16588190, 'product_size': 0}]
        mock_df_products_size.return_value = pd.DataFrame(data_df_products_size)
        scene_tool_box.common.write_to_db_result = MagicMock()
        scene_tool_box.calculate_display_size()
        kpi_results = scene_tool_box.common.write_to_db_result.mock_calls[0][2]
        if kpi_results:
            numerator = kpi_results['numerator_result']
            denominator = kpi_results['denominator_result']
            # test if the numerator is greater then denominator (if the subgroup is greater then containing group)
            self.assertGreaterEqual(denominator, numerator,
                                    'the numerator cant be greater then denominator')

    def test__get_filterd_matches_test_type(self):
        """
            test that the result is a DF
        """
        scene_tool_box = PngcnSceneKpis(self.ProjectConnector_mock,
                                        self.common_mock, 16588190,
                                        self.data_provider_mock)
        # test that the result is a DF
        df_type = scene_tool_box.get_filterd_matches()
        self.assertIsInstance(df_type, type(pd.DataFrame()))

    def test__get_filterd_matches_test_manufacturer_size(self):
        """
            test that there is only one manufacturer fk in the return DF
        """
        scene_tool_box = PngcnSceneKpis(self.ProjectConnector_mock,
                                        self.common_mock, 16588190,
                                        self.data_provider_mock)
        # test that the result is a DF
        df_type = scene_tool_box.get_filterd_matches()
        # test that there is only one manufacturer fk in the return DF
        unique_fk = len(df_type['manufacturer_fk'].unique())
        self.assertEqual(unique_fk, 1)

    def test__get_filterd_matches_test_manufacturer_fk(self):
        """
            test that the only manufacturer fk in DF is Png
        """
        scene_tool_box = PngcnSceneKpis(self.ProjectConnector_mock,
                                        self.common_mock, 16588190,
                                        self.data_provider_mock)

        # test that the result is a DF
        df_type = scene_tool_box.get_filterd_matches()

        # test that the only manufacturer fk in DF is Png
        png_fk = df_type['manufacturer_fk'].unique()[0]
        self.assertEqual(png_fk, 4)

    def test_calculate_linear_or_presize_linear_length_test_results_length(self):
        """
            test that we write 8 fields to DB
        """
        scene_tool_box = PngcnSceneKpis(self.ProjectConnector_mock,
                                        self.common_mock, 16588190,
                                        self.data_provider_mock)
        data = [{'scene_fk': 101, 'manufacturer_fk': 2, 'product_fk': 252, 'width_mm': 0.84, 'width_mm_advance': 1.23},
                {'scene_fk': 121, 'manufacturer_fk': 4, 'product_fk': 132,
                 'width_mm': 0.80, 'width_mm_advance': 0.99},
                {'scene_fk': 201, 'manufacturer_fk': 4, 'product_fk': 152,
                 'width_mm': 0.28, 'width_mm_advance': 0.75},
                {'scene_fk': 151, 'manufacturer_fk': 5, 'product_fk': 172, 'width_mm': 0.95, 'width_mm_advance': 0.15}]
        scene_tool_box.get_filterd_matches = MagicMock(return_value=pd.DataFrame(data))
        scene_tool_box.png_manufacturer_fk = 4
        scene_tool_box.common.write_to_db_result = MagicMock()
        width = random.choice(['width_mm', 'width_mm_advance'])
        scene_tool_box.calculate_linear_or_presize_linear_length(width)
        kpi_results = scene_tool_box.common.write_to_db_result.mock_calls[0][2]
        if kpi_results:
            self.assertEqual(len(kpi_results), 8, 'expects to write 8 parameters to db')

    def test_calculate_linear_or_presize_linear_length_test_results_type(self):
        """
            test that the type of the numerator and denominator is float
        """
        scene_tool_box = PngcnSceneKpis(self.ProjectConnector_mock,
                                        self.common_mock, 16588190,
                                        self.data_provider_mock)
        data = [{'scene_fk': 101, 'manufacturer_fk': 2, 'product_fk': 252, 'width_mm': 0.84, 'width_mm_advance': 1.23},
                {'scene_fk': 121, 'manufacturer_fk': 4, 'product_fk': 132,
                 'width_mm': 0.80, 'width_mm_advance': 0.99},
                {'scene_fk': 201, 'manufacturer_fk': 4, 'product_fk': 152,
                 'width_mm': 0.28, 'width_mm_advance': 0.75},
                {'scene_fk': 151, 'manufacturer_fk': 5, 'product_fk': 172, 'width_mm': 0.95, 'width_mm_advance': 0.15}]
        scene_tool_box.get_filterd_matches = MagicMock(return_value=pd.DataFrame(data))
        scene_tool_box.png_manufacturer_fk = 4
        scene_tool_box.common.write_to_db_result = MagicMock()
        width = random.choice(['width_mm', 'width_mm_advance'])
        scene_tool_box.calculate_linear_or_presize_linear_length(width)
        kpi_results = scene_tool_box.common.write_to_db_result.mock_calls[0][2]
        if kpi_results:
            numerator = kpi_results['numerator_result']
            denominator = kpi_results['denominator_result']
            self.assertIsInstance(denominator, float)
            self.assertIsInstance(numerator, float)

    def test_calculate_linear_or_presize_linear_length_test_results_sanity(self):
        """
            test if the numerator is greater then denominator (if the subgroup is greater then containing group)
        """
        scene_tool_box = PngcnSceneKpis(self.ProjectConnector_mock,
                                        self.common_mock, 16588190,
                                        self.data_provider_mock)
        data = [{'scene_fk': 101, 'manufacturer_fk': 2, 'product_fk': 252, 'width_mm': 0.84, 'width_mm_advance': 1.23},
                {'scene_fk': 121, 'manufacturer_fk': 4, 'product_fk': 132,
                 'width_mm': 0.80, 'width_mm_advance': 0.99},
                {'scene_fk': 201, 'manufacturer_fk': 4, 'product_fk': 152,
                 'width_mm': 0.28, 'width_mm_advance': 0.75},
                {'scene_fk': 151, 'manufacturer_fk': 5, 'product_fk': 172, 'width_mm': 0.95, 'width_mm_advance': 0.15}]
        scene_tool_box.get_filterd_matches = MagicMock(return_value=pd.DataFrame(data))
        scene_tool_box.png_manufacturer_fk = 4
        scene_tool_box.common.write_to_db_result = MagicMock()
        width = random.choice(['width_mm', 'width_mm_advance'])
        scene_tool_box.calculate_linear_or_presize_linear_length(width)
        kpi_results = scene_tool_box.common.write_to_db_result.mock_calls[0][2]
        if kpi_results:
            numerator = kpi_results['numerator_result']
            denominator = kpi_results['denominator_result']
            self.assertGreaterEqual(denominator, numerator,
                                    'the numerator cant be greater then denominator')

    def test_calculate_linear_length(self):
        """
            test that the function returns 0 (finished as expected)
        """
        scene_tool_box = PngcnSceneKpis(self.ProjectConnector_mock,
                                        self.common_mock, 16588190,
                                        self.data_provider_mock)
        self.assertEquals(scene_tool_box.calculate_linear_length(), 0)

    def test_calculate_presize_linear_length(self):
        """
            test that the function returns 0 (finished as expected)
        """
        scene_tool_box = PngcnSceneKpis(self.ProjectConnector_mock,
                                        self.common_mock, 16588190,
                                        self.data_provider_mock)
        self.assertEquals(scene_tool_box.calculate_presize_linear_length(), 0)

    def test_save_nlsos_as_kpi_results(self):
        """
           test that the score and result are as expected
        """
        scene_tool_box = PngcnSceneKpis(self.ProjectConnector_mock,
                                        self.common_mock, 16588190,
                                        self.data_provider_mock)
        data = [{'gross_len_split_stack_new': 13, 'product_type': 'Irrelevant', 'product_fk': None, 'rlv_sos_sc': 0,
                 'gross_len_split_stack': 0.15},
                {'gross_len_split_stack_new': 65, 'product_type': '', 'product_fk': 252, 'rlv_sos_sc': 1,
                 'gross_len_split_stack': 1.23},
                {'gross_len_split_stack_new': 35, 'product_type': '', 'product_fk': 253, 'rlv_sos_sc': 1,
                 'gross_len_split_stack': 1.23},
                {'gross_len_split_stack_new': 121, 'product_type': 'Irrelevant', 'product_fk': 132, 'rlv_sos_sc': 0,
                 'gross_len_split_stack': 0.99},
                {'gross_len_split_stack_new': 201, 'product_type': 'Irrelevant', 'product_fk': 132, 'rlv_sos_sc': 0,
                 'gross_len_split_stack': 0.75},
                {'gross_len_split_stack_new': 13, 'product_type': '', 'product_fk': 272, 'rlv_sos_sc': 0,
                 'gross_len_split_stack': 0.15}]

        scene_tool_box.common.write_to_db_result = MagicMock()
        scene_tool_box.save_nlsos_as_kpi_results(pd.DataFrame(data))
        kpi_results = scene_tool_box.common.write_to_db_result.mock_calls
        result = kpi_results[0][2]['score']
        expected_result = 65.0
        self.assertEqual(result, expected_result)

    def test_insert_into_kpi_scene_results(self):
        """
           test that the score are as expected
        """
        scene_tool_box = PngcnSceneKpis(self.ProjectConnector_mock,
                                        self.common_mock, 16588190,
                                        self.data_provider_mock)
        data = [{'pk': 101, 'display_group': 5, 'product_fk': 252, 'facings': 0.84, 'product_size': 1.23},
                {'pk': 121, 'display_group': 4, 'product_fk': 132,
                 'facings': 0.80, 'product_size': 0.99},
                {'pk': 201, 'display_group': 4, 'product_fk': 132,
                 'facings': 0.28, 'product_size': 0.75},
                {'pk': 151, 'display_group': 5, 'product_fk': 252, 'facings': 0.95, 'product_size': 0.15}]

        scene_tool_box.get_display_group = MagicMock()
        scene_tool_box.get_display_group.return_value = 1
        scene_tool_box.common.write_to_db_result = MagicMock()
        scene_tool_box.insert_into_kpi_scene_results(data)
        kpi_results = scene_tool_box.common.write_to_db_result.mock_calls
        result = kpi_results[1][2]['result']
        expected_result = 1.23 + 0.15
        score = kpi_results[1][2]['score']
        expected_score = 0.84 + 0.95
        self.assertEqual(result, expected_result)
        self.assertEqual(score, expected_score)

    def test_get_eye_level_shelves(self):
        scene_tool_box = PngcnSceneKpis(self.ProjectConnector_mock,
                                        self.common_mock, 16588190,
                                        self.data_provider_mock)
        data = pd.DataFrame(
            [{'bay_number': 1, 'shelf_number': 3}, {'bay_number': 1, 'shelf_number': 3},
             {'bay_number': 1, 'shelf_number': 6}, {'bay_number': 1, 'shelf_number': 6},
             {'bay_number': 2, 'shelf_number': 2}, {'bay_number': 2, 'shelf_number': 4},
             {'bay_number': 2, 'shelf_number': 10}, {'bay_number': 2, 'shelf_number': 9},
             {'bay_number': 3, 'shelf_number': 1}, {'bay_number': 3, 'shelf_number': 1},
             {'bay_number': 3, 'shelf_number': 2}, {'bay_number': 3, 'shelf_number': 2}])
        scene_tool_box.get_filterd_matches = MagicMock(return_value=pd.DataFrame(data))
        scene_tool_box.common.write_to_db_result = MagicMock()
        kpi_results = scene_tool_box.get_eye_level_shelves(data)
        self.assertEqual(len(kpi_results[kpi_results['bay_number'] == 3]), 4, 'expects to have 4 lines with bay number 3')
        self.assertTrue(kpi_results[kpi_results['bay_number'] == 1].empty, "Expected to have an empty df where bay number =1")

    def test_calculate_sequence_eye_level(self):
        scene_tool_box = PngcnSceneKpis(self.ProjectConnector_mock,
                                        self.common_mock, 16588190,
                                        self.data_provider_mock)
        entity_df = pd.DataFrame([{'entity_fk': 17, 'entity_name': u'SFG Handwash', 'entity_type_fk': 1002,
                      'entity_type_name': u'eye_level_fragments'},
                      {'entity_fk': 18, 'entity_name': u'SFG Bodywash', 'entity_type_fk': 1002,
                       'entity_type_name': u'eye_level_fragments'},
                     {'entity_fk': 27, 'entity_name': u'Competitor Other', 'entity_type_fk': 1002,
                      'entity_type_name': u'eye_level_fragments'}])
        data = pd.DataFrame(
                [{'scene_fk': 16588190, 'manufacturer_name': 'P&G\xe5\xae\x9d\xe6\xb4\x81', 'brand_name': 'Safeguard',
                  'category': 'Personal Cleaning Care', 'product_fk': 252, 'stacking_layer': 1, 'category_fk': 101,
                  'bay_number': 1, 'shelf_number': 2, 'facing_sequence_number': 3, 'sub_category': 'Handwash'},
                {'scene_fk': 16588190, 'manufacturer_name': 'P&G\xe5\xae\x9d\xe6\xb4\x81', 'brand_name': 'Safeguard',
                 'category': 'something', 'product_fk': 132, 'stacking_layer': 2, 'category_fk': 101,
                 'bay_number':1, 'shelf_number': 2, 'facing_sequence_number': 3, 'sub_category': 'Handwash'},
                {'scene_fk': 16588190, 'manufacturer_name': 'P&G\xe5\xae\x9d\xe6\xb4\x81', 'brand_name': 'Safeguard',
                 'category': 'Personal Cleaning Care', 'product_fk': 152, 'stacking_layer': 1, 'category_fk': 102,
                 'bay_number': 2, 'shelf_number': 2, 'facing_sequence_number': 1, 'sub_category': 'Bodywash'},
                {'scene_fk': 16588190, 'manufacturer_name': 'P&G\xe5\xae\x9d\xe6\xb4\x81', 'brand_name': 'Safeguard',
                 'category': 'Personal Cleaning Care', 'product_fk': 172, 'stacking_layer': 1, 'category_fk': 101,
                 'bay_number': 2, 'shelf_number': 2, 'facing_sequence_number': 3, 'sub_category': 'Handwash'},
                 {'scene_fk': 16588190, 'manufacturer_name': 'P&G\xe5\xae\x9d\xe6\xb4\x81', 'brand_name': 'hola',
                  'category': 'Personal Cleaning Care', 'product_fk': 173, 'stacking_layer': 1, 'category_fk': 101,
                  'bay_number': 2, 'shelf_number': 2, 'facing_sequence_number': 2, 'sub_category': 'HOLA'}])
        scene_tool_box.get_filterd_matches = MagicMock(return_value=pd.DataFrame(data))
        scene_tool_box.common.write_to_db_result = MagicMock()
        scene_tool_box.calculate_sequence_eye_level(entity_df, data)
        kpi_results = scene_tool_box.common.write_to_db_result.mock_calls
        if kpi_results:
            self.assertEqual(len(kpi_results), 3, 'expects to write 3 parameters to db')
            self.assertEqual(kpi_results[2][2]['numerator_id'], 17, "numerator_id !=17, sequence written isn't correct")
        else:
            raise Exception('No results were saved')

    def test_calculate_facing_eye_level(self):
        scene_tool_box = PngcnSceneKpis(self.ProjectConnector_mock,
                                        self.common_mock, 16588190,
                                        self.data_provider_mock)
        data = pd.DataFrame(
                [{'scene_fk': 16588190, 'manufacturer_name': 'P&G\xe5\xae\x9d\xe6\xb4\x81', 'brand_name': 'Safeguard',
                  'category': 'Personal Cleaning Care', 'product_fk': 152, 'stacking_layer': 1, 'category_fk': 101,
                  'bay_number': 1, 'shelf_number': 1, 'facing_sequence_number': 3, 'sub_category': 'Handwash'},
                {'scene_fk': 16588190, 'manufacturer_name': 'P&G\xe5\xae\x9d\xe6\xb4\x81', 'brand_name': 'Safeguard',
                 'category': 'something', 'product_fk': 152, 'stacking_layer': 2, 'category_fk': 101,
                 'bay_number':1, 'shelf_number':2, 'facing_sequence_number': 3, 'sub_category': 'Handwash'},
                {'scene_fk': 16588190, 'manufacturer_name': 'P&G\xe5\xae\x9d\xe6\xb4\x81', 'brand_name': 'Safeguard',
                 'category': 'Personal Cleaning Care', 'product_fk': 152, 'stacking_layer': 1, 'category_fk': 102,
                 'bay_number': 2, 'shelf_number': 2, 'facing_sequence_number': 1, 'sub_category': 'Bodywash'},
                {'scene_fk': 16588190, 'manufacturer_name': 'P&G\xe5\xae\x9d\xe6\xb4\x81', 'brand_name': 'Safeguard',
                 'category': 'Personal Cleaning Care', 'product_fk': 152, 'stacking_layer': 1, 'category_fk': 101,
                 'bay_number': 2, 'shelf_number': 2, 'facing_sequence_number': 3, 'sub_category': 'Handwash'},
                 {'scene_fk': 16588190, 'manufacturer_name': 'P&G\xe5\xae\x9d\xe6\xb4\x81', 'brand_name': 'hola',
                  'category': 'Personal Cleaning Care', 'product_fk': 152, 'stacking_layer': 1, 'category_fk': 101,
                  'bay_number': 2, 'shelf_number': 2, 'facing_sequence_number': 2, 'sub_category': 'HOLA'}])
        scene_tool_box.get_filterd_matches = MagicMock(return_value=pd.DataFrame(data))
        scene_tool_box.common.write_to_db_result = MagicMock()
        scene_tool_box.calculate_facing_eye_level(data)
        kpi_results = scene_tool_box.common.write_to_db_result.mock_calls
        if kpi_results:
            self.assertEqual(len(kpi_results), 2, 'expects to write 2 parameters to db')
            self.assertEqual(kpi_results[1][2]['result'], 4, "result isn't 4 although there are 4 facings in shelf 2")
        else:
            raise Exception('No results were saved')