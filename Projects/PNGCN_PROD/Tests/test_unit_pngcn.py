# -*- coding: utf-8 -*-
import random

from Trax.Utils.Testing.Case import TestUnitCase
from mock import MagicMock, Mock
from Projects.PNGCN_PROD.SceneKpis.KPISceneToolBox import PngcnSceneKpis
import pandas as pd

__author__ = 'avrahama'


class Test_PNGCN(TestUnitCase):

    @property
    def import_path(self):
        return 'Projects.PNGCN_PROD.SceneKpis.KPISceneToolBox.PngcnSceneKpis'

    def set_up(self):
        super(Test_PNGCN, self).set_up()

        # mock PSProjectConnector
        self.ProjectConnector_mock = self.mock_object('ProjectConnector', path='KPIUtils_v2.DB.PsProjectConnector')
        self.PSProjectConnector = self.mock_object('PSProjectConnector',
                                                   path='KPIUtils_v2.DB.PsProjectConnector')

        # mock 'Common' object used in toolbox
        self.common_mock = self.mock_object('Common.get_kpi_fk_by_kpi_name', path='KPIUtils_v2.DB.CommonV2')
        self.SessionInfo_mock = self.mock_object('SessionInfo', path='Trax.Algo.Calculations.Core.Shortcuts')
        self.common_mock.return_value = 3

        # get the relevant DFs
        matches = pd.read_csv('Data/matches.csv')
        scif = pd.read_csv('Data/scif.csv')
        all_products = pd.read_csv('Data/all_products.csv')
        session_info = pd.read_csv('Data/session_info.csv')

        # create a dict of data_provider object relevant attributes
        mydict = {'matches': matches,
                  'scene_item_facts': scif,
                  'all_products': all_products,
                  'session_info': session_info,
                  'store_fk': session_info['store_fk'].iloc[0],
                  'visit_date': session_info['visit_date'].iloc[0],
                  'session_and_store_info': pd.DataFrame({'values': [4, 67, 8, 2]})
                  }

        # decode manufacturer_name (to work around get_png_manufacturer_fk method)
        mydict['all_products']['manufacturer_name'] = mydict['all_products']['manufacturer_name'].str.decode('utf8')

        # mock 'data provider' object giving to the toolbox
        self.data_provider_mock = MagicMock()

        # making data_provider_mock behave like a dict
        self.data_provider_mock.__getitem__.side_effect = mydict.__getitem__
        self.data_provider_mock.__iter__.side_effect = mydict.__iter__

    def test__get_filterd_matches(self):
        """
            1. test that the result is a DF
            2. test that there is only one manufacturer fk in the return DF
            3. test that the only manufacturer fk in DF is Png
        """
        scene_tool_box = PngcnSceneKpis(self.ProjectConnector_mock,
                                        self.common_mock, 16588190,
                                        self.data_provider_mock)

        # test that the result is a DF
        DFtype = scene_tool_box.get_filterd_matches()
        self.assertIsInstance(DFtype, type(pd.DataFrame()))

        # test that there is only one manufacturer fk in the return DF
        UniqueFK = len(DFtype['manufacturer_fk'].unique())
        self.assertEqual(UniqueFK, 1)

        # test that the only manufacturer fk in DF is Png
        PngFK = DFtype['manufacturer_fk'].unique()[0]
        self.assertEqual(PngFK, 4)

    def test__get_display_size_of_product_in_scene(self):
        """
            1. test that the result is a DF
        """
        scene_tool_box = PngcnSceneKpis(self.ProjectConnector_mock,
                                        self.common_mock, 16588190,
                                        self.data_provider_mock)
        self.assertIsInstance(scene_tool_box._get_display_size_of_product_in_scene(),
                              type(pd.DataFrame()))

    def test_calculate_linear_length(self):
        """
            test that the function returns 0 (finished as expected)
        """
        scene_tool_box = PngcnSceneKpis(self.ProjectConnector_mock,
                                        self.common_mock, 16588190,
                                        self.data_provider_mock)
        self.assertEquals(scene_tool_box.calculate_linear_length(), 0)

    def test_calculate_presize_linear_lengthh(self):
        """
            test that the function returns 0 (finished as expected)
        """
        scene_tool_box = PngcnSceneKpis(self.ProjectConnector_mock,
                                        self.common_mock, 16588190,
                                        self.data_provider_mock)
        self.assertEquals(scene_tool_box.calculate_presize_linear_length(), 0)

    def test_calculate_linear_or_presize_linear_length(self):
        """
            1. test if the numerator is greater then denominator (if the subgroup is greater then containing group)
            2. test that we write 8 fields to DB
            3. test that the type of the numerator and denominator is float
        """

        scene_tool_box = PngcnSceneKpis(self.ProjectConnector_mock, self.common_mock, 16588190, self.data_provider_mock)
        data = [{'scene_fk': 101, 'manufacturer_fk': 2, 'product_fk': 252, 'width_mm': 0.84, 'width_mm_advance': 1.23},
                {'scene_fk': 121, 'manufacturer_fk': 4, 'product_fk': 132, 'width_mm': 0.80, 'width_mm_advance': 0.99},
                {'scene_fk': 201, 'manufacturer_fk': 4, 'product_fk': 152, 'width_mm': 0.28, 'width_mm_advance': 0.75},
                {'scene_fk': 151, 'manufacturer_fk': 5, 'product_fk': 172, 'width_mm': 0.95, 'width_mm_advance': 0.15}]

        scene_tool_box.get_filterd_matches = MagicMock(return_value=pd.DataFrame(data))
        scene_tool_box.png_manufacturer_fk = 4
        scene_tool_box.common.write_to_db_result = MagicMock()

        width = random.choice(['width_mm', 'width_mm_advance'])
        scene_tool_box.calculate_linear_or_presize_linear_length(width)

        if scene_tool_box.common.write_to_db_result.mock_calls:
            inputs = scene_tool_box.common.write_to_db_result.mock_calls
            numerator = inputs[0][2]['numerator_result']
            denominator = inputs[0][2]['denominator_result']
            self.assertGreaterEqual(denominator, numerator, 'the numerator cant be greater then denominator')
            self.assertEqual(len(inputs[0][2]), 8, 'expects to write 8 parameters to db')
            self.assertIsInstance(denominator, float)
            self.assertIsInstance(numerator, float)
