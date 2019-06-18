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
        self.common_mock.return_value = 3

        # mock 'data provider' object giving to the toolbox
        self.data_provider_mock = MagicMock()

    def test_get_png_manufacturer_fk(self):
        """
            test that the return value type is a string
        """
        scene_tool_box = PngcnSceneKpis(self.ProjectConnector_mock,
                                        self.common_mock, 16588190,
                                        self.data_provider_mock)
        data = [{'manufacturer_name': 'NotPNG', 'manufacturer_fk': 2},
                {'manufacturer_name': 'PNG', 'manufacturer_fk': 4},
                {'manufacturer_name': 'PNG', 'manufacturer_fk': 4},
                {'manufacturer_name': 'NotPNG', 'manufacturer_fk': 5}]
        scene_tool_box.all_products = MagicMock(return_value=pd.DataFrame(data))
        result = scene_tool_box.get_png_manufacturer_fk()
        self.assertIsInstance(result, str)
        # self.all_products[self.all_products['manufacturer_name'].str.encode("utf8") == PNG_MANUFACTURER]
        # ['manufacturer_fk'].values[0]



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




