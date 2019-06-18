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


    def test_calculate_linear_or_presize_linear_length(self):
        """
        1. test if the numerator is greater then denominator (if the subgroup is greater then containing group)
        :return:
        """
        # mock: project_connector, common, scene_id
        scene_tool_box = PngcnSceneKpis(self.ProjectConnector_mock, self.common_mock, 16588190, self.data_provider_mock)
        data = [{'scene_fk': 101,'manufacturer_fk': 2,'product_fk': 252,'width_mm': 0.84 ,'width_mm_advance': 1.23},
                {'scene_fk': 121,'manufacturer_fk': 4,'product_fk': 132,'width_mm': 0.80 ,'width_mm_advance': 0.99},
                {'scene_fk': 201,'manufacturer_fk': 4,'product_fk': 152,'width_mm': 0.28 ,'width_mm_advance': 0.75},
                {'scene_fk': 151,'manufacturer_fk': 5,'product_fk': 172,'width_mm': 0.95 ,'width_mm_advance': 0.15}
        ]
        scene_tool_box.get_filterd_matches = MagicMock(return_value=pd.DataFrame(data))
        scene_tool_box.common.write_to_db_result = MagicMock()
        scene_tool_box.calculate_linear_or_presize_linear_length('width_mm')
        print scene_tool_box.common.write_to_db_result.mock_calls

        # kpi_fk = self.common.get_kpi_fk_by_kpi_name(PRESIZE_LINEAR_LENGTH_PER_LENGTH)
        # numerator = self.scif.width_mm.sum()  # get the width of P&G products in scene
        # denominator = self.matches_from_data_provider.width_mm.sum()  # get the width of all products in scene
        # if denominator:
        #     score = numerator / denominator  # get the percentage of P&G products from all products
        #     self.common.write_to_db_result




        if scene_tool_box.calculate_linear_or_presize_linear_length('width_mm'):
            print scene_tool_box.common.write_to_db_result.call_args_list
        else:
            print 'no display'
        print '*********************************************\n'
        denominator, numerator = 3, 5

        self.assertGreaterEqual(denominator, numerator, 'numerator cant be greater then denominator')




        print type(scene_tool_box)

