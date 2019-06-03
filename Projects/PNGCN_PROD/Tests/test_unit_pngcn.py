from Trax.Utils.Testing.Case import MockingTestCase
from mock import MagicMock
from Projects.PNGCN_PROD.SceneKpis.KPISceneToolBox import PngcnSceneKpis

__author__ = 'avrahama'

class Test_PNGCN(MockingTestCase):

    @property
    def import_path(self):
        return 'Projects.PNGCN_PROD.SceneKpis.KPISceneToolBox.PngcnSceneKpis'

    def set_up(self):
        super(Test_PNGCN, self).set_up()

        # mock PSProjectConnector
        self.ProjectConnector_mock = self.mock_object('ProjectConnector', path='KPIUtils_v2.DB.PsProjectConnector')

        # mock 'Common' object used in toolbox
        self.common_mock = self.mock_object('Common.get_kpi_fk_by_kpi_name', path='KPIUtils_v2.DB.CommonV2')
        self.common_mock.return_value = 3

        # mock 'data provider' object giving to the toolbox
        self.data_provider_mock = MagicMock()


    def test_calculate_presize_linear_length(self):
        """
        1. test if the numerator is greater then denominator (if the subgroup is greater then containing group)
        :return:
        """


        # mock: project_connector, common, scene_id
        scene_tool_box = PngcnSceneKpis(self.ProjectConnector_mock, self.common_mock, 132487, self.data_provider_mock)

        # mock:
        # kpi_fk = self.common.get_kpi_fk_by_kpi_name(PRESIZE_LINEAR_LENGTH_PER_LENGTH)
        # numerator = self.scif.width_mm.sum()  # get the width of P&G products in scene
        # denominator = self.matches_from_data_provider.width_mm.sum()  # get the width of all products in scene
        # if denominator:
        #     score = numerator / denominator  # get the percentage of P&G products from all products
        #     self.common.write_to_db_result
        a = scene_tool_box.common.get_kpi_fk_by_kpi_name
        print '*********************************************\n',a # = self.mock_object('Common', path='KPIUtils_v2.DB.CommonV2')
        denominator, numerator = 3, 2

        self.assertGreaterEqual(denominator, numerator, 'numerator cant be greater then denominator')




        print type(scene_tool_box)

