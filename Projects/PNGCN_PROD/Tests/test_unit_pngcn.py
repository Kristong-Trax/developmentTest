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
        self.common_mock = self.mock_object('Common', path='KPIUtils_v2.DB.CommonV2')

        # mock 'data provider' object giving to the toolbox
        self.data_provider_mock = MagicMock()


    def test_calculate_presize_linear_length(self):
        # mock: project_connector, common, scene_id
        scene_tool_box = PngcnSceneKpis(self.ProjectConnector_mock, self.common_mock, 132487, self.data_provider_mock)

        print type(scene_tool_box)

