from Trax.Utils.Testing.Case import MockingTestCase
# from Projects.PNGCN_PROD.SceneKpis.KPISceneToolBox import PngcnSceneKpis

__author__ = 'avrahama'

class Test_PNGCN(MockingTestCase):

    @property
    def import_path(self):
        return 'Projects.PNGCN_PROD.SceneKpis.KPISceneToolBox.PngcnSceneKpis'

    def set_up(self):
        super(Test_PNGCN, self).set_up()

    def test_calculate_presize_linear_length(self):

        pass