from Projects.GOOGLEJP_SAND.SceneKPIGenerator import SceneGOOGLEJP_SANDGenerator
from Trax.Apps.Services.KEngine.Handlers.Utils.Scripts import SceneBaseClass

__author__ = 'Eli_Shivi_Sam'


class SceneCalculations(SceneBaseClass):
    def __init__(self, data_provider):
        super(SceneCalculations, self).__init__(data_provider)
        self.scene_generator = SceneGOOGLEJP_SANDGenerator(self._data_provider)

    def calculate_kpis(self):
        self.scene_generator.main_function()
