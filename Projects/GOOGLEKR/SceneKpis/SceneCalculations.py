from Projects.GOOGLEKR.SceneKPIGenerator import GOOGLEKRSceneGenerator
from Trax.Apps.Services.KEngine.Handlers.Utils.Scripts import SceneBaseClass

__author__ = 'Eli_Shivi_Sam'


class GOOGLEKRSceneCalculations(SceneBaseClass):
    def __init__(self, data_provider):
        super(GOOGLEKRSceneCalculations, self).__init__(data_provider)
        self.scene_generator = GOOGLEKRSceneGenerator(self._data_provider)

    def calculate_kpis(self):
        self.scene_generator.main_function()
