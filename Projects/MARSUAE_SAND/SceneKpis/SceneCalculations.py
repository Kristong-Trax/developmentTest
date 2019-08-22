
from Trax.Apps.Services.KEngine.Handlers.Utils.Scripts import SceneBaseClass
from Projects.MARSUAE_SAND.KPISceneGenerator import SceneGenerator

__author__ = 'natalyak'


class MARSUAE_SANDSceneCalculations(SceneBaseClass):
    def __init__(self, data_provider):
        super(MARSUAE_SANDSceneCalculations, self).__init__(data_provider)
        self.scene_generator = SceneGenerator(self._data_provider)

    def calculate_kpis(self):
        self.scene_generator.scene_score()

