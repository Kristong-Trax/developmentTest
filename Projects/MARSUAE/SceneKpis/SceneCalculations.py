
from Trax.Apps.Services.KEngine.Handlers.Utils.Scripts import SceneBaseClass
from Projects.MARSUAE.KPISceneGenerator import SceneGenerator

__author__ = 'natalyak'


class MARSUAESceneCalculations(SceneBaseClass):
    def __init__(self, data_provider):
        super(MARSUAESceneCalculations, self).__init__(data_provider)
        self.scene_generator = SceneGenerator(self._data_provider)

    def calculate_kpis(self):
        self.scene_generator.scene_score()

