from Projects.TSINGTAOBEERCN_SAND.SceneKPIGenerator import SceneGenerator
from Trax.Apps.Services.KEngine.Handlers.Utils.Scripts import SceneBaseClass

__author__ = 'ilays'


class SceneCalculations(SceneBaseClass):
    def __init__(self, data_provider):
        super(SceneCalculations, self).__init__(data_provider)
        self.scene_generator = SceneGenerator(self._data_provider)

    def calculate_kpis(self):
        self.scene_generator.main_function()
