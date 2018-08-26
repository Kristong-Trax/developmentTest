from Projects.CCBZA_SAND.KPIGenerator import CCBZA_SANDGenerator
from Trax.Apps.Services.KEngine.Handlers.Utils.Scripts import SceneBaseClass

class CCBZA_SANDSceneCalculations(SceneBaseClass):
    def __init__(self, data_provider):
        super(CCBZA_SANDSceneCalculations, self).__init__(data_provider)
        self.scene_generator = CCBZA_SANDGenerator(self._data_provider)

    def calculate_kpis(self):
        # self.timer.start()
        self.scene_generator.main_scene_function()
        # self.timer.stop('KPIGenerator.run_project_calculations')