from Trax.Apps.Services.KEngine.Handlers.Utils.Scripts import SceneBaseClass
from Projects.CCUS_SAND.KPISceneGenerator import CCUS_SANDSceneGenerator


class CCUS_SANDSceneCalculations(SceneBaseClass):
    def __init__(self, data_provider):
        super(CCUS_SANDSceneCalculations, self).__init__(data_provider)
        self.scene_generator = CCUS_SANDSceneGenerator(self._data_provider)
        # self._monitor = None
        # self.timer = self._monitor.Timer('Perform', 'Init Session')

    def calculate_kpis(self):
        # self.timer.start()
        self.scene_generator.scene_score()
        # self.timer.stop('KPIGenerator.run_project_calculations')
