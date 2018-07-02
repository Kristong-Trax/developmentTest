from Trax.Apps.Services.KEngine.Handlerpackage.Scripts import SceneBaseClass
# from Projects.CCIT_SAND.KPIGenerator import Generator
from Projects.CCIT_SAND.KPISceneGenerator import SceneGenerator


class SceneCalculations(SceneBaseClass):
    def __init__(self, data_provider):
        super(SceneCalculations, self).__init__(data_provider)
        self.scene_generator = SceneGenerator(self._data_provider)
        # self._monitor = None
        # self.timer = self._monitor.Timer('Perform', 'Init Session')

    def calculate_kpis(self):
        # self.timer.start()
        self.scene_generator.scene_score()
        # self.timer.stop('KPIGenerator.run_project_calculations')
