from Trax.Apps.Services.KEngine.Handlers.Utils.Scripts import SceneBaseClass
from Projects.CCIT_SAND.KPIGenerator import Generator


class SceneCalculations(SceneBaseClass):
    def __init__(self, data_provider):
        super(SceneCalculations, self).__init__(data_provider)
        # self._monitor = None
        # self.timer = self._monitor.Timer('Perform', 'Init Session')

    def calculate_kpis(self):
        # self.timer.start()
        Generator(self._data_provider).occupancy_calculation()
        # self.timer.stop('KPIGenerator.run_project_calculations')
