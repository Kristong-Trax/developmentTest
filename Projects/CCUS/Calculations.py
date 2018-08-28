from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
from Trax.Algo.Calculations.Core.DataProvider import Output, KEngineDataProvider
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from mock import MagicMock
from Projects.CCUS.KPIGenerator import CCUSGenerator
from Projects.CCUS.SceneKpis.SceneCalculations import SceneCalculations

__author__ = 'ortal_shivi'


class CCUSCalculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        CCUSGenerator(self.data_provider, self.output).main_function()
        self.timer.stop('KPIGenerator.run_project_calculations')


if __name__ == '__main__':
    LoggerInitializer.init('ccus calculations')
    Config.init()
    project_name = 'ccus'
    data_provider = KEngineDataProvider(project_name, monitor=MagicMock())
    session = '93527cf7-a84f-4228-80d0-91d7fa13ece8'
    scenes = [63147, 63148]
    for scene in scenes:
        data_provider.load_scene_data(session, scene)
        SceneCalculations(data_provider).calculate_kpis()
    # data_provider.load_session_data(session)
    # output = Output()
    # CCUSCalculations(data_provider, output).run_project_calculations()
