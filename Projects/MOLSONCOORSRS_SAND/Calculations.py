
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Utils.Logging.Logger import Log
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer

from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
from Projects.MOLSONCOORSRS_SAND.KPIGenerator import MOLSONCOORSRS_SANDGenerator

__author__ = 'sergey'


class MOLSONCOORSRS_SANDCalculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        MOLSONCOORSRS_SANDGenerator(self.data_provider, self.output).main_function()
        self.timer.stop('KPIMOLSONCOORSRS_SANDGenerator.run_project_calculations')


if __name__ == '__main__':
    LoggerInitializer.init('molsoncoorsrs calculations')
    Config.init()
    project_name = 'molsoncoorsrs-sand'
    data_provider = KEngineDataProvider(project_name)
    sessions = [
                'd95208d8-080e-4f85-a8c4-221b321f79ba',
                '00aed1c0-5d62-4133-b874-c58aa6ee9ed8',
    ]
    for session in sessions:
        data_provider.load_session_data(session)
        output = Output()
        MOLSONCOORSRS_SANDCalculations(data_provider, output).run_project_calculations()



