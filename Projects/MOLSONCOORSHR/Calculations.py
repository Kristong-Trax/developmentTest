
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Utils.Logging.Logger import Log
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer

from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
from Projects.MOLSONCOORSHR.KPIGenerator import MOLSONCOORSHRGenerator

__author__ = 'sergey'


class MOLSONCOORSHRCalculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        MOLSONCOORSHRGenerator(self.data_provider, self.output).main_function()
        self.timer.stop('KPIMOLSONCOORSHRGenerator.run_project_calculations')


if __name__ == '__main__':
    LoggerInitializer.init('molsoncoorshr calculations')
    Config.init()
    project_name = 'molsoncoorshr'
    data_provider = KEngineDataProvider(project_name)
    sessions = [
                'f0ad8421-3be7-4c29-88ce-58a832ce18bd',
    ]
    for session in sessions:
        data_provider.load_session_data(session)
        output = Output()
        MOLSONCOORSHRCalculations(data_provider, output).run_project_calculations()



