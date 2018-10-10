
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Utils.Logging.Logger import Log
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer

from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
from Projects.MOLSONCOORSHR.KPIGenerator import Generator

__author__ = 'sergey'


class Calculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        Generator(self.data_provider, self.output).main_function()
        self.timer.stop('KPIGenerator.run_project_calculations')


if __name__ == '__main__':
    LoggerInitializer.init('molsoncoorshr calculations')
    Config.init()
    project_name = 'molsoncoorshr-sand'
    data_provider = KEngineDataProvider(project_name)
    sessions = [
                'F3586DC2-D078-4011-8847-8300FC5EEBE2'
    ]
    for session in sessions:
        data_provider.load_session_data(session)
        output = Output()
        Calculations(data_provider, output).run_project_calculations()



