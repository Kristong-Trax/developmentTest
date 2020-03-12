
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
from Projects.GSKAU.KPIGenerator import Generator
from Trax.Utils.Logging.Logger import Log
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
__author__ = 'limorc'
class Calculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        Generator(self.data_provider, self.output).main_function()
        self.timer.stop('KPIGenerator.run_project_calculations')
if __name__ == '__main__':
    LoggerInitializer.init('gskau calculations')
    Config.init()
    project_name = 'gskau'
    sessions = ['bfdd5adc-55d0-4822-84ba-558efc9b3903']
    data_provider = KEngineDataProvider(project_name)
    # RUN FOR Session level KPIs
    for sess in sessions:
        Log.info("[Session level] Running for session: {}".format(sess))
        data_provider.load_session_data(sess)
        output = Output()
        Calculations(data_provider, output).run_project_calculations()


