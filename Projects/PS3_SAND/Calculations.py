
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
from Projects.PS3_SAND.KPIGenerator import PS3SandGenerator
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer


class PS3SandCalculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        PS3SandGenerator(self.data_provider, self.output).main_function()
        self.timer.stop('KPIGenerator.run_project_calculations')

if __name__ == '__main__':
   LoggerInitializer.init('ps3-sand calculations')
   Config.init()
   project_name = 'ps3-sand'
   data_provider = KEngineDataProvider(project_name)
   session = '17a54de5-4247-4bc8-8136-0ef084d5055d'
   data_provider.load_session_data(session)
   output = Output()
   PS3SandCalculations(data_provider, output).run_project_calculations()
