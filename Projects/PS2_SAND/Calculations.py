from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
from Projects.PS2_SAND.KPIGenerator import PS2SandGenerator
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Trax.Utils.Conf.Configuration import Config


class PS2SandCalculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        PS2SandGenerator(self.data_provider, self.output).main_function()
        self.timer.stop('KPIGenerator.run_project_calculations')


if __name__ == '__main__':
    LoggerInitializer.init('PS2Sand calculations')
    Config.init()
    project_name = 'ps2-sand'
    data_provider = KEngineDataProvider(project_name)
    session = 'C2D8A9DC-C94D-4C24-B10A-36F3BC61E6FB'
    data_provider.load_session_data(session)
    output = Output()
    PS2SandCalculations(data_provider, output).run_project_calculations()
