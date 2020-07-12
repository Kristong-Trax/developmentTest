from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
from Projects.CBCIL_SAND.KPIGenerator import CBCILSANDGenerator
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Trax.Utils.Conf.Configuration import Config


class PS2SandCalculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        CBCILSANDGenerator(self.data_provider, self.output).main_function()
        self.timer.stop('KPIGenerator.run_project_calculations')


if __name__ == '__main__':
    LoggerInitializer.init('Simon')
    Config.init()
    project_name = 'ps2-sand'
    data_provider = KEngineDataProvider(project_name)
    session = '4673efb5-31b1-436f-8d3b-52071290f091'
    data_provider.load_session_data(session)
    output = Output()
    PS2SandCalculations(data_provider, output).run_project_calculations()
