
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Projects.DIAGEOAR_SAND.KPIGenerator import DIAGEOARGenerator


__author__ = 'Yasmin'


class DIAGEOARCalculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        DIAGEOARGenerator(self.data_provider, self.output).main_function()
        self.timer.stop('KPIGenerator.run_project_calculations')


# if __name__ == '__main__':
#     LoggerInitializer.init('diageoar-sand calculations')
#     Config.init()
#     project_name = 'diageoar-sand'
#     data_provider = KEngineDataProvider(project_name)
#     session = 'ca8a3f2e-693a-11e8-b53f-127c7ae559da'
#     data_provider.load_session_data(session)
#     output = Output()
#     DIAGEOARCalculations(data_provider, output).run_project_calculations()
