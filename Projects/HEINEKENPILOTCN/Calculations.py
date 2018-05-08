
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
# from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
# from Trax.Utils.Conf.Configuration import Config
# from Trax.Cloud.Services.Connector.Logger import LoggerInitializer


from Projects.HEINEKENPILOTCN.KPIGenerator import HEINEKENPILOTCNGenerator

__author__ = 'Yasmin'


class HEINEKENPILOTCNCalculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        HEINEKENPILOTCNGenerator(self.data_provider, self.output).main_function()
        self.timer.stop('KPIGenerator.run_project_calculations')


# if __name__ == '__main__':
#     LoggerInitializer.init('heinekencn-sand calculations')
#     Config.init()
#     project_name = 'heinekenpilotcn'
#     data_provider = KEngineDataProvider(project_name)
#     session = '6582CF28-4244-4FCF-8BCD-37C4521CD7A5'
#     data_provider.load_session_data(session)
#     output = Output()
#     HEINEKENPILOTCNCalculations(data_provider, output).run_project_calculations()
