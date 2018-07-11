# from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
# from Trax.Utils.Conf.Configuration import Config
# from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript

from KPIUtils.LAVAZZA.SuccessCriteria import LAVAZZAGSuccessCriteria

__author__ = 'Nimrod'


class LAVAZZACalculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        LAVAZZAGSuccessCriteria(self.data_provider, self.output).main_function()
        self.timer.stop('KPIGenerator.run_project_calculations')


# if __name__ == '__main__':
#     LoggerInitializer.init('lavazza calculations')
#     Config.init()
#     project_name = 'lavazza'
#     data_provider = KEngineDataProvider(project_name)
#     session = ''
#     data_provider.load_session_data(session)
#     output = Output()
#     LAVAZZACalculations(data_provider, output).run_project_calculations()
