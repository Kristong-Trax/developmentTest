
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
# from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
# from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
# from Trax.Utils.Conf.Configuration import Configgit


import os
from KPIUtils.GlobalProjects.SANOFI.KPIGenerator import SANOFIGenerator


__author__ = 'Shani'


class BIVNCalculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        TEMPLATE_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.realpath(__file__))), 'BIVN', 'Data', 'Template.xlsx')
        SANOFIGenerator(self.data_provider, self.output, TEMPLATE_PATH).main_function()
        self.timer.stop('KPIGenerator.run_project_calculations')


# if __name__ == '__main__':
#     LoggerInitializer.init('sanofivn calculations')
#     Config.init()
#     project_name = 'bivn'
#     data_provider = KEngineDataProvider(project_name)
#     session = '7ed957ba-0764-4be1-9469-d3fe597a17ca'
#     data_provider.load_session_data(session)
#     output = Output()
#     BIVNCalculations(data_provider, output).run_project_calculations()
