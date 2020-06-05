
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
import os
from KPIUtils.GlobalProjects.SANOFI.KPIGenerator import SANOFIGenerator


__author__ = 'Shani'


class SANOFICMCalculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        TEMPLATE_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.realpath(__file__))), 'SANOFICM', 'Data', 'Template.xlsx')
        TEMPLATE_PATH2 = TEMPLATE_PATH.replace("/Template.xlsx", "/template_jan.xlsx")
        SANOFIGenerator(self.data_provider, self.output, TEMPLATE_PATH).main_function()
        self.timer.stop('KPIGenerator.run_project_calculations')

# from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
# from Trax.Utils.Conf.Configuration import Config
# from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
# if __name__ == '__main__':
#     LoggerInitializer.init('sanoficm calculations')
#     Config.init()
#     project_name = 'sanoficm'
#     data_provider = KEngineDataProvider(project_name)
#     session = '306F715C-E37F-4144-AC0B-97A9DC564B04'
#     data_provider.load_session_data(session)
#     output = Output()
#     SANOFIAUCalculations(data_provider, output).run_project_calculations()
