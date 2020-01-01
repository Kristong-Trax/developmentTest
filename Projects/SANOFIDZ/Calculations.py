
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
# from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
# from Trax.Utils.Conf.Configuration import Config
# from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
import os
from KPIUtils.GlobalProjects.SANOFI_3.KPIGenerator import SANOFIGenerator


__author__ = 'Shani'


class SANOFIAUCalculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        TEMPLATE_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.realpath(__file__))), 'SANOFIDZ', 'Data', 'Template.xlsx')
        SANOFIGenerator(self.data_provider, self.output, TEMPLATE_PATH).main_function()
        self.timer.stop('KPIGenerator.run_project_calculations')


# if __name__ == '__main__':
#     LoggerInitializer.init('sanofiau calculations')
#     Config.init()
#     project_name = 'sanofidz'
#     data_provider = KEngineDataProvider(project_name)
#     session = '3D081D8D-D6EF-4944-8926-9BC07D2DB22C'
#     data_provider.load_session_data(session)
#     output = Output()
#     SANOFIAUCalculations(data_provider, output).run_project_calculations()
