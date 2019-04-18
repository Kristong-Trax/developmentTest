
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
# from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
# from Trax.Utils.Conf.Configuration import Config
# from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
import os
from KPIUtils.GlobalProjects.SANOFI.KPIGenerator import SANOFIGenerator


__author__ = 'Shani'


class BIKRCalculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        if str(self.data_provider.visit_date) >= '2019-01-01' and str(self.data_provider.visit_date) <= '2019-03-31':
            TEMPLATE_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.realpath(__file__))), 'BIKR', 'Data',
                                         'Template_Q1_2019.xlsx')
        else:
            TEMPLATE_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.realpath(__file__))), 'BIKR', 'Data',
                                         'Template.xlsx')
        SANOFIGenerator(self.data_provider, self.output, TEMPLATE_PATH).main_function()
        self.timer.stop('KPIGenerator.run_project_calculations')


# if __name__ == '__main__':
#     LoggerInitializer.init('bikr calculations')
#     Config.init()
#     project_name = 'bikr'
#     data_provider = KEngineDataProvider(project_name)
#     session = '3D081D8D-D6EF-4944-8926-9BC07D2DB22C'
#     data_provider.load_session_data(session)
#     output = Output()
#     SANOFIAUCalculations(data_provider, output).run_project_calculations()
