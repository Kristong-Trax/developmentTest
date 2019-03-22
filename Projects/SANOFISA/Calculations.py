
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
import os
from KPIUtils.GlobalProjects.SANOFI_2.KPIGenerator import SANOFIGenerator


__author__ = 'Shani'


class SANOFISACalculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        TEMPLATE_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.realpath(__file__))), 'SANOFISA', 'Data', 'Template.xlsx')
        SANOFIGenerator(self.data_provider, self.output, TEMPLATE_PATH).main_function()
        self.timer.stop('KPIGenerator.run_project_calculations')


# from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
# from Trax.Utils.Conf.Configuration import Config
# from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
# if __name__ == '__main__':
#     LoggerInitializer.init('sanofisa calculations')
#     Config.init()
#     project_name = 'sanofisa'
#     data_provider = KEngineDataProvider(project_name)
#     session = '69D4BF95-6E2B-453E-AAAA-4E73A8DF8975'
#     data_provider.load_session_data(session)
#     output = Output()
#     SANOFISACalculations(data_provider, output).run_project_calculations()
