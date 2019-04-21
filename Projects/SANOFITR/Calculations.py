
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
# from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
# from Trax.Utils.Conf.Configuration import Config
# from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
import os
from KPIUtils.GlobalProjects.SANOFI_2.KPIGenerator import SANOFIGenerator


__author__ = 'Shani'


class SANOFITRCalculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        TEMPLATE_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.realpath(__file__))), 'SANOFITR', 'Data', 'Template.xlsx')
        TEMPLATE_PATH2 = TEMPLATE_PATH.replace("/Template.xlsx", "/template_jan.xlsx")
        SANOFIGenerator(self.data_provider, self.output, TEMPLATE_PATH).main_function()
        self.timer.stop('KPIGenerator.run_project_calculations')


# if __name__ == '__main__':
#     LoggerInitializer.init('sanofitr calculations')
#     Config.init()
#     project_name = 'sanofitr'
#     data_provider = KEngineDataProvider(project_name)
#     # session = '7F2F4C6A-E4E7-4F03-B7A3-A875930FD893'  # error with pandas - not competed
#     # session = '15B92241-3E84-4D6C-81C9-4A9385435867' # no error
#     session = 'F0889AAE-2A4A-4957-9885-5A9FF6D0E5C3'  # no error
#     data_provider.load_session_data(session)
#     output = Output()
#     SANOFITRCalculations(data_provider, output).run_project_calculations()
