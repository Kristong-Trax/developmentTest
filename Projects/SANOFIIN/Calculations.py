
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
import os
from KPIUtils.GlobalProjects.SANOFI_2.KPIGenerator import SANOFIGenerator


__author__ = 'Shani'


class SANOFIINCalculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        TEMPLATE_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.realpath(__file__))), 'SANOFIIN', 'Data', 'Template.xlsx')
        SANOFIGenerator(self.data_provider, self.output, TEMPLATE_PATH).main_function()
        self.timer.stop('KPIGenerator.run_project_calculations')

# from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
# from Trax.Utils.Conf.Configuration import Config
# from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
#
# if __name__ == '__main__':
#     LoggerInitializer.init('sanofiin calculations')
#     Config.init()
#     project_name = 'sanofiin'
#     data_provider = KEngineDataProvider(project_name)
#     session = 'b85d1929-ceb1-4460-b865-3fa8684a64db'
#     data_provider.load_session_data(session)
#     output = Output()
#     SANOFIINCalculations(data_provider, output).run_project_calculations()
