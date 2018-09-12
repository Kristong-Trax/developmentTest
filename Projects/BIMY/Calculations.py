
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
# from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
# from Trax.Utils.Conf.Configuration import Config
# from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
import os
from KPIUtils.GlobalProjects.SANOFI.KPIGenerator import SANOFIGenerator


__author__ = 'Shani'


class BIMYCalculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        TEMPLATE_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.realpath(__file__))), 'BIMY', 'Data', 'Template_11_Sep_2018.xlsx')
        SANOFIGenerator(self.data_provider, self.output, TEMPLATE_PATH).main_function()
        self.timer.stop('KPIGenerator.run_project_calculations')


# if __name__ == '__main__':
#     LoggerInitializer.init('bimy calculations')
#     Config.init()
#     project_name = 'bimy'
#     data_provider = KEngineDataProvider(project_name)
#     session = '41a7a15e-e4c2-4ba6-b7ee-53593d9a68d9'
#     data_provider.load_session_data(session)
#     output = Output()
#     BIMYCalculations(data_provider, output).run_project_calculations()
