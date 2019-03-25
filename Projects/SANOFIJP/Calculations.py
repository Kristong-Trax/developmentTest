
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
# from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
# from Trax.Utils.Conf.Configuration import Config
# from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
import os
from KPIUtils.GlobalProjects.SANOFI_2.KPIGenerator import SANOFIGenerator


__author__ = 'nidhinb'


class SANOFIJPCalculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        TEMPLATE_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.realpath(__file__))), 'SANOFIJP', 'Data', 'Template.xlsx')
        SANOFIGenerator(self.data_provider, self.output, TEMPLATE_PATH).main_function()
        self.timer.stop('KPIGenerator.run_project_calculations')


# if __name__ == '__main__':
#     LoggerInitializer.init('sanofijp calculations')
#     Config.init()
#     project_name = 'sanofijp'
#     data_provider = KEngineDataProvider(project_name)
#     session = '31eb50cd-e799-4798-a27f-dc5ea557ccc7'
#     data_provider.load_session_data(session)
#     output = Output()
#     SANOFIJPCalculations(data_provider, output).run_project_calculations()
