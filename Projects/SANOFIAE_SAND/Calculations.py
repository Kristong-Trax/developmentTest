
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
# from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
# from Trax.Utils.Conf.Configuration import Config
# from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
import os
from KPIUtils.GlobalProjects.SANOFI_3.KPIGenerator import SANOFIGenerator


__author__ = 'Shani'


class SANOFIAE_SANDCalculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        TEMPLATE_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.realpath(__file__))), 'SANOFIAE_SAND', 'Data', 'Template.xlsx')
        SANOFIGenerator(self.data_provider, self.output, TEMPLATE_PATH).main_function()
        self.timer.stop('KPIGenerator.run_project_calculations')


# if __name__ == '__main__':
#     LoggerInitializer.init('sanofiae-sand calculations')
#     Config.init()
#     project_name = 'sanofiae-sand'
#     data_provider = KEngineDataProvider(project_name)
#     sessions = ['67103399-80CD-429C-A085-2BA7ED6320C2', 'C2D64D76-08D0-4573-8B1E-46DD4A3397E2', 'a633f183-ac3c-4703-ad9a-3db3237c8605', '5E1AADB2-5DFE-4EC5-89EF-F2986ED1EF07', '2BA885DE-A45B-4F70-9BEF-6A3B195797DD']
#     for session in sessions:
#         data_provider.load_session_data(session)
#         output = Output()
#         SANOFIAE_SANDCalculations(data_provider, output).run_project_calculations()
