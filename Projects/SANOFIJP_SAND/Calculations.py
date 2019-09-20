
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
# from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
# from Trax.Utils.Conf.Configuration import Config
# from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
import os
from Projects.SANOFIJP_SAND.KPIGenerator import Generator
from KPIUtils.GlobalProjects.SANOFI_2.KPIGenerator import SANOFIGenerator


__author__ = 'nidhinb'


class SANOFIJP_SANDCalculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        TEMPLATE_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.realpath(__file__))), 'SANOFIJP_SAND', 'Data', 'Template.xlsx')
        SANOFIGenerator(self.data_provider, self.output, TEMPLATE_PATH).main_function()
        # For Custom KPI -- PROS-11486
        Generator(self.data_provider, self.output).main_function()
        self.timer.stop('KPIGenerator.run_project_calculations')


# if __name__ == '__main__':
#     LoggerInitializer.init('sanofijp-sand calculations')
#     Config.init()
#     project_name = 'sanofijp-sand'
#     data_provider = KEngineDataProvider(project_name)
#     sessions = [
#         "F2089166-6702-4311-9BAB-A550E48B6681",
#         "F7E70CBC-1BAD-4B20-AFC7-9BD13AD17E5F",
#         "FE163E25-6015-42D1-B458-934D3BDBA064",
#         ]
#     for session in sessions:
#         print "Running for session ", session
#         data_provider.load_session_data(session)
#         output = Output()
#         SANOFIJP_SANDCalculations(data_provider, output).run_project_calculations()
