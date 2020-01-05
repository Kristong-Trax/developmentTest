
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
import os
from KPIUtils.GlobalProjects.SANOFI_3.KPIGenerator import SANOFIGenerator


__author__ = 'limorc'


class SANOFICNCalculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        TEMPLATE_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.realpath(__file__))), 'SANOFICN', 'Data', 'Test_Template.xlsx')
        # TEMPLATE_PATH2 = TEMPLATE_PATH.replace("/Template.xlsx", "/template_jan.xlsx")
        SANOFIGenerator(self.data_provider, self.output, TEMPLATE_PATH).main_function()
        self.timer.stop('KPIGenerator.run_project_calculations')


from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
if __name__ == '__main__':
    LoggerInitializer.init('sanoficn calculations')
    Config.init()
    project_name = 'sanoficn'
    data_provider = KEngineDataProvider(project_name)
    session = "A5B576AC-3DEC-4F7A-8217-30D3D4473BC5"
    data_provider.load_session_data(session)
    output = Output()
    SANOFICNCalculations(data_provider, output).run_project_calculations()
