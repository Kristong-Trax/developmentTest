
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
import os
from KPIUtils.GlobalProjects.SANOFI.KPIGenerator import SANOFIGenerator


__author__ = 'Shani'


class SANOFIUACalculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        template_path = os.path.join(os.path.dirname(os.path.dirname(os.path.realpath(__file__))), 'SANOFIUA', 'Data', 'Template.xlsx')
        # TEMPLATE_PATH2 = TEMPLATE_PATH.replace("/Template.xlsx", "/template_jan.xlsx")
        # SANOFIGenerator(self.data_provider, self.output, TEMPLATE_PATH, template=TEMPLATE_PATH2).main_function()
        SANOFIGenerator(self.data_provider, self.output, template_path).main_function()

        self.timer.stop('KPIGenerator.run_project_calculations')


if __name__ == '__main__':
    LoggerInitializer.init('sanofiua calculations')
    Config.init()
    project_name = 'sanofiua'
    data_provider = KEngineDataProvider(project_name)
    session = '1624816B-1195-4A15-8D40-F1CE889AF9F5'
    data_provider.load_session_data(session)
    output = Output()
    SANOFIUACalculations(data_provider, output).run_project_calculations()
