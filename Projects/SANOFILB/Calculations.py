
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
import os
from KPIUtils.GlobalProjects.SANOFI.KPIGenerator import SANOFIGenerator


__author__ = 'Shani'


class SANOFILBCalculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        TEMPLATE_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.realpath(__file__))), 'SANOFILB', 'Data', 'Template.xlsx')
        TEMPLATE_PATH2 = TEMPLATE_PATH.replace("/Template.xlsx", "/template_jan.xlsx")
        SANOFIGenerator(self.data_provider, self.output, TEMPLATE_PATH).main_function()
        self.timer.stop('KPIGenerator.run_project_calculations')


# if __name__ == '__main__':
#     LoggerInitializer.init('sanofilb calculations')
#     Config.init()
#     project_name = 'sanofilb'
#     data_provider = KEngineDataProvider(project_name)
#     session = '0E734E98-DF10-43EB-B00E-F29E8A601B35'
#     data_provider.load_session_data(session)
#     output = Output()
#     SANOFILBCalculations(data_provider, output).run_project_calculations()
