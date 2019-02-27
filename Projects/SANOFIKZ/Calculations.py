
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
# from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
# from Trax.Utils.Conf.Configuration import Config
# from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
import os
from KPIUtils.GlobalProjects.SANOFI.KPIGenerator import SANOFIGenerator


__author__ = 'Shani'


class SANOFIKZCalculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        TEMPLATE_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.realpath(__file__))), 'SANOFIKZ', 'Data', 'Template.xlsx')
        TEMPLATE_PATH2 = TEMPLATE_PATH.replace("/Template.xlsx", "/template_jan.xlsx")
        SANOFIGenerator(self.data_provider, self.output, TEMPLATE_PATH, template2=TEMPLATE_PATH2).main_function()
        self.timer.stop('KPIGenerator.run_project_calculations')

#
# if __name__ == '__main__':
#     LoggerInitializer.init('sanofikz calculations')
#     Config.init()
#     project_name = 'sanofikz'
#     data_provider = KEngineDataProvider(project_name)
#     session = 'FFD0945A-789A-4998-B6DD-FAC0C5BB920F'
#     data_provider.load_session_data(session)
#     output = Output()
#     SANOFIKZCalculations(data_provider, output).run_project_calculations()
