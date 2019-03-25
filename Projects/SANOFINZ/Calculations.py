import os

from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
# from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
# from Trax.Utils.Conf.Configuration import Config
# from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from KPIUtils.GlobalProjects.SANOFI_2.KPIGenerator import SANOFIGenerator


__author__ = 'Shani'


class SANOFINZCalculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        template_path = os.path.join(os.path.dirname(os.path.dirname(os.path.realpath(__file__))), 'SANOFINZ', 'Data', 'Template.xlsx')
        SANOFIGenerator(self.data_provider, self.output, template_path).main_function()
        self.timer.stop('KPIGenerator.run_project_calculations')


# if __name__ == '__main__':
#     LoggerInitializer.init('sanofinz calculations')
#     Config.init()
#     project_name = 'sanofinz'
#     data_provider = KEngineDataProvider(project_name)
#     session = '10c91df2-7aee-4d46-a9f8-af70482c2bd7'
#     data_provider.load_session_data(session)
#     output = Output()
#     SANOFINZCalculations(data_provider, output).run_project_calculations()
