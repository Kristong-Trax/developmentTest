
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
from KPIUtils.GlobalProjects.SANOFI_2.KPIGenerator import SANOFIGenerator
import os


__author__ = 'Shani'


class PS3SandCalculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        template_path = os.path.join(os.path.dirname(os.path.dirname(os.path.realpath(__file__))), 'SANOFIEG',
                                     'Data', 'Template.xlsx')
        SANOFIGenerator(self.data_provider, self.output, template_path).main_function()
        self.timer.stop('KPIGenerator.run_project_calculations')


# from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
# from Trax.Utils.Conf.Configuration import Config
# from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
# from Trax.Utils.Logging.Logger import Log
# if __name__ == '__main__':
#     LoggerInitializer.init('ps2-sand calculations')
#     Config.init()
#     project_name = 'ps3-sand'
#     data_provider = KEngineDataProvider(project_name)
#     session = '215c5f45-41e9-4fd9-af70-3ec45cfe3608'
#     data_provider.load_session_data(session)
#     output = Output()
#     PS3SandCalculations(data_provider, output).run_project_calculations()
