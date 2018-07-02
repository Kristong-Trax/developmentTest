
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
# from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
# from Trax.Utils.Conf.Configuration import Config
# from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
# from Trax.Utils.Logging.Logger import Log
import os
from KPIUtils.GlobalProjects.SANOFI.KPIGenerator import SANOFIGenerator


__author__ = 'Shani'


class SANOFIRUCalculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        TEMPLATE_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.realpath(__file__))), 'SANOFIRU', 'Data', 'Template.xlsx')
        SANOFIGenerator(self.data_provider, self.output, TEMPLATE_PATH).main_function()
        self.timer.stop('KPIGenerator.run_project_calculations')


# if __name__ == '__main__':
#     Log.init('sanofiru calculations')
#     Config.init()
#     project_name = 'sanofiru'
#     data_provider = KEngineDataProvider(project_name)
#     session = '7763E8B8-AEC9-404F-B8BB-FA0563DD1A11'
#     data_provider.load_session_data(session)
#     output = Output()
#     SANOFIRUCalculations(data_provider, output).run_project_calculations()
