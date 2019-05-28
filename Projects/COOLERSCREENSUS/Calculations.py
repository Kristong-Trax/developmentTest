from KPIUtils.DB.Common import Common
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Trax.Utils.Conf.Configuration import Config

from Projects.COOLERSCREENSUS.Utils.KPIGenerator import COOLERSCREENSUSKGenerator


class COOLERSCREENSUSCalculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        common = Common(self.data_provider)
        COOLERSCREENSUSKGenerator(self.data_provider, self.output, common).main_function()
        self.timer.stop('KPIGenerator.run_project_calculations')

#
# if __name__ == '__main__':
#     LoggerInitializer.init('coolerscreensus calculations')
#     Config.init()
#     project_name = 'coolerscreensus'
#     data_provider = KEngineDataProvider(project_name)
#     # session = 'efdd2028-6f09-46ff-ad02-18874a6f45b2'
#     sessions = ['237a5761-f6d6-4b22-a2ce-9e839df1090b']
#     for session in sessions:
#         data_provider.load_session_data(session)
#         output = Output()
#         COOLERSCREENSUSCalculations(data_provider, output).run_project_calculations()
#
