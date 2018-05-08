
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
# from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
# from Trax.Utils.Conf.Configuration import Config
# from Trax.Cloud.Services.Connector.Logger import LoggerInitializer

from KPIUtils.GlobalProjects.DIAGEO.KPIGenerator import DIAGEOGenerator
from KPIUtils.DB.Common import Common

__author__ = 'Nimrod'


class DIAGEOMX_SANDCalculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        common = Common(self.data_provider)
        diageo_generator = DIAGEOGenerator(self.data_provider, self.output, common)
        diageo_generator.diageo_global_assortment_function()
        diageo_generator.diageo_global_share_of_shelf_function()
        common.commit_results_data_to_new_tables()
        self.timer.stop('KPIGenerator.run_project_calculations')


# if __name__ == '__main__':
#     LoggerInitializer.init('diageomx calculations')
#     Config.init()
#     project_name = 'diageomx-sand'
#     data_provider = KEngineDataProvider(project_name)
#     session = 'bf9a1932-27c6-43e7-9fad-1568880a0dba'
#     data_provider.load_session_data(session)
#     output = Output()
#     DIAGEOMX_SANDCalculations(data_provider, output).run_project_calculations()
