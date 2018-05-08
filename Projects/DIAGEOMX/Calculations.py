
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
# from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
# from Trax.Utils.Conf.Configuration import Config
# from Trax.Cloud.Services.Connector.Logger import LoggerInitializer

from Projects.DIAGEOMX.KPIGenerator import DIAGEOMXGenerator
from KPIUtils.GlobalProjects.DIAGEO.KPIGenerator import DIAGEOGenerator
from KPIUtils.DB.Common import Common

__author__ = 'Nimrod'


class DIAGEOMXCalculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        DIAGEOMXGenerator(self.data_provider, self.output).main_function()
        common = Common(self.data_provider)
        diageo_generator = DIAGEOGenerator(self.data_provider, self.output, common)
        diageo_generator.diageo_global_assortment_function()
        diageo_generator.diageo_global_share_of_shelf_function()
        common.commit_results_data_to_new_tables()
        self.timer.stop('KPIGenerator.run_project_calculations')


# if __name__ == '__main__':
#     LoggerInitializer.init('diageomx calculations')
#     Config.init()
#     project_name = 'diageomx'
#     data_provider = KEngineDataProvider(project_name)
#     session = '2af5397c-4cf8-46b8-bd3e-739c40dcefb0'
#     data_provider.load_session_data(session)
#     output = Output()
#     DIAGEOMXCalculations(data_provider, output).run_project_calculations()
