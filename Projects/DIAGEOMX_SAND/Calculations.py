import os
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
# from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
# from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
# from Trax.Utils.Conf.Configuration import Config
from Projects.DIAGEOMX_SAND.KPIGenerator import DIAGEOMX_SANDGenerator
from KPIUtils.GlobalProjects.DIAGEO.KPIGenerator import DIAGEOGenerator
from KPIUtils.DB.Common import Common

__author__ = 'Nimrod'


class DIAGEOMX_SANDCalculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        template_path = os.path.join(os.path.dirname(os.path.dirname(os.path.realpath(__file__))), 'DIAGEOMX_SAND', 'Data',
                                     'TOUCH POINT.xlsx')

        common = Common(self.data_provider)
        diageo_generator = DIAGEOGenerator(self.data_provider, self.output, common)
        diageo_generator.diageo_global_assortment_function()
        diageo_generator.diageo_global_share_of_shelf_function()
        diageo_generator.diageo_global_touch_point_function(template_path)
        common.commit_results_data_to_new_tables()
        common.commit_results_data()  # old tables
        DIAGEOMX_SANDGenerator(self.data_provider, self.output).main_function()

        self.timer.stop('KPIGenerator.run_project_calculations')


# if __name__ == '__main__':
#     LoggerInitializer.init('diageomx sand calculations')
#     Config.init()
#     project_name = 'diageomx-sand'
#     data_provider = KEngineDataProvider(project_name)
#     session = '314f03d1-9150-400a-b0d5-760046b3206a' # d0924ace-7589-43e2-b666-1249bf4eb664, 314f03d1-9150-400a-b0d5-760046b3206a, 9e558522-3149-4f30-92da-20021c574610 , 7f8f5642-80b1-4181-9dea-db06cf9805a5, 524397a0-b88b-42b4-a7c4-44cc50307fa2
#     data_provider.load_session_data(session)
#     output = Output()
#     DIAGEOMX_SANDCalculations(data_provider, output).run_project_calculations()
