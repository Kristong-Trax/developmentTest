import os
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
# from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
# from Trax.Utils.Conf.Configuration import Config
# from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from KPIUtils.DB.Common import Common
from Projects.DIAGEOIE_SAND.KPIGenerator import DIAGEOIE_SANDGenerator
from KPIUtils.GlobalProjects.DIAGEO.KPIGenerator import DIAGEOGenerator

__author__ = 'Yasmin'

BAT_TYPE_WEIGHT = {'bar primary': 1.1, 'bar secondary': 0.8,
                   'mega bar primary':1.9, 'mega bar secondary': 0.9,
                   }

class DIAGEOIECalculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        template_path = os.path.join(os.path.dirname(os.path.dirname(os.path.realpath(__file__))), 'DIAGEOIE_SAND',
                                     'Data', 'Brand Score.xlsx')
        DIAGEOIE_SANDGenerator(self.data_provider, self.output).main_function()
        common = Common(self.data_provider)
        diageo_generator = DIAGEOGenerator(self.data_provider, self.output, common)
        diageo_generator.diageo_global_tap_brand_score_function(template_path)
        common.commit_results_data_to_new_tables()
        DIAGEOGenerator(self.data_provider, self.output, common).diageo_global_assortment_function()
        common.commit_results_data_to_new_tables()
        self.timer.stop('KPIGenerator.run_project_calculations')


# if __name__ == '__main__':
#     LoggerInitializer.init('diageoie-sand calculations')
#     Config.init()
#     project_name = 'diageoie-sand'
#     data_provider = KEngineDataProvider(project_name)
#     session = '2c965f54-e29e-4319-a84b-bf1783fdd7f4'
#     data_provider.load_session_data(session)
#     output = Output()
#     DIAGEOIECalculations(data_provider, output).run_project_calculations()
