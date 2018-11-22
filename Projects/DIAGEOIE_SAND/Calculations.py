
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer

from Projects.DIAGEOIE_SAND.KPIGenerator import DIAGEOIESandGenerator

__author__ = 'Yasmin'

BAT_TYPE_WEIGHT = {'bar primary': 1.1, 'bar secondary': 0.8,
                   'mega bar primary':1.9, 'mega bar secondary': 0.9, }


class DIAGEOIECalculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        DIAGEOIESandGenerator(self.data_provider, self.output).main_function()
        # template_path = os.path.join(os.path.dirname(os.path.dirname(os.path.realpath(__file__))), 'DIAGEOIE_SAND',
        #                              'Data', 'Brand Score.xlsx')
        # diageo_generator.diageo_global_tap_brand_score_function(template_path)  # todo --> global migration.
        self.timer.stop('KPIGenerator.run_project_calculations')


# if __name__ == '__main__':
#     LoggerInitializer.init('diageoie-sand calculations')
#     Config.init()
#     project_name = 'diageoie-sand'
#     data_provider = KEngineDataProvider(project_name)
#     session = '0C43A1B6-FE95-4E4D-AAEC-60B04DD08980'
#     data_provider.load_session_data(session)
#     output = Output()
#     DIAGEOIECalculations(data_provider, output).run_project_calculations()
