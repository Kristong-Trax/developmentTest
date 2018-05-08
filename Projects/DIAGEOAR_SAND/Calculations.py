
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
# from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
# from Trax.Utils.Conf.Configuration import Config
# from Trax.Cloud.Services.Connector.Logger import LoggerInitializer

from Projects.DIAGEOAR_SAND.KPIGenerator import DIAGEOARGenerator
from KPIUtils.GlobalProjects.DIAGEO.KPIGenerator import DIAGEOGenerator
from KPIUtils.DB.Common import Common

__author__ = 'Yasmin'


class DIAGEOARCalculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        common = Common(self.data_provider)
        DIAGEOARGenerator(self.data_provider, self.output).main_function()
        DIAGEOGenerator(self.data_provider, self.output, common).diageo_global_assortment_function()
        common.commit_results_data_to_new_tables()
        self.timer.stop('KPIGenerator.run_project_calculations')


# if __name__ == '__main__':
#     LoggerInitializer.init('diageoar-sand calculations')
#     Config.init()
#     project_name = 'diageoar-sand'
#     data_provider = KEngineDataProvider(project_name)
#     session = ''
#     data_provider.load_session_data(session)
#     output = Output()
#     DIAGEOARCalculations(data_provider, output).run_project_calculations()
