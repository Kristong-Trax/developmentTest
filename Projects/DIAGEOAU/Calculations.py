
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
# from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
# from Trax.Utils.Conf.Configuration import Config
# from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from KPIUtils.GlobalProjects.DIAGEO.KPIGenerator import DIAGEOGenerator
from KPIUtils.DB.Common import Common
from Projects.DIAGEOAU.KPIGenerator import DIAGEOAUGenerator

__author__ = 'Nimrod'


class DIAGEOAUCalculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        common = Common(self.data_provider)
        DIAGEOAUGenerator(self.data_provider, self.output).main_function()
        DIAGEOGenerator(self.data_provider, self.output, common).diageo_global_assortment_function()
        common.commit_results_data_to_new_tables()
        self.timer.stop('KPIGenerator.run_project_calculations')


# if __name__ == '__main__':
#     LoggerInitializer.init('diageoau calculations')
#     Config.init()
#     project_name = 'diageoau'
#     data_provider = KEngineDataProvider(project_name)
#     session = 'B24409C4-891D-458B-BDB1-6D11D8B05C2D'
#     data_provider.load_session_data(session)
#     output = Output()
#     DIAGEOAUCalculations(data_provider, output).run_project_calculations()
