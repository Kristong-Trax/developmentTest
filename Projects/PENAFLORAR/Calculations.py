
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
# from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
# from Trax.Utils.Conf.Configuration import Config
# from Trax.Cloud.Services.Connector.Logger import LoggerInitializer

from KPIUtils.GlobalProjects.DIAGEO.KPIGenerator import DIAGEOGenerator
from Projects.PENAFLORAR.KPIGenerator import PENAFLORARDIAGEOARGenerator
from KPIUtils.DB.Common import Common

__author__ = 'Yasmin'


class PENAFLORARDIAGEOARCalculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        common = Common(self.data_provider)
        PENAFLORARDIAGEOARGenerator(self.data_provider, self.output).main_function()
        DIAGEOGenerator(self.data_provider, self.output, common).diageo_global_assortment_function()
        common.commit_results_data_to_new_tables()
        self.timer.stop('KPIGenerator.run_project_calculations')


# if __name__ == '__main__':
#     LoggerInitializer.init('diageoar-sand calculations')
#     Config.init()
#     project_name = 'penaflorar-sand'
#     data_provider = KEngineDataProvider(project_name)
#     session = '30bd8dec-78c0-43cf-8028-5587f0819e19'
#     data_provider.load_session_data(session)
#     output = Output()
#     PENAFLORARDIAGEOARCalculations(data_provider, output).run_project_calculations()
