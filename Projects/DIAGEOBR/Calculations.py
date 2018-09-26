
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer

from Projects.DIAGEOBR.KPIGenerator import DIAGEOBRGenerator
from KPIUtils.GlobalProjects.DIAGEO.KPIGenerator import DIAGEOGenerator
from KPIUtils.DB.Common import Common

__author__ = 'Nimrod'


class DIAGEOBRCalculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        common = Common(self.data_provider)
        diageo_generator = DIAGEOGenerator(self.data_provider, self.output, common)
        diageo_generator.diageo_global_assortment_function()
        common.commit_results_data_to_new_tables()
        common.commit_results_data()  # old tables
        DIAGEOBRGenerator(self.data_provider, self.output).main_function()
        self.timer.stop('KPIGenerator.run_project_calculations')

# if __name__ == '__main__':
#    LoggerInitializer.init('diageobr calculations')
#    Config.init()
#    project_name = 'diageobr'
#    data_provider = KEngineDataProvider(project_name)
#    session = '9bbd244e-c59b-44fb-b4d8-1f402b5514ce'
#    data_provider.load_session_data(session)
#    output = Output()
#    DIAGEOBRCalculations(data_provider, output).run_project_calculations()
