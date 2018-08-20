
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from KPIUtils.DB.Common import Common
from Projects.DIAGEOAU_SAND.KPIGenerator import DIAGEOAU_SANDGenerator
from KPIUtils.GlobalProjects.DIAGEO.KPIGenerator import DIAGEOGenerator

__author__ = 'Nimrod'


class DIAGEOAU_SANDCalculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        common = Common(self.data_provider)
        DIAGEOAU_SANDGenerator(self.data_provider, self.output).main_function()
        # DIAGEOGenerator(self.data_provider, self.output, common).diageo_global_assortment_function()
        # common.commit_results_data_to_new_tables()
        self.timer.stop('KPIGenerator.run_project_calculations')


if __name__ == '__main__':
    LoggerInitializer.init('diageoau calculations')
    Config.init()
    project_name = 'diageoau-sand'
    data_provider = KEngineDataProvider(project_name)
    session = 'd460b087-e2ed-4791-a443-d4991c037313'
    data_provider.load_session_data(session)
    output = Output()
    DIAGEOAU_SANDCalculations(data_provider, output).run_project_calculations()
