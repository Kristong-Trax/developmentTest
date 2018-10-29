
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer

from Projects.DIAGEOTW_SAND.KPIGenerator import DIAGEOTW_SANDGenerator
from KPIUtils.GlobalProjects.DIAGEO.KPIGenerator import DIAGEOGenerator
from KPIUtils.DB.Common import Common

__author__ = 'Nimrod'


class DIAGEOTW_SANDCalculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        common = Common(self.data_provider)
        DIAGEOTW_SANDGenerator(self.data_provider, self.output).main_function()
        # DIAGEOGenerator(self.data_provider, self.output, common).diageo_global_assortment_function()
        common.commit_results_data_to_new_tables()
        self.timer.stop('KPIGenerator.run_project_calculations')


if __name__ == '__main__':
    LoggerInitializer.init('diageoau calculations')
    Config.init()
    project_name = 'diageotw-sand'
    data_provider = KEngineDataProvider(project_name)
    session = '1DDA97EE-668C-4008-8727-2605C38EE3D6'
    data_provider.load_session_data(session)
    output = Output()
    DIAGEOTW_SANDCalculations(data_provider, output).run_project_calculations()
