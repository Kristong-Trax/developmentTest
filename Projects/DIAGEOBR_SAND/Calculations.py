
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer

from Projects.DIAGEOBR_SAND.KPIGenerator import DIAGEOBR_SANDGenerator
from KPIUtils.GlobalProjects.DIAGEO.KPIGenerator import DIAGEOGenerator
from KPIUtils.DB.Common import Common

__author__ = 'Nimrod'


class DIAGEOBR_SANDCalculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        common = Common(self.data_provider)
        diageo_generator = DIAGEOGenerator(self.data_provider, self.output, common)
        diageo_generator.diageo_global_assortment_function()
        common.commit_results_data_to_new_tables()
        common.commit_results_data()  # old tables
        DIAGEOBR_SANDGenerator(self.data_provider, self.output).main_function()
        self.timer.stop('KPIGenerator.run_project_calculations')

if __name__ == '__main__':
    LoggerInitializer.init('diageobr calculations')
    Config.init()
    project_name = 'diageobr-sand'
    data_provider = KEngineDataProvider(project_name)
    for session in [
        '6f31e63d-6a1a-4222-9a14-bf3f24ce1eec',
        '3f333d63-50c3-4a98-b095-6684106d7cd7',
        '15d492cb-cce9-49c8-89d4-5c41d57573ac'
    ]:
        data_provider.load_session_data(session)
        output = Output()
        DIAGEOBR_SANDCalculations(data_provider, output).run_project_calculations()
