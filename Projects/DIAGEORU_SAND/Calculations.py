
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer

from Projects.DIAGEORU_SAND.KPIGenerator import DIAGEORUGenerator
from KPIUtils.GlobalProjects.DIAGEO.KPIGenerator import DIAGEOGenerator
from KPIUtils.DB.Common import Common

__author__ = 'Nimrod'


class DIAGEORUCalculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        common = Common(self.data_provider)
        DIAGEORUGenerator(self.data_provider, self.output).main_function()
        DIAGEOGenerator(self.data_provider, self.output, common).diageo_global_assortment_function()
        common.commit_results_data_to_new_tables()
        self.timer.stop('KPIGenerator.run_project_calculations')


if __name__ == '__main__':
    LoggerInitializer.init('diageoru-sand calculations')
    Config.init()
    project_name = 'diageoru-sand'
    data_provider = KEngineDataProvider(project_name)
    session = 'aef8cee0-682a-4972-9359-c83ec5547653'
    data_provider.load_session_data(session)
    output = Output()
    DIAGEORUCalculations(data_provider, output).run_project_calculations()
