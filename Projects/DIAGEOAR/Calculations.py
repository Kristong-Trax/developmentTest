
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
# from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
# from Trax.Utils.Conf.Configuration import Config
# from Trax.Cloud.Services.Connector.Logger import LoggerInitializer

from KPIUtils.GlobalProjects.DIAGEO.KPIGenerator import DIAGEOGenerator
from Projects.DIAGEOAR.KPIGenerator import DIAGEOARDIAGEOARGenerator
from KPIUtils.DB.Common import Common

__author__ = 'Yasmin'


class DIAGEOARDIAGEOARCalculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        DIAGEOARDIAGEOARGenerator(self.data_provider, self.output).main_function()
        self.timer.stop('KPIGenerator.run_project_calculations')


# if __name__ == '__main__':
#     LoggerInitializer.init('diageoar-sand calculations')
#     Config.init()
#     project_name = 'diageoar'
#     data_provider = KEngineDataProvider(project_name)
#     session = '46199985-c389-11e7-af51-12d95a7ac5ac'
#     data_provider.load_session_data(session)
#     output = Output()
#     DIAGEOARDIAGEOARCalculations(data_provider, output).run_project_calculations()
