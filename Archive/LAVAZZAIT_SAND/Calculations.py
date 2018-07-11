
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
# from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
# from Trax.Utils.Conf.Configuration import Config
# from Trax.Cloud.Services.Connector.Logger import LoggerInitializer

from KPIUtils.LAVAZZA.SuccessCriteria import LAVAZZAGSuccessCriteria

__author__ = 'Nimrod'


class LAVAZZAIT_SANDCalculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        LAVAZZAGSuccessCriteria(self.data_provider, self.output).main_function()
        self.timer.stop('KPIGenerator.run_project_calculations')


# if __name__ == '__main__':
#     LoggerInitializer.init('Lavazza DE Success Criteria')
#     Config.init()
#     project_name = 'lavazzait_sand'
#     data_provider = KEngineDataProvider(project_name)
#     session = '64cf60ed-42b6-4275-b7ca-95d23b87dee4'
#     data_provider.load_session_data(session)
#     output = Output()
#     LAVAZZAIT_SANDCalculations(data_provider, output).run_project_calculations()
