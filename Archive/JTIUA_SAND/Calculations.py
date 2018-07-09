
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
# from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
# from Trax.Utils.Conf.Configuration import Config
# from Trax.Cloud.Services.Connector.Logger import LoggerInitializer


from Projects.JTIUA_SAND.KPIGenerator import JTIUA_SANDGenerator

__author__ = 'Nimrod'


class JTIUA_SANDCalculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        JTIUA_SANDGenerator(self.data_provider, self.output).main_function()
        self.timer.stop('KPIGenerator.run_project_calculations')


# if __name__ == '__main__':
#     LoggerInitializer.init('jtiua calculations')
#     Config.init()
#     project_name = 'jtiua-sand'
#     data_provider = KEngineDataProvider(project_name)
#     session = 'a88557dd-ba1c-4571-a4b3-b022f1388152'
#     data_provider.load_session_data(session)
#     output = Output()
#     JTIUA_SANDCalculations(data_provider, output).run_project_calculations()
