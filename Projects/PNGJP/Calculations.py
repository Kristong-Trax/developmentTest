# from Trax.Utils.Conf.Configuration import Config
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
from Projects.PNGJP.KPIGenerator import PNGJPGenerator
# from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
# from Trax.Cloud.Services.Connector.Logger import LoggerInitializer


__author__ = 'Nimrod'


class PNGJPCalculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        PNGJPGenerator(self.data_provider, self.output).main_function()
        self.timer.stop('KPIGenerator.run_project_calculations')

# if __name__ == '__main__':
#     LoggerInitializer.init('pngjp calculations')
#     Config.init()
#     project_name = 'pngjp'
#     data_provider = KEngineDataProvider(project_name)
#     session = 'ED9E7E80-5A03-478F-ADAF-F3535F9B0DED'
#     data_provider.load_session_data(session)
#     output = Output()
#     PNGJPCalculations(data_provider, output).run_project_calculations()