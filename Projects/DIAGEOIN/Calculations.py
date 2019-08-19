
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
# from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
# from Trax.Utils.Conf.Configuration import Config
# from Trax.Cloud.Services.Connector.Logger import LoggerInitializer

from Projects.DIAGEOIN.KPIGenerator import DIAGEOINGenerator


__author__ = 'satya'


class DIAGEOINCalculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        DIAGEOINGenerator(self.data_provider, self.output).main_function()
        self.timer.stop('KPIGenerator.run_project_calculations')


# if __name__ == '__main__':
#     LoggerInitializer.init('DIAGEOIN calculations')
#     Config.init()
#     project_name = 'diageoin'
#     data_provider = KEngineDataProvider(project_name)
#     #session = 'f6463d98-5e1c-44e8-a571-8999b79e5e3d'
#     #session = 'bba91efe-a875-46e6-832c-1b18bb915805'
#     #session = 'ef4d3219-a30e-4262-9971-8c5c95f107ff'
#     #session = 'be57e282-ee34-4c99-a86b-b561a36b43b2'
#     #session = 'be2e295f-4704-4c8f-b4b5-fac451806331'
#     #session = 'b31a22c9-1d58-4b3a-bc5a-2d23ba4acf23'
#     session = 'c7aa8241-df29-4986-a617-7b426e62af26'
#     data_provider.load_session_data(session)
#     output = Output()
#     DIAGEOINCalculations(data_provider, output).run_project_calculations()