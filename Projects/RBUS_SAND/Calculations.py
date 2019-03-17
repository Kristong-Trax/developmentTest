
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
from Projects.RBUS_SAND.KPIGenerator import RBUSGenerator

__author__ = 'yoava'


class RBUSCalculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        RBUSGenerator(self.data_provider, self.output).main_function()
        self.timer.stop('KPIGenerator.run_project_calculations')


# from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
# from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
# from Trax.Utils.Conf.Configuration import Config
# if __name__ == '__main__':
#     LoggerInitializer.init('rbus-sand calculations')
#     Config.init()
#     project_name = 'rbus-sand'
#     data_provider = KEngineDataProvider(project_name)
#     sessions  = ['9520D361-3A79-4509-9F68-4964E098D165',
# "6B7502BF-D4B2-44EF-90FE-39ECEC169A69",
# "6CA76BC7-50EC-4667-9659-E63D9A92DFD6",
# "4C963BF2-0912-4C5E-A423-CF5F6DC14201",
# "B0B4F206-EC0C-4225-826E-8338E53E09E6",
# "F2B0267C-6168-4431-8AFB-F01E647FF604",
# "917e9a32-0fb4-4b9a-9b5b-1474386b7f8c",
# "89207c5c-c286-4845-9e95-3f34c683bf7c",
# "13161EBB-3111-41D2-9A91-74D37F8BC6BA",
# "3D26E1C8-A1EF-4168-B38A-9C85113A1A65"]
#     for session in sessions:
#         data_provider.load_session_data(session)
#         output = Output()
#         RBUSCalculations(data_provider, output).run_project_calculations()
