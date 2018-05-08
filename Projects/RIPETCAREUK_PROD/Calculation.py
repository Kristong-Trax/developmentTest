
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
# from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
# from Trax.Utils.Conf.Configuration import Config
# from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Projects.RIPETCAREUK_PROD.KPIGenerator import MarsUkGenerator

__author__ = 'Nimrod'


class MarsUkCalculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        MarsUkGenerator(self.data_provider, self.output).main_function()
        self.timer.stop('KPIGenerator.run_project_calculations')


# if __name__ == '__main__':
#     LoggerInitializer.init('TREX')
#     Config.init()
#     project_name = 'ripetcareuk-prod'
#     sessions_2016 = ['fe2e796a-648a-445e-a23b-50a9b8944aeb',
#                      'f40fd2ef-4bd2-4b4e-b7f6-779d42df6fef',
#                      'a959bdde-7660-487c-bb91-435d11e15514',
#                      '19bac8fb-a7f5-438a-885b-07379b10a89d',
#                      'd47832cf-aeff-4879-986c-ee61cb754812'
#                 ]
#     sessions_2017 = ['de0b6eef-1432-4ee9-b6d5-114be2b5a61d',
#                      '0020b0cb-830a-4323-9588-52130f222450',
#                      '8beb1811-4184-4cc3-8b4a-5f446d557125',
#                      '9ede3c35-65c3-44db-b7b0-69f8f00b4862',
#                      '2DED301E-1E32-4128-85A8-F28433724815'
#                      ]
#
#     for session in sessions_2016:
#         data_provider = KEngineDataProvider(project_name)
#         data_provider.load_session_data(session)
#         output = Output()
#         MarsUkCalculations(data_provider, output).run_project_calculations()
