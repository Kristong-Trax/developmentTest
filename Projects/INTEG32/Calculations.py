from Projects.GFKDE.Calculations import GFKDECalculations
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
from KPIUtils_v2.Utils.Decorators.Decorators import log_runtime


class INTEG32Calculations(BaseCalculationsScript):

    @log_runtime("INTEG32 session runtime")
    def run_project_calculations(self):
        GFKDECalculations(self.data_provider, output=None).run_project_calculations()


# from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider
# from Trax.Utils.Conf.Configuration import Config
# from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
#
# if __name__ == '__main__':
#     LoggerInitializer.init('gfkde calculations')
#     Config.init()
#     project_name = 'gfkde'
#     data_provider = KEngineDataProvider(project_name)
#     # sessions = ['ad9cfb70-cdef-42e7-bfed-2bc9c4012ce1',
#     #             '492a6090-9e75-43f7-9461-4315cf92f9e0',
#     #             '498668b8-0644-4dd2-9391-2702b7d4cffb',
#     #             '56ec7fae-a00d-4b2f-b62f-94d5a0a6a172',
#     #             '9d48cf56-6f9a-4ac6-ba29-d4d587aeaa2d',
#     #             '29d8ecb9-d335-403b-bbd5-411ff1d352a2',
#     #             'b84269ac-774e-4e9b-a2f3-9984fefca213',
#     #             'fae5cf3a-16ea-40e3-9016-ff4bb2730291',
#     #             '77f12245-8b0c-414e-b160-2f60d7cb61ec',
#     #             '20d2c696-bd96-40cb-a6e0-dc731fcca34a',
#     #             'a707419d-3a76-4d66-a45b-5be7762baa89',
#     #             '4ad2199b-02c2-4dbb-a8a2-204b9d872ca1',
#     #             '6a1c63e4-58e1-45e7-a6b0-1125a35b944d',
#     #             '0c2d4138-fa35-4328-b156-281ad72bf8a9',
#     #             'd7ced1ad-0dd5-4bee-9061-51d2fde8c99c',
#     #             '5f39c5b0-9a13-408c-bcca-6d4e1df72d25',
#     #             '9e6fc5ea-62fe-45c2-be69-a80f297c0103',
#     #             '5c96586a-6c8e-4283-855c-f9e527696f61',
#     #             '9c579504-defe-4504-a15e-925264a6408f']
#     sessions = ['8889d1de-366d-4895-910e-e9d1396f3730',
#                 '9117364e-2b00-4c1d-b4ab-f1ff1a2a5bde',
#                 'a11f8105-cb53-4f28-b0fc-40d5657f71fa']
#
#     for session in sessions:
#         data_provider.load_session_data(session)
#         INTEG32Calculations(data_provider=data_provider, output=None).run_project_calculations()