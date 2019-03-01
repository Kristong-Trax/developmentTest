# import os
# from Projects.GMIUS.Helpers.Result_Uploader import ResultUploader
# from Projects.GMIUS.Helpers.Entity_Uploader import EntityUploader
# from Projects.GMIUS.Helpers.Atomic_Farse import AtomicFarse
# from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
# from Trax.Utils.Conf.Configuration import Config
# from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
# from Projects.GMIUS.Calculations import Calculations
#
#
# if __name__ == '__main__':
#     LoggerInitializer.init('gmius calculations')
#     Config.init()
#     project_name = 'gmius'
#     # ru = ResultUploader(project_name, '/home/samk/dev/kpi_factory/Projects/GMIUS/Data/RBG GMI KPI Template v0.2.xlsx')
#     # eu = EntityUploader(project_name, '/home/samk/dev/kpi_factory/Projects/GMIUS/Data/RBG GMI KPI Template v0.2.xlsx')
#     # af = AtomicFarse(project_name, '/home/samk/dev/kpi_factory/Projects/GMIUS/Data/RBG GMI KPI Template v0.2.xlsx')
#     # asdfas
#
#     sessions = ['508e23d5-f7c4-41a3-a5fd-9c0b123f82a3']
#
#     # sessions = [
#     #     # '36536ea2-a6c1-4c52-b9fc-8168fc0c385d',
#     #     # '2fe10dac-8bda-40ba-91eb-ea86016a6c6b',
#     #     # '10314089-10b2-416c-8db4-6c255f12492e',
#     #     # '3ee63d70-0696-4513-8307-957131460c3d',
#     #     # '50c99053-4504-4d05-af28-34a464706633',
#     #     '26175b23-f786-4564-a9a4-e8810ce231d9',
#     # ]
#     for session in sessions:
#         data_provider = KEngineDataProvider(project_name)
#         data_provider.load_session_data(session)
#         output = Output()
#         Calculations(data_provider, output).run_project_calculations()
#
