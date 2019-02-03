# import pandas as pd
# from Trax.Utils.Conf.Keys import DbUsers
# from mock import patch
# 
# from KPIUtils.DB.Common import Common
from Projects.RINIELSENUS.KPIGenerator import MarsUsGenerator
from Projects.RINIELSENUS.Utils.ParseTemplates import ParseMarsUsTemplates
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
from Trax.Utils.Logging.Logger import Log


__author__ = 'nethanel'


class MarsUsCalculations(BaseCalculationsScript):
    def run_project_calculations(self):
        if True:
            self.timer.start()
            try:
                MarsUsGenerator(self.data_provider, self.output).main_function()
            except:
                Log.error('Mars US kpis not calculated')
            self.timer.stop('KPIGenerator.run_project_calculations')


# if __name__ == '__main__':
#     LoggerInitializer.init('TREX')
#     Config.init()
#     # docker_user = DbUsers.Docker
#     # dbusers_class_path = 'Trax.Utils.Conf.Keys'
#     # dbusers_patcher = patch('{0}.DbUser'.format(dbusers_class_path))
#     # dbusers_mock = dbusers_patcher.start()
#     # dbusers_mock.return_value = docker_user
#     project_name = 'rinielsenus'
#
#     sessions = [
#         # 'd4391ffb-78a2-4f70-be3c-a0309f47b1ea',
#         'b3970bc2-de98-4dc5-9289-75cf9255e8d0',
#         # '63a38dae-84da-4214-95b3-6cdd33c27869'
#     ]
#     sessions = [
#         '8b05e500-3607-4c63-97ec-db541a15a332',
#         '785d14ba-7a56-45a1-9ce0-09330ddf4663',
#         '6285d617-2b6a-44f0-8d7b-8bbbb93246d6',
#         '4dd124a5-1070-4d7c-ab61-12164976be6d',
#         'ede337b9-9125-444f-8174-399802e32bad',
#
#     ]
#
#     # sessions = pd.read_csv('/home/Ilan/Documents/projects/marus/0612_batch_300.csv')['session_uid'].tolist()[:25]
#
#     for session in sessions:
#         print('*******************************************************************')
#         print('--------------{}-------------'.format(session))
#         Log.info('starting session : {}'.format(session))
#         data_provider = KEngineDataProvider(project_name)
#         # session = Common(data_provider).get_session_id(session)
#         data_provider.load_session_data(session)
#         output = Output()
#         MarsUsCalculations(data_provider, output).run_project_calculations()

