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
import time


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


if __name__ == '__main__':
    # LoggerInitializer.init('TREX')
    LoggerInitializer.init('rinielsenus')
    Config.init()
    # docker_user = DbUsers.Docker
    # dbusers_class_path = 'Trax.Utils.Conf.Keys'
    # dbusers_patcher = patch('{0}.DbUser'.format(dbusers_class_path))
    # dbusers_mock = dbusers_patcher.start()
    # dbusers_mock.return_value = docker_user
    project_name = 'rinielsenus'

    sessions = [
        # 'd4391ffb-78a2-4f70-be3c-a0309f47b1ea',
        # 'b3970bc2-de98-4dc5-9289-75cf9255e8d0',
        # '63a38dae-84da-4214-95b3-6cdd33c27869'
        # 'e8dab645-dd5f-4ab4-89d5-f17a0e4b6aab',
        # '2d1c224a-b413-4694-b9fa-5a5aa5198dcd'
        # 'c768c2fb-d616-4bd6-81d1-b283039b8133',
        # '79680014-b66b-43e4-8ccc-f25b752aecff'
        # 'e8d50c6c-a7f8-401a-a4d4-149415365cf7'
    ]
    sessions = [
                'e8dab645-dd5f-4ab4-89d5-f17a0e4b6aab',
                '2d1c224a-b413-4694-b9fa-5a5aa5198dcd'
                ]

    # sessions = pd.read_csv('/home/Ilan/Documents/projects/marus/0612_batch_300.csv')['session_uid'].tolist()[:25]
    # sessions = ['b0c69988-5527-4034-ab42-e07f968d2891']
    for session in sessions:
        print
        print('*******************************************************************')
        print('--------------{}-------------'.format(session))
        s = time.time()
        Log.info('starting session : {}'.format(session))
        data_provider = KEngineDataProvider(project_name)
        # session = Common(data_provider).get_session_id(session)
        data_provider.load_session_data(session)
        output = Output()
        MarsUsCalculations(data_provider, output).run_project_calculations()
        print('session took {} minutes to calculate'.format((time.time() - s)/60.0))
