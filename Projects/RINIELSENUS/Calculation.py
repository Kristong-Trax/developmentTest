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
        'd4391ffb-78a2-4f70-be3c-a0309f47b1ea',
        'b3970bc2-de98-4dc5-9289-75cf9255e8d0',
        '63a38dae-84da-4214-95b3-6cdd33c27869',
        'e8dab645-dd5f-4ab4-89d5-f17a0e4b6aab',
        '2d1c224a-b413-4694-b9fa-5a5aa5198dcd',
        'c768c2fb-d616-4bd6-81d1-b283039b8133',
        '79680014-b66b-43e4-8ccc-f25b752aecff',
        'e8d50c6c-a7f8-401a-a4d4-149415365cf7',

        'a505a0c0-ca71-49dd-bb88-f5214ecb2f0f ',
                    '8837a922-9510-4de4-bd52-f1619491ee4e'
    ]
    # sessions = [
    #     'e8dab645-dd5f-4ab4-89d5-f17a0e4b6aab',
    #     # '2d1c224a-b413-4694-b9fa-5a5aa5198dcd'
    # ]
    #
    # # sessions = pd.read_csv('/home/Ilan/Documents/projects/marus/0612_batch_300.csv')['session_uid'].tolist()[:25]
    # sessions = ['c93fc99a-78cf-4634-b7ab-59a9a62bf038']
    # sessions = [
    #            # '230c5655-52a4-4b68-a047-bfee8e390e76',
    #            'a505a0c0-ca71-49dd-bb88-f5214ecb2f0f ',
    #             '8837a922-9510-4de4-bd52-f1619491ee4e'
    #             ]
    #
    # sessions = ['f192c9a1-d954-4811-b89d-825815b3f2c4']

    sessions = [
        '420f3fa9-c285-48cf-ab16-8d2f764b45bd',
        '4dbab4c0-39c9-4e73-90af-af3cc9e3e843',
        '1dfd7282-5c5e-4783-b180-17e42b899e6b',
        '5f9dc5a0-22b7-4a64-8823-fa3a91505a68',
        '19b93d82-0c58-403d-9208-c5602743e100',
        'ec740f03-f7b6-4e84-9d19-80200eeae30e',
        '96b0d2a0-49a4-40d6-b59f-e76feb18741e',
        'a194f37c-7912-4341-837d-e97b584db2ee',
        '0fd5a157-5cef-4e09-9a3f-740f87e481ac',
        'a0036e16-6e0d-49ad-abfb-91f0a35d7cd9',
    ]

    sessions = ['c1119515-58bc-4655-8469-7afd5ec78c1c']

    sessions = [
        '0508b10a-f02d-401d-aaac-32778f04ea46',
        '98988859-c7cd-4279-8401-975aece024c8',
        '34b71f6f-8452-40d9-93a8-9923f5028803',
        '39c656e1-1663-44bc-b28e-dcb31aeaa9ec',
        '3be52507-cec6-4ee2-bff0-0bf47a345c78',
        '45c8c2b6-e8fe-43cb-9274-b76a0d4e3585',
        '40e51eec-07c7-48c9-91ac-c73305b966cb',
        'a99acaee-2542-4e59-9507-1b90348c2893',
        'a419ffbd-ecb2-46b4-b5f0-e33bf07e64ec',

    ]

    sessions = ['f920817e-3539-49aa-88c7-cade84e21ef4',
                '4c6e4746-835e-41d6-96fb-677a5a26cd0f',
                '74be7e6d-18af-4e31-9e41-f8b8aaf2241c',
                'ff26ffe8-58e7-4118-bcac-5985ed2d9250',
                '8c355431-6bb3-4c85-a608-722dbbe763be',
]


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
