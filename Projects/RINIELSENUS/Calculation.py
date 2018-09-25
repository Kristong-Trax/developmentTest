# import pandas as pd
# from Trax.Utils.Conf.Keys import DbUsers
# from mock import patch
# 
# from KPIUtils.DB.Common import Common
from Projects.RINIELSENUS.KPIGenerator import MarsUsGenerator
# from Projects.RINIELSENUS.Utils.ParseTemplates import ParseMarsUsTemplates
# from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
# from Trax.Utils.Conf.Configuration import Config
# from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
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
#                 '5687bd59-fd20-423e-8bea-d1b4a28da264',
                # '0aa034cb-6057-4d62-8150-4b1207491e39',
                # 'ae04abb8-a0b2-49a0-a681-60563c5e6efe',
                # '0fce09fd-0b33-4495-8e8b-e213aa30e94f',
                # '11e562a0-928c-4794-9d9b-64cd6544c84d',
                # '5C8E563C-66B6-405F-865F-22EDCD91C7A1'
                # 'bc90bfdb-e446-4de0-9911-22391648578b',
                # 'a4b927b2-42ac-4879-aec1-52bd2f716068'
    # ]
    #
    # sessions = [
    #             '0784fd4e-1bde-4511-acb9-04d5cbd2aae9',
    #             '0cd3a0f5-2e5c-4163-8605-32eb689590b1',
    #             '13599d17-6bba-4926-a713-3051b411d6f4',
    #             # '1b814545-51b4-4ce8-960b-d53815937b5b',
    #             # '38992e47-d6ef-4ad5-987b-df27d9cfbbfb',
    #             # '62468ec9-d607-42f6-bb34-542410f1f556',
    #             ]
    #
    # sessions = [
    #             '03b2eb1d-fad9-430d-a04b-26e62ecd837c',
    #             '05894f40-b6fc-47ba-932d-e162b59322bb',
    #             '065fbf93-b335-45cb-8434-477913898594',
    #             '242d1600-e2c2-45f3-994c-eb57623a4deb'
    #             ]

    # sessions = [
    #             # 'a799771a-bc07-47a0-b2aa-c3d62a56038b',
    #             # '4afdbfd5-3257-4d01-94f8-37fd16ce950f',
    #             # '0a138058-3b88-4543-8501-d19f83c1b671',
    #             'cd3465ce-fb3d-4cc3-b487-c216521f0e87',
    #             # '81cf50bf-c4f7-4812-9e2f-d28d0fcecdca'
    #             ]
    #
    #
    # # sessions = pd.read_csv('/home/Ilan/Documents/projects/marus/0612_batch_300.csv')['session_uid'].tolist()[:25]
    #
    # for session in sessions:
    #     print('*******************************************************************')
    #     print('--------------{}-------------'.format(session))
    #     Log.info('starting session : {}'.format(session))
    #     data_provider = KEngineDataProvider(project_name)
    #     # session = Common(data_provider).get_session_id(session)
    #     data_provider.load_session_data(session)
    #     output = Output()
    #     MarsUsCalculations(data_provider, output).run_project_calculations()

