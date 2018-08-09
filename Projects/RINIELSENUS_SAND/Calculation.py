# import pandas as pd
# from Trax.Utils.Conf.Keys import DbUsers
# from mock import patch

# from KPIUtils.DB.Common import Common
from Projects.RINIELSENUS_SAND.KPIGenerator import RinielsenUS_SANDGenerator
# from Projects.RINIELSENUS_SAND.Utils.ParseTemplates import ParseMarsUsTemplates
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
from Trax.Utils.Logging.Logger import Log


__author__ = 'nethanel'


class RinielsenUSSandCalculations(BaseCalculationsScript):
    def run_project_calculations(self):
        if True:
            self.timer.start()
            try:
                RinielsenUS_SANDGenerator(self.data_provider, self.output).main_function()
            except:
                Log.error('Rinielsen US kpis not calculated')
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
#                 # '5687bd59-fd20-423e-8bea-d1b4a28da264',
#                 '0aa034cb-6057-4d62-8150-4b1207491e39',
#                 # 'ae04abb8-a0b2-49a0-a681-60563c5e6efe',
#                 # '0fce09fd-0b33-4495-8e8b-e213aa30e94f',
#                 # '11e562a0-928c-4794-9d9b-64cd6544c84d',
#                 # '5C8E563C-66B6-405F-865F-22EDCD91C7A1'
#     ]
#     # sessions = pd.read_csv('/home/Ilan/Documents/projects/marus/0612_batch_300.csv')['session_uid'].tolist()[:25]
#
#     for session in sessions:
#         Log.info('starting session : {}'.format(session))
#         data_provider = KEngineDataProvider(project_name)
#         # session = Common(data_provider).get_session_id(session)
#         data_provider.load_session_data(session)
#         output = Output()
#         MarsUsCalculations(data_provider, output).run_project_calculations()
#
if __name__ == '__main__':
    LoggerInitializer.init('rinielsenus-sand calculations')
    Config.init()
    project_name = 'rinielsenus-sand'
    data_provider = KEngineDataProvider(project_name)
    session = 'e6bff9b4-07c7-4e80-93af-63759e103deb'
    data_provider.load_session_data(session)
    output = Output()
    RinielsenUSSandCalculations(data_provider, output).run_project_calculations()
