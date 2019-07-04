from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Projects.DELMONTEUS.Calculations import Calculations


if __name__ == '__main__':
    LoggerInitializer.init('delmonteus calculations')
    Config.init()
    project_name = 'delmonteus'

    sessions = [
                '7622a28d-faa0-4332-8daf-e94147fe961c',
                'e54a25c4-00f9-4287-8b1f-07af15a23dd4',
                '9f3a2329-6cb1-4279-8520-d23c2fd6d75f',
                'b1319c6e-2bfb-414f-82a3-1bb8a66428d8',
                'da67f51f-27f3-404c-a00b-2bb1fbe881ec',
                '794c638e-6f87-4f7a-bb2b-30affd83b910',
                'e0c62f6f-428b-4a9c-a08a-f4e2c5f77404',
                '37750ad0-a454-4e28-98a0-1a8753528782',
                'b6e51b3a-30ff-4081-89ff-10a6e0bd8a90',
                '80d936ca-f7be-45cf-a75d-bb32bb2dd0c2',
                ]


    # sessions = ['e54a25c4-00f9-4287-8b1f-07af15a23dd4']
    sessions = ['2c2a7be1-acc0-4373-b4bc-d08db2753ffb']

    for session in sessions:
        print('~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~{}~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~'.format(session))
        data_provider = KEngineDataProvider(project_name)
        data_provider.load_session_data(session)
        output = Output()
        Calculations(data_provider, output).run_project_calculations()
