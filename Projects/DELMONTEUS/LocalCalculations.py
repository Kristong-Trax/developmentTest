from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Projects.DELMONTEUS.Calculations import Calculations


if __name__ == '__main__':
    LoggerInitializer.init('delmonteus calculations')
    Config.init()
    project_name = 'delmonteus'

    from Trax.Data.Projects.Store import ProjectStore
    ProjectStore.get_project(project_name)['rds_name'] = ProjectStore.get_project(project_name)['rds_name'].replace(
        'mysql.', 'mysql.vpn.')

    sessions = [
        'e54a25c4-00f9-4287-8b1f-07af15a23dd4',
        'e54a25c4-00f9-4287-8b1f-07af15a23dd4',
        'b6e51b3a-30ff-4081-89ff-10a6e0bd8a90',
        '4d7d78ff-c3b6-4487-b14d-80a551eeb3f8',
        'd8cd3868-c487-43e4-943a-e15fe18d751a',
        'e0c62f6f-428b-4a9c-a08a-f4e2c5f77404',
        '7622a28d-faa0-4332-8daf-e94147fe961c',
        '475c7a06-ab42-41a1-9359-8c0b0c5ce2b2',
        'ad004314-eb47-4d8a-b267-514b0ac9a767',
        '0796a34c-cd4b-40ab-9b12-c39c9b34f116',
        '37750ad0-a454-4e28-98a0-1a8753528782',
        '9f3a2329-6cb1-4279-8520-d23c2fd6d75f',

    ]


    # sessions = ['e54a25c4-00f9-4287-8b1f-07af15a23dd4']
    # sessions = ['2c2a7be1-acc0-4373-b4bc-d08db2753ffb']
    # sessions = ['9f3a2329-6cb1-4279-8520-d23c2fd6d75f']

    sessions = ['19e7914a-1c8a-4208-bbbb-270e5a326a1b']

    for session in sessions:
        print('~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~{}~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~'.format(session))
        data_provider = KEngineDataProvider(project_name)
        data_provider.load_session_data(session)
        output = Output()
        Calculations(data_provider, output).run_project_calculations()
