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

    sessions = [
        '0796a34c-cd4b-40ab-9b12-c39c9b34f116',
        'b6e51b3a-30ff-4081-89ff-10a6e0bd8a90',
        '37750ad0-a454-4e28-98a0-1a8753528782',
        '80d936ca-f7be-45cf-a75d-bb32bb2dd0c2',
        '03fc01a2-22a9-4804-9949-43c01187749e',
        '04140cd0-2080-41cc-8c93-acbd47771b6a',
        '041550ae-cee3-4029-ac65-a59e15efa8b7',
        '04188833-016f-4cb4-88fd-82b2cb7e4e63',
        '044b4f5e-366f-482d-9078-5c303f8b3c3b',
        '0497cd3a-3764-40f5-b8ae-73501cb52941',
        '04a31579-5b75-4433-95c9-ed9cff2f2dff',
        '04ac6918-e491-4381-8286-5a7626e6734b',
        '04d5bbfc-c195-486b-afaf-01f283d0bea7',
        '052f3995-e35c-4015-abdc-5b7c586d5a43',
        '05319b71-6d10-4136-bcf2-54f78556abd1',
        '05329f7b-4c3c-4c25-86a1-7464fd5bfb98',
        '054e7437-9d6c-42e9-b210-ae7e8debffa9',
        '055ee527-d766-4c88-9513-dba67d543e4c',
        '0574016a-4d61-4d2d-bbf4-2ec49d23e238',
        '058ffc83-2eaf-4478-807b-68b463c4dedd',
        '05901a9c-237f-47e2-a600-57a833258fe8',
        '8a69571a-93d0-4c70-a3ce-8599d1ac7f0d',
        '7e4e0cf0-d624-4cd2-8662-eaec9c1f0666',
        'c9bb1b0a-34ad-414c-a31c-86e8873ab75a',
        '0f134729-8f07-4f81-9941-61e679469dd2',
        '4d7d78ff-c3b6-4487-b14d-80a551eeb3f8',
        '51a1d064-4548-44a3-ad05-9c8a6bdff64a',
        'd8cd3868-c487-43e4-943a-e15fe18d751a',
        '73eacc94-832a-40af-a878-4d093b17d53a',
        '00790bf2-90f2-4c9c-b533-d312986ecf60',
        '0bbc78de-8b87-42b9-970d-c5b99101d41b',
        '0ee00ea5-0292-4184-8987-7a7a58562d8f',
        '4e2268f5-ae76-4159-9839-6047262f1286',
        '1473d13d-1743-48d1-aac5-eca6af8f08a1',
        '7c95a0c1-79d2-47b6-9642-a7e7345e2b76',
        '00701a05-2cac-4b50-9b9a-8308521aba02',
        '475c7a06-ab42-41a1-9359-8c0b0c5ce2b2',
        '9c031bfb-f0bc-4757-bbd6-fa03f37b5c35',
        'b7d18fd1-ec5c-45ae-bf98-700b7cc7d70c',
        'bbc35a66-c655-4fb2-a95e-bdc13d8ea57c',
        'ad004314-eb47-4d8a-b267-514b0ac9a767',
        '92b7d806-e87a-4936-b956-082219902f9c',
        '6ba314aa-232a-4e19-82f5-ba74d424f7d1',
        '8f9629b8-6eef-4964-a94e-1ddd987bf1f6',
        'b52c9068-8d14-4a47-a37d-30d2303d293f',
        '804e8665-079a-4fee-b6dc-faaa0564d21e',
        '1cca12bd-3a1c-4b04-a453-ce3051f0aa0f',
        '4c1207c6-cfaf-49bf-8596-00f56ec433d2',
        '22a659d7-0fef-444c-a997-befa8b881964',
        'a4cb2375-c78b-4b93-a383-701e225a5213',
        'da270135-d876-43a6-a8be-ffa9e0b8086f',
        '0bbf7cd4-db17-4f2e-84f3-0827b965d204',
        '59a99f31-1eb9-4bdc-bd54-7d6ee3d53e9d',
        'cf8d88e8-f0ee-40c6-988f-abb5b6e19593',
        'e45ce3a5-cb9d-4c0f-8e0e-d8c42920db61',
        'fdbda6f2-5cb1-4193-97ee-fbcabab92fff',
        'ba04c9ae-85b9-42fd-b80a-2fad50c20517',
    ]


    # sessions = ['e54a25c4-00f9-4287-8b1f-07af15a23dd4']
    # sessions = ['2c2a7be1-acc0-4373-b4bc-d08db2753ffb']
    # sessions = ['9f3a2329-6cb1-4279-8520-d23c2fd6d75f']

    sessions = ['00701a05-2cac-4b50-9b9a-8308521aba02']
    sessions = ['70a8fcf5-0313-4a7f-81de-643d714c7543']

    for session in sessions:
        print('~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~{}~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~'.format(session))
        data_provider = KEngineDataProvider(project_name)
        data_provider.load_session_data(session)
        output = Output()
        Calculations(data_provider, output).run_project_calculations()
