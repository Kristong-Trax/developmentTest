from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Projects.CCNAYARMX.Calculations import Calculations

if __name__ == '__main__':
    LoggerInitializer.init('ccnayarmx calculations')
    Config.init()
    project_name = 'ccnayarmx'

    from Trax.Data.Projects.Store import ProjectStore

    ProjectStore.get_project(project_name)['rds_name'] = ProjectStore.get_project(project_name)['rds_name'].replace(
        'mysql.vpn.', 'mysql.')

    data_provider = KEngineDataProvider(project_name)
    session_list = [
        'a25897ed-412a-4c13-a65a-7e89a143f08f',
        # '8c6282cd-f039-417c-b095-e634e72009b2',
        # 'fd6ffdbc-aed1-4985-8839-e6e818dd1cba',
        # '37faa72d-6736-4b91-a706-200e86393cac',
        # 'fd6ffdbc-aed1-4985-8839-e6e818dd1cba',
        # '64547dd6-a854-4d2b-9a0d-d2fdc1309eb1'

        # '0001bcaa-fa9f-41d7-9b05-ecf606e3fcea',
        # '0061aa06-5a5c-4632-809a-05896f5d0449',
        # '00719b93-e709-453f-a95a-3d8f4c0c323f'

        # 'ff28d81f-8fe0-4404-a63c-264b1e094b39',
        # 'fd6ffdbc-aed1-4985-8839-e6e818dd1cba',
        # 'fd0abb6f-1699-40e5-a29e-3b2ed863a45b',
        # 'fcf9cff3-3ff4-4ed2-bdc8-899a207b4d2f'

        # '2d802f39-05d4-47ee-9af0-97b7cb3efa46',
        # 'd5613cbc-6f10-46a2-9e99-acb6c8c21edb',
        # '2d802f39-05d4-47ee-9af0-97b7cb3efa46',
        # 'f8f413dc-7a9c-47d6-813b-dadd7f834a5f'
        # 'fd0abb6f-1699-40e5-a29e-3b2ed863a45b'

        # 'd5613cbc-6f10-46a2-9e99-acb6c8c21edb',
        # 'cf12065b-e383-4009-a1e8-c8d9ced9bbf9',
        # 'b974cb3b-a609-4610-ab14-ae94163d0c76',
        # 'cf12065b-e383-4009-a1e8-c8d9ced9bbf9',
        # 'a25897ed-412a-4c13-a65a-7e89a143f08f',
        # 'd64cbf6a-c1eb-4c10-a880-668617dd38ac',
        # '77acfbe0-347e-4f23-bab6-f93781515b31'
    ]
    for session in session_list:
        data_provider.load_session_data(session)
        output = Output()
        Calculations(data_provider, output).run_project_calculations()
