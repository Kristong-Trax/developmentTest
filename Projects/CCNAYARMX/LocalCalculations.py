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

        # 'd5613cbc-6f10-46a2-9e99-acb6c8c21edb',
        'cf12065b-e383-4009-a1e8-c8d9ced9bbf9',
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
