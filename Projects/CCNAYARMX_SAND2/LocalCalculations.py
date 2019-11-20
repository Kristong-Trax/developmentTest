from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Projects.CCNAYARMX_SAND2.Calculations import Calculations

if __name__ == '__main__':
    LoggerInitializer.init('ccnayarmx-sand2 calculations')
    Config.init()
    project_name = 'ccnayarmx-sand2'

    from Trax.Data.Projects.Store import ProjectStore

    ProjectStore.get_project(project_name)['rds_name'] = ProjectStore.get_project(project_name)['rds_name'].replace(
        'mysql.vpn.', 'mysql.')

    data_provider = KEngineDataProvider(project_name)
    session_list = [
        'c2323d65-b6dc-46c4-bc3e-d3d3d9a15302',
        #             'e1f58875-2b5b-4b93-a596-ae5baca33b09'
        # '9c558394-4305-4f5e-94de-5fd8101db0e7'
    ]
    for session in session_list:
        data_provider.load_session_data(session)
        output = Output()
        Calculations(data_provider, output).run_project_calculations()
