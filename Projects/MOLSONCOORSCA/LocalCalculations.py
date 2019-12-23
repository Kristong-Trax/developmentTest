from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Projects.MOLSONCOORSCA.Calculations import Calculations

from Projects.MOLSONCOORSCA.Helpers.kpi_speculator import AtomicSpeculator

if __name__ == '__main__':
    LoggerInitializer.init('MOLSONCOORSCA calculations')
    Config.init()
    project_name = 'molsoncoorsca'

    # AtomicSpeculator(project_name, '/home/samk/Downloads/KPI Overview Molson Coors v5.3.xlsx').run()

    # from Trax.Data.Projects.Store import ProjectStore
    # ProjectStore.get_project(project_name)['rds_name'] = ProjectStore.get_project(project_name)['rds_name'].replace(
    #     'mysql.', 'mysql.vpn.')

    sessions = [

        '4B02965C-290D-4221-B91E-B430A51E2498'

                ]

    for session in sessions:
        print('~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~{}~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~'.format(session))
        data_provider = KEngineDataProvider(project_name)
        data_provider.load_session_data(session)
        output = Output()
        Calculations(data_provider, output).run_project_calculations()
