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
                # 'AD5CDDE6-4157-4F1F-B6FF-49BDF3F8EC82',  # 15775
        # '9F19D5B1-B95D-40E6-A7EA-B36E963A0770',
        # 'F3ABA765-DEF6-46A4-8C1E-BC818532398A',
        # '26F8B85F-F6B9-4BAE-89EC-4A3C499BAE19',
        '8B46BA22-FFC6-438B-9B5B-AAB189F4F498'


        # '39D117D6-B2A0-4523-92CE-E286CFC4AB60'

                ]

    # sessions = ['F3ABA765-DEF6-46A4-8C1E-BC818532398A']

    for session in sessions:
        print('~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~{}~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~'.format(session))
        data_provider = KEngineDataProvider(project_name)
        data_provider.load_session_data(session)
        output = Output()
        Calculations(data_provider, output).run_project_calculations()
