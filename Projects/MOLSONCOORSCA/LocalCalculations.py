from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Projects.MOLSONCOORSCA.Calculations import Calculations
import os
import pandas as pd

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
        'FE94DB1C-273E-452E-83E0-5F33D4B2EC63',
        'FBBA7062-A750-4157-A708-D4072CD3B164',
        'FA21DCE8-CFD4-4E95-BA13-3B27A95616B5',
        'E51A03DB-4535-4D35-9AB9-506467885F4F',
        'EC4CE9B8-A98D-4F57-8318-E3F8F0E72154',
        '4B02965C-290D-4221-B91E-B430A51E2498'
    ]

    for session in sessions:
        print('~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~{}~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~'.format(session))
        data_provider = KEngineDataProvider(project_name)
        data_provider.load_session_data(session)
        output = Output()
        Calculations(data_provider, output).run_project_calculations()

    TEST_SESSIONS_PATH = os.path.join(os.path.dirname(__file__), 'Data', 'test sessions.xlsx')
    test_sessions = pd.read_excel(TEST_SESSIONS_PATH)
    for session in test_sessions.itertuples():
        data_provider = KEngineDataProvider(project_name)
        data_provider.load_session_data(session.session_uid)
        output = Output()
        Calculations(data_provider, output).run_project_calculations()
