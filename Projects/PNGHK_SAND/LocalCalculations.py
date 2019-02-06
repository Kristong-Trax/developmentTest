
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Projects.PNGHK_SAND.Calculations import Calculations


if __name__ == '__main__':
    LoggerInitializer.init('pnghk-sand calculations')
    Config.init()
    project_name = 'pnghk-sand'
    data_provider = KEngineDataProvider(project_name)
    data_provider = KEngineDataProvider(project_name)
    sessions = ['6BE5606E-0FC2-41BB-9907-DC60E81B79BA',
                '3db4eb8e-4d4c-4b32-b131-2a5ac57e0234',
                'eaf3b934-8430-4c72-87e5-9d5ea85a596e']
    for session in sessions:
        data_provider.load_session_data(session)
        output = Output()
        Calculations(data_provider, output).run_project_calculations()
