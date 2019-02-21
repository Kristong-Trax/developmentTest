
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Projects.PNGHK.Calculations import Calculations


if __name__ == '__main__':
    LoggerInitializer.init('pnghk calculations')
    Config.init()
    project_name = 'pnghk'
    data_provider = KEngineDataProvider(project_name)
    data_provider = KEngineDataProvider(project_name)
    sessions = ['fd12720b-be37-43cc-ab35-4e44d3a2aa5c',
                'fbc3e8c6-1857-46d0-9f10-ccf19cfa4d72',
                'ea5dfbd5-8ea0-4e5c-b5f8-f05c61ac37cd'
                ]
    for session in sessions:
        data_provider.load_session_data(session)
        output = Output()
        Calculations(data_provider, output).run_project_calculations()
