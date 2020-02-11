
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Projects.NESTLEBEVUS.Calculations import Calculations


if __name__ == '__main__':
    LoggerInitializer.init('nestlebevus calculations')
    Config.init()
    project_name = 'nestlebevus'
    data_provider = KEngineDataProvider(project_name)
    session_list = ['6e44528c-91d2-4b2e-b4c4-200c56c5bb37',
                    '9d7f2b8e-c3e1-4ac1-bcb9-a881db1e9ac9']
    for session in session_list:
        data_provider.load_session_data(session)
        output = Output()
        Calculations(data_provider, output).run_project_calculations()
