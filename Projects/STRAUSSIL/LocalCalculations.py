
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Projects.STRAUSSIL.Calculations import Calculations


if __name__ == '__main__':
    LoggerInitializer.init('straussil calculations')
    Config.init()
    project_name = 'straussil'
    data_provider = KEngineDataProvider(project_name)
    session_list = ['757170a1-179f-41c2-a316-7b72e4057d12']
    for session in session_list:
        data_provider.load_session_data(session)
        output = Output()
        Calculations(data_provider, output).run_project_calculations()
