
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Projects.STRAUSSIL_SAND.Calculations import Calculations


if __name__ == '__main__':
    LoggerInitializer.init('straussil-sand calculations')
    Config.init()
    project_name = 'straussil-sand'
    data_provider = KEngineDataProvider(project_name)
    session_list = ['85ed1059-8ad9-4830-ab43-318b84cda6a0']
    for session in session_list:
        data_provider.load_session_data(session)
        output = Output()
        Calculations(data_provider, output).run_project_calculations()
