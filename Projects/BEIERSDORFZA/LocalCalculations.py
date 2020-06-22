from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Projects.BEIERSDORFZA.Calculations import Calculations
from Trax.Utils.Conf.Configuration import Config


if __name__ == '__main__':
    LoggerInitializer.init('beiersdorfza calculations')
    Config.init()
    project_name = 'beiersdorfza'
    data_provider = KEngineDataProvider(project_name)
    session_list = ['']
    for session in session_list:
        data_provider.load_session_data(session)
        output = Output()
        Calculations(data_provider, output).run_project_calculations()
