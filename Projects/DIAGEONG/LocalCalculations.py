
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Projects.DIAGEONG.Calculations import Calculations


if __name__ == '__main__':
    LoggerInitializer.init('diageong calculations')
    Config.init()
    project_name = 'diageong'
    data_provider = KEngineDataProvider(project_name)
    session_list = ['6372ba61-456e-4b1e-a3d6-15329222fa5f']
    for session in session_list:
        data_provider.load_session_data(session)
        output = Output()
        Calculations(data_provider, output).run_project_calculations()
