
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Projects.HEINEKENMX.Calculations import Calculations


if __name__ == '__main__':
    LoggerInitializer.init('heinekenmx calculations')
    Config.init()
    project_name = 'heinekenmx'
    data_provider = KEngineDataProvider(project_name)
    session_list = ['4c325225-d5f4-45b9-a658-45f4e65d7268']
    for session in session_list:
        data_provider.load_session_data(session)
        output = Output()
        Calculations(data_provider, output).run_project_calculations()