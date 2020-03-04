
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Projects.CCNAYARMX.Calculations import Calculations


if __name__ == '__main__':
    LoggerInitializer.init('ccnayarmx calculations')
    Config.init()
    project_name = 'ccnayarmx'
    data_provider = KEngineDataProvider(project_name)
    session_list = [
#         '5f20ba65-63ae-4057-a8b0-3491accf7869',
    ]

    for session in session_list:
        data_provider.load_session_data(session)
        output = Output()
        Calculations(data_provider, output).run_project_calculations()
