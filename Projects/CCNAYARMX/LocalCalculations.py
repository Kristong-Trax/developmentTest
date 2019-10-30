
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
# '69b028e2-5119-4c11-a846-94ca29adede4',
# 'cf7bd046-0acc-4c60-bb33-3ed5dfdd836e',
# 'E45618DB-39E9-45A4-B541-054C85515A69',
'43913E06-9CFF-48C2-B768-F1924EB32D9C'
    ]

    for session in session_list:
        data_provider.load_session_data(session)
        output = Output()
        Calculations(data_provider, output).run_project_calculations()
