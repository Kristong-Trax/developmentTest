
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer

from Projects.NESTLEUS.Calculations import Calculations
from Projects.NESTLEUS.Utils import Const

import pandas as pd

def checkout(sessions):
    for session in sessions:
        data_provider.load_session_data(session)
        output = Output()
        Calculations(data_provider, output).run_project_calculations()

if __name__ == '__main__':
    LoggerInitializer.init('nestleus calculations')
    Config.init()
    project_name = 'nestleus'
    data_provider = KEngineDataProvider(project_name)

    checkout([
        #'0d71ea3c-6430-488e-88e1-dd83ef64d2c9'
        # '177c0014-46e6-41cc-a35d-2e2c462a2537',
    #           '50730f02-8ac1-4918-bc48-acf6fc02dfb6',
    #           '6a54ba4b-c2da-4939-bb70-79101f4016e1',
    #           'd318462b-324d-4426-abcf-21d191c6170d',
    #           '6a54ba4b-c2da-4939-bb70-79101f4016e1'
    ])

    test_sessions = pd.read_excel(Const.TEST_SESSIONS_PATH)

    i = 0.0
    for session in test_sessions.itertuples():
        data_provider.load_session_data(session.Session_uid)
        output = Output()
        Calculations(data_provider, output).run_project_calculations()
        i += 1
        print("Completed {}% of local test sessions".format(round(i/len(test_sessions)*100, 2)))

    print("Done")
