
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer

from Projects.NESTLEUS.Calculations import Calculations
from Projects.NESTLEUS.Utils import Const

import pandas as pd

def checkout(session):
    data_provider.load_session_data(session)
    output = Output()
    Calculations(data_provider, output).run_project_calculations()

if __name__ == '__main__':
    LoggerInitializer.init('nestleus calculations')
    Config.init()
    project_name = 'nestleus'
    data_provider = KEngineDataProvider(project_name)

    # checkout('03c096f9-21a1-4468-b6a5-49421a576c92')

    test_sessions = pd.read_excel(Const.TEST_SESSIONS_PATH)

    i = 0.0
    for session in test_sessions.itertuples():
        data_provider.load_session_data(session.Session_uid)
        output = Output()
        Calculations(data_provider, output).run_project_calculations()
        i += 1
        print("Completed {}% of local test sessions".format(round(i/len(test_sessions)*100, 2)))

    print("Done")
