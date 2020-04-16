
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

    #checkout('0d71ea3c-6430-488e-88e1-dd83ef64d2c9')

    test_sessions = pd.read_excel(Const.TEST_SESSIONS_PATH)

    for session in test_sessions.itertuples():
        data_provider.load_session_data(session.Session_uid)
        output = Output()
        Calculations(data_provider, output).run_project_calculations()

    print("Done")
