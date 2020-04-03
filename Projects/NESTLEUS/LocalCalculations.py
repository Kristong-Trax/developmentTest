
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer

from Projects.NESTLEUS.Calculations import Calculations
from Projects.NESTLEUS.Utils import Const

import pandas as pd


if __name__ == '__main__':
    LoggerInitializer.init('nestleus calculations')
    Config.init()
    project_name = 'nestleus'
    data_provider = KEngineDataProvider(project_name)

    test_sessions = pd.read_excel(Const.TEST_SESSIONS_PATH)
    session = 'ff474ba2-fb55-422a-adb1-44ae3410bbf3' #'9bffe5d5-ee61-4246-b545-560c2acfbb1b'

    for session in test_sessions.itertuples():
        data_provider.load_session_data(session.Session_uid)
        output = Output()
        Calculations(data_provider, output).run_project_calculations()

    print("Done")