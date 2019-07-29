import pandas as pd
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Projects.MONDELEZUS.Calculations import Calculations




if __name__ == '__main__':
    LoggerInitializer.init('mondelezus calculations')
    Config.init()
    project_name = 'mondelezus'

    sessions = [
        '589a704b-c4ae-4c44-b4a9-91b4595bd83e'

                ]

    for session in sessions:
        data_provider = KEngineDataProvider(project_name)
        data_provider.load_session_data(session)
        output = Output()
        Calculations(data_provider, output).run_project_calculations()

