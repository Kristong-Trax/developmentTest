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
        'fcd488da-a2ed-4587-8579-6574fec69c8a'

                ]

    for session in sessions:
        data_provider = KEngineDataProvider(project_name)
        data_provider.load_session_data(session)
        output = Output()
        Calculations(data_provider, output).run_project_calculations()

