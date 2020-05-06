from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Trax.Utils.Conf.Configuration import Config

from Projects.BATRU.Calculations import BATRUCalculations


if __name__ == '__main__':
    LoggerInitializer.init('BATRU calculations')
    Config.init()

    project_name = 'batru'
    data_provider = KEngineDataProvider(project_name)
    sessions = [
        # 'FFF7C170-12E7-4EC6-B292-D8BF1870741C',
        # 'c312125d-6814-49e5-af73-8e894e0e557d',
        'fffc4533-2b49-472b-b25c-30af62123bdd',
        'AF410EF7-90D8-431A-AF12-60DE6ED7438B'
    ]
    for session in sessions:
        data_provider.load_session_data(session)
        output = Output()
        BATRUCalculations(data_provider, output).run_project_calculations()
