from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Trax.Utils.Conf.Configuration import Config

from Projects.CCRU.Calculations import CCRUCalculations


if __name__ == '__main__':
    LoggerInitializer.init('CCRU calculations')
    Config.init()
    project_name = 'ccru'
    data_provider = KEngineDataProvider(project_name)
    session_uids = [
        'FFF6C1E8-4691-4538-8483-E8FCA3038F5C'
    ]
    for session in session_uids:
        data_provider.load_session_data(session)
        output = Output()
        CCRUCalculations(data_provider, output).run_project_calculations()
