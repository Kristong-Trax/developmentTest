from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Projects.MARSRU_PROD.Calculations import MARSRU_PRODCalculations


if __name__ == '__main__':
    LoggerInitializer.init('MARSRU calculations')
    Config.init()
    project_name = 'marsru-prod'
    session_uids = [
        'b6677800-673d-43f8-90e2-967c57d36bd6',
    ]
    data_provider = KEngineDataProvider(project_name)
    output = Output()
    for session in session_uids:
        data_provider.load_session_data(session)
        MARSRU_PRODCalculations(data_provider, output).run_project_calculations()