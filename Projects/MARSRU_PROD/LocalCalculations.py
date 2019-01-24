from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Projects.MARSRU_PROD.Calculations import MARSRU_PRODCalculations


if __name__ == '__main__':
    LoggerInitial+izer.init('MARSRU calculations')
    Config.init()
    project_name = 'marsru-prod'
    session_uids = [
        'ff9c6b4b-463d-46e9-af28-9bc40478ae6a',
    ]
    data_provider = KEngineDataProvider(project_name)
    output = Output()
    for session in session_uids:
        data_provider.load_session_data(session)
        MARSRU_PRODCalculations(data_provider, output).run_project_calculations()

