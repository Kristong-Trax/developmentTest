from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Projects.MARSRU2_SAND.Calculations import MARSRU2_SANDCalculations


if __name__ == '__main__':
    LoggerInitializer.init('MARSRU calculations')
    Config.init()
    project_name = 'marsru2-sand'
    session_uids = [
        'fabda4af-65d9-42f3-a07d-978797d5e950',
    ]
    data_provider = KEngineDataProvider(project_name)
    output = Output()
    for session in session_uids:
        data_provider.load_session_data(session)
        MARSRU2_SANDCalculations(data_provider, output).run_project_calculations()

