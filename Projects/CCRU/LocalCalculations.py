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
        'BBD53241-FF75-479A-A2E3-6DB6DAC5FC3C',

        # SAND
        # '3a3e96ee-3dfa-47f2-8cdb-6226756f62f8',
        # 'ff2d747e-99d6-4aab-a8fc-7eb625ee9c34',
        # 'ffc688ee-eb0e-483c-84a5-610017a7006d'
    ]
    for session in session_uids:
        data_provider.load_session_data(session)
        output = Output()
        CCRUCalculations(data_provider, output).run_project_calculations()
