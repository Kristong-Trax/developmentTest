from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Trax.Utils.Conf.Configuration import Config

from Projects.CCRU_SAND.Calculations import CCRU_SANDCalculations


if __name__ == '__main__':
    LoggerInitializer.init('CCRU calculations')
    Config.init()
    project_name = 'ccru_sand'
    data_provider = KEngineDataProvider(project_name)
    session_uids = [
        '708E9277-2828-4574-85CA-0EF17A634B29',

        # SAND
        # '3a3e96ee-3dfa-47f2-8cdb-6226756f62f8',
        # 'ff2d747e-99d6-4aab-a8fc-7eb625ee9c34',
        # 'ffc688ee-eb0e-483c-84a5-610017a7006d'
    ]
    for session in session_uids:
        data_provider.load_session_data(session)
        output = Output()
        CCRU_SANDCalculations(data_provider, output).run_project_calculations()
