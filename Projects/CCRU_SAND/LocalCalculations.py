from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Trax.Utils.Conf.Configuration import Config

from Projects.CCRU_SAND.Calculations import CCRU_SANDCalculations


if __name__ == '__main__':
    LoggerInitializer.init('CCRU calculations')
    Config.init()
    project_name = 'ccru-sand'
    data_provider = KEngineDataProvider(project_name)
    session_uids = [
        # 'FFF338AF-9505-4354-A2B7-835BF78090F3',
        'FF81168A-C990-40C2-943D-F68FC9AAACA2',
        'ffff2c80-697c-4e61-996e-893142806c55',
        'FFF99139-8B36-4272-92CE-E3A9696B819C',
        'ffd035af-1396-4b79-a7ac-25210dc8f995',
        'fe7080f5-3969-4005-a890-3afe5ffe6960',
        'FC8D623A-0DA1-4305-9DBF-9D2CC1301F38',
        'FE6D7CDF-161E-46BE-91A3-28B300ED35B0',
        'F66CE76B-4B52-4E4D-870D-0A1B0EFCEB17',
        'FA6E6AC6-C606-4195-B28C-932B07A289E5',
        'FEBEF96B-C8F9-47E4-BDD3-5087D4AB7121',
    ]
    for session in session_uids:
        data_provider.load_session_data(session)
        output = Output()
        CCRU_SANDCalculations(data_provider, output).run_project_calculations()
