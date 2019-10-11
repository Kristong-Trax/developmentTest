from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Trax.Utils.Conf.Configuration import Config

from Projects.CCRU.Calculations import CCRUCalculations


if __name__ == '__main__':
    LoggerInitializer.init('CCRU calculations')
    Config.init()
    project_name = 'ccru-sand'
    data_provider = KEngineDataProvider(project_name)
    session_uids = [
        'FFFF36A6-57C5-407D-8B18-9D8A3FF3E20C',
        'FFFF0222-11BC-4480-9D0A-9B0A500B98B9',
        'fffb03f5-850f-47b4-a9a7-4582ce2217c1',
        'ffdf0e81-4026-471e-91ed-04fea8c50178',

        # 'ff5a9495-b910-4ffe-b526-69f7c7348d09',
        # 'ff3daa13-0e4b-49dc-9241-55626ba7c779',
        # 'ff3ab26f-d43f-416e-a2f8-44727ddc6588',
        # 'FEFEB55D-A571-42B3-ABE4-87F887FB7A00'

        # SAND
        # '3a3e96ee-3dfa-47f2-8cdb-6226756f62f8',
        # 'ff2d747e-99d6-4aab-a8fc-7eb625ee9c34',
        # 'ffc688ee-eb0e-483c-84a5-610017a7006d'
    ]
    for session in session_uids:
        data_provider.load_session_data(session)
        output = Output()
        CCRUCalculations(data_provider, output).run_project_calculations()
