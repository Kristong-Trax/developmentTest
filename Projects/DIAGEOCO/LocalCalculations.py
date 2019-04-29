
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Projects.DIAGEOCO.Calculations import DIAGEOCOCalculations


if __name__ == '__main__':
    LoggerInitializer.init('diageoco calculations')
    Config.init()
    project_name = 'diageoco'
    sessions = [# 'D8A8183F-3494-44C6-B3DB-D7650B625759',
               # 'dac74549-89f5-48da-beaf-cba548b8698e',
               '0292f227-ebfb-49d5-852a-e9fb76cacb54']

    for session in sessions:
        data_provider = KEngineDataProvider(project_name)
        data_provider.load_session_data(session)
        output = Output()
        DIAGEOCOCalculations(data_provider, output).run_project_calculations()
