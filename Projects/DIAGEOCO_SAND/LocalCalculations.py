
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Projects.DIAGEOCO_SAND.Calculations import DIAGEOCO_SANDCalculations


if __name__ == '__main__':
    LoggerInitializer.init('diageoco-sand calculations')
    Config.init()
    project_name = 'diageoco-sand'
    data_provider = KEngineDataProvider(project_name)
    session = '26AB3270-1EF4-4038-BBB8-ADC590320552'
    data_provider.load_session_data(session)
    output = Output()
    DIAGEOCO_SANDCalculations(data_provider, output).run_project_calculations()
