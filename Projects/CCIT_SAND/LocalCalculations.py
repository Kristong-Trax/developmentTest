
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Projects.CCIT_SAND.Calculations import Calculations
from Projects.CCIT_SAND.SceneCalculations import SceneCalculations


if __name__ == '__main__':
    LoggerInitializer.init('ccit-sand calculations')
    Config.init()
    project_name = 'ccit-sand'
    data_provider = KEngineDataProvider(project_name)
    session = 'FA35EE7B-1012-4EFD-9B88-0A5BD3CBFBCD'
    data_provider.load_session_data(session)
    output = Output()
    # Calculations(data_provider, output).run_project_calculations()
    SceneCalculations(data_provider).calculate_kpis()
