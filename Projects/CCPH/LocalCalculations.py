
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Projects.CCPH.Calculations import Calculations


if __name__ == '__main__':
    LoggerInitializer.init('ccphl calculations')
    Config.init()
    project_name = 'ccphl'
    data_provider = KEngineDataProvider(project_name)
    #session = 'bc8e0033-683f-4927-b6ce-6ea88c76b10d'
    session = '912D33FA-9DB8-4C4D-ABF8-8925FEBE7E44'
    data_provider.load_session_data(session)
    output = Output()
    Calculations(data_provider, output).run_project_calculations()
