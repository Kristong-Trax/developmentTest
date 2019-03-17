
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Projects.PEPSICOUK.Calculations import Calculations


if __name__ == '__main__':
    LoggerInitializer.init('pepsicouk calculations')
    Config.init()
    # project_name = 'pepsicouk'
    project_name = 'diageous-sand'
    data_provider = KEngineDataProvider(project_name)
    session = 'BA496E48-9C2F-4C92-81EB-1203CC7B0B83'
    data_provider.load_session_data(session)
    output = Output()
    Calculations(data_provider, output).run_project_calculations()
