
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Projects.CCBOTTLERSUS_SAND.Calculations import Calculations


if __name__ == '__main__':
    LoggerInitializer.init('ccbottlersus-sand calculations')
    Config.init()
    project_name = 'ccbottlersus'
    sessions = [
        # '7c5284d4-93e0-46e2-a31c-75075d2323e0', # CR&LT
        '55d5c959-cb08-477b-9f9e-0af4fa9f3795', # DRUG
        '714f5168-b9d9-4f9b-8f3e-3a0723c68253', # VALUE
    ]
    for session in sessions:
        data_provider = KEngineDataProvider(project_name)
        data_provider.load_session_data(session)
        output = Output()
        Calculations(data_provider, output).run_project_calculations()
