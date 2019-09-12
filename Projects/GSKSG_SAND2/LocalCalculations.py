
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Projects.GSKSG_SAND2.Calculations import Calculations


if __name__ == '__main__':
    LoggerInitializer.init('gsksg calculations')
    Config.init()
    project_name = 'gsksg-sand2'
    data_provider = KEngineDataProvider(project_name)
    sessions = ['0328237F-35DF-473B-A257-B5BA20B4D421', '0DF3A8B2-2747-4D44-A7EE-A72210B1B67D', 'FE849CA4-694F-4F39-8B1E-1CDA5CCF7512']
    for session in sessions:
        # session = 'FE849CA4-694F-4F39-8B1E-1CDA5CCF7512'
        # session = 'fdae28d2-6e49-45d2-b357-cd9ca7a13de6'
        data_provider.load_session_data(session)
        output = Output()
        Calculations(data_provider, output).run_project_calculations()
