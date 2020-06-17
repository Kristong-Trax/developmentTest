
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Projects.MONDELEZUSPS.Calculations import Calculations


if __name__ == '__main__':
    LoggerInitializer.init('mondelezusps calculations')
    Config.init()
    project_name = 'mondelezusps'
    data_provider = KEngineDataProvider(project_name)
    session_list = ['a5216695-4828-48e2-971b-29cd2af42189']
    for session in session_list:
        data_provider.load_session_data(session)
        output = Output()
        Calculations(data_provider, output).run_project_calculations()
