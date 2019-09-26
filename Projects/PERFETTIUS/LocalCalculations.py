
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Projects.PERFETTIUS.Calculations import Calculations


if __name__ == '__main__':
    LoggerInitializer.init('perfettius calculations')
    Config.init()
    project_name = 'perfettius'
    data_provider = KEngineDataProvider(project_name)
    session_list = ['fcb4313c-9b91-40ed-966f-a1d35e0099ff']
    for session in session_list:
        data_provider.load_session_data(session)
        output = Output()
        Calculations(data_provider, output).run_project_calculations()
