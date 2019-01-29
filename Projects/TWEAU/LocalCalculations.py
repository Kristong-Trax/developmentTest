
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Projects.TWEAU.Calculations import Calculations


if __name__ == '__main__':
    LoggerInitializer.init('twegau calculations')
    Config.init()
    project_name = 'twegau'
    data_provider = KEngineDataProvider(project_name)
    sessions = ['CD88A798-0F99-4E7C-B077-5F0238AD9754', '']
    for session in sessions:
        data_provider.load_session_data(session)
        output = Output()
        Calculations(data_provider, output).run_project_calculations()
