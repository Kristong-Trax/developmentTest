
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Projects.BATAU.Calculations import Calculations


if __name__ == '__main__':
    LoggerInitializer.init('batau custom kpi calculations')
    Config.init()
    project_name = 'batau'
    data_provider = KEngineDataProvider(project_name)
    session = '1707F91D-E4A6-401B-86C8-E04AA942BCA8'
    #session ='EF7A4F7A-4C99-4179-8886-BA95DCF22F0C'
    data_provider.load_session_data(session)
    output = Output()
    Calculations(data_provider, output).run_project_calculations()
