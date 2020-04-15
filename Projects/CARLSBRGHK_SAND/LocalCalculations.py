
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Projects.CARLSBRGHK_SAND.Calculations import Calculations


if __name__ == '__main__':
    LoggerInitializer.init('carlsberg-sand calculations')
    Config.init()
    project_name = 'carlsbrghk-sand'
    data_provider = KEngineDataProvider(project_name)
    session = 'e1b28eca-4cfb-423d-8c0e-379428e14a76'
    data_provider.load_session_data(session)
    output = Output()
    Calculations(data_provider, output).run_project_calculations()
