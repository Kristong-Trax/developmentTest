
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Projects.CBCDAIRYIL_SAND.Calculations import Calculations


if __name__ == '__main__':
    LoggerInitializer.init('cbcdairyil-sand calculations')
    Config.init()
    project_name = 'cbcdairyil-sand'
    data_provider = KEngineDataProvider(project_name)
    session = '8c40ca36-5270-486f-bd78-9a8038b7dfad'
    data_provider.load_session_data(session)
    output = Output()
    Calculations(data_provider, output).run_project_calculations()
