
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Projects.LIONJP_SAND.Calculations import Calculations


if __name__ == '__main__':
    LoggerInitializer.init('lionjp-sand calculations')
    Config.init()
    project_name = 'lionjp-sand'
    data_provider = KEngineDataProvider(project_name)

    sessions = [
        'A225E151-3AD8-4845-90B4-ABC3B3964111',
        '831E0585-4B60-48CB-9427-86070484A72B',
        '2DA6D045-1D4E-403A-B01E-577DABE69402',
    ]
    for session in sessions:
        print "Running for {}".format(session)
        data_provider.load_session_data(session)
        output = Output()
        Calculations(data_provider, output).run_project_calculations()
