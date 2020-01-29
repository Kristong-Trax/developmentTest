
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Projects.JRIJP.Calculations import Calculations


if __name__ == '__main__':
    LoggerInitializer.init('jrijp calculations')
    Config.init()
    project_name = 'jrijp'
    data_provider = KEngineDataProvider(project_name)

    sessions = [
        "348CBAC0-0995-475B-B84A-E8C8D3229295"
    ]

    for session in sessions:
        print "Running for {}".format(session)
        data_provider.load_session_data(session)
        output = Output()
        Calculations(data_provider, output).run_project_calculations()
