
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Projects.LIONJP_SAND.Calculations import Calculations


if __name__ == '__main__':
    LoggerInitializer.init('lionjp-sand2 calculations')
    Config.init()
    project_name = 'psapac-sand2'
    data_provider = KEngineDataProvider(project_name)

    sessions = ['EAA21AFD-8FEF-4DFD-94CF-E7ADF8130DA4']

    for session in sessions:
        print "Running for {}".format(session)
        data_provider.load_session_data(session)
        output = Output()
        Calculations(data_provider, output).run_project_calculations()
