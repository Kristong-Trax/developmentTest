from DebuggingToolsSDK.Singleton import OnlyOne
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Trax.Utils.Conf.Configuration import Config
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output

from Projects.HEINZCR_SAND.Calculations import HEINZCRCalculations


if __name__ == '__main__':
    LoggerInitializer.init('heinzcr-sand calculations')
    Config.init()
    project_name = 'heinzcr-sand'
    data_provider = KEngineDataProvider(project_name)
    singleton = OnlyOne()
    # singleton.is_active = True

    sessions = ['7D2A86BC-CC37-42EB-B79D-06A48FD7FEF7']
    for session in sessions:
        data_provider.load_session_data(session)
        output = Output()
        HEINZCRCalculations(data_provider, output).run_project_calculations()

    print "a"