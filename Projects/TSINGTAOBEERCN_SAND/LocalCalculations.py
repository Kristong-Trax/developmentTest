
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Projects.TSINGTAOBEERCN_SAND.Calculations import Calculations


if __name__ == '__main__':
    LoggerInitializer.init('tsingtaobeercn-sand calculations')
    Config.init()
    project_name = 'tsingtaobeercn-sand'
    data_provider = KEngineDataProvider(project_name)
    session = '635ED507-A0CE-4DCC-8F71-1D4D39F2271C'
    data_provider.load_session_data(session)
    output = Output()
    Calculations(data_provider, output).run_project_calculations()
