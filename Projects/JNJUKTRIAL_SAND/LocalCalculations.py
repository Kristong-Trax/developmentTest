
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Projects.JNJUKTRIAL_SAND.Calculations import JNJUKCalculations


if __name__ == '__main__':
    LoggerInitializer.init('jnjuk calculations')
    Config.init()
    project_name = 'jnjuk'
    data_provider = KEngineDataProvider(project_name)
    session = 'b5693138-cf2b-407f-a3ec-95195e62d082'
    data_provider.load_session_data(session)
    output = Output()
    JNJUKCalculations(data_provider, output).run_project_calculations()
