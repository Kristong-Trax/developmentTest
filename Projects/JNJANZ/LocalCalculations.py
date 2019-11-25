from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Projects.JNJANZ.Calculations import JNJANZCalculations


if __name__ == '__main__':
    LoggerInitializer.init('jnjanz calculations')
    Config.init()
    project_name = 'jnjanz'
    data_provider = KEngineDataProvider(project_name)
    session = '443782B5-44DB-47E8-B40B-E4C498DA5A4A'
    data_provider.load_session_data(session)
    output = Output()
    JNJANZCalculations(data_provider, output).run_project_calculations()
