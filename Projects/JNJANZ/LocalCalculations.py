from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Projects.JNJANZ.Calculations import JNJANZCalculations


if __name__ == '__main__':
    LoggerInitializer.init('jnjanz calculations')
    Config.init()
    project_name = 'jnjanz'
    data_provider = KEngineDataProvider(project_name)
    sessions = ['1F113395-8F4D-48E2-953F-0DE401734D31']
    for session in sessions:
        data_provider.load_session_data(session)
        output = Output()
        JNJANZCalculations(data_provider, output).run_project_calculations()
