from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Projects.JNJANZ.Calculations import JNJANZCalculations


if __name__ == '__main__':
    LoggerInitializer.init('jnjanz calculations')
    Config.init()
    project_name = 'jnjanz'
    data_provider = KEngineDataProvider(project_name)
    sessions = ['CE9BD14E-DBF1-4F82-BEE4-E1D61F0B211F']
    for session in sessions:
        data_provider.load_session_data(session)
        output = Output()
        JNJANZCalculations(data_provider, output).run_project_calculations()
