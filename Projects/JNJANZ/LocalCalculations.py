from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Projects.JNJANZ.Calculations import JNJANZCalculations


if __name__ == '__main__':
    LoggerInitializer.init('jnjanz calculations')
    Config.init()
    project_name = 'jnjanz'
    data_provider = KEngineDataProvider(project_name)
    sessions = ['443782B5-44DB-47E8-B40B-E4C498DA5A4A'
,'455260DD-1BCA-4FF7-B8BC-ACB5A5E14977']
    for session in sessions:
        data_provider.load_session_data(session)
        output = Output()
        JNJANZCalculations(data_provider, output).run_project_calculations()
