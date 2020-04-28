from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Projects.JNJANZ_SAND.Calculations import JNJANZ_SANDCalculations


if __name__ == '__main__':
    LoggerInitializer.init('JNJANZ_SAND calculations')
    Config.init()
    project_name = 'jnjanz-sand' #'JNJANZ_SAND'
    data_provider = KEngineDataProvider(project_name)
    sessions = [
        'fe232496-0e59-4929-8337-13fb91c34304',
        '0bcd304d-01af-4e27-8634-0e62505019a2',
        ]
    # 'CE9BD14E-DBF1-4F82-BEE4-E1D61F0B211F'
    for session in sessions:
        data_provider.load_session_data(session)
        output = Output()
        JNJANZ_SANDCalculations(data_provider, output).run_project_calculations()
