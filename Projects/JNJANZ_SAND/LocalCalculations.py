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
        # 'fe232496-0e59-4929-8337-13fb91c34304',
        # '0bcd304d-01af-4e27-8634-0e62505019a2',
        #'65c81d87-8495-4c60-a6cc-b40d7509e39f', #prev
        # '16a473cc-9472-47c9-b6b3-8888393a0a17', #prev
        # '064186dc-8e39-4c61-9fa3-4618baba761d'  #prev
        '91f4e691-6195-43c8-9874-304a9661abc8'
        ]
    # 'CE9BD14E-DBF1-4F82-BEE4-E1D61F0B211F'
    for session in sessions:
        data_provider.load_session_data(session)
        output = Output()
        JNJANZ_SANDCalculations(data_provider, output).run_project_calculations()
