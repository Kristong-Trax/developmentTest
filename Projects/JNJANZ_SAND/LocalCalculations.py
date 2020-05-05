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
        # '91f4e691-6195-43c8-9874-304a9661abc8' # prev session

        # bulk re-calc
        '09d53186-1b6d-425e-bd6b-deded5a73b2a',
        '0dab27f9-15b5-4b13-8bf6-3c453245c059',
        '1246c2aa-e157-42a8-881d-6f0706afd714',
        '16b06374-26d9-40cd-8e66-d6a22d7c6e51',
        '16ea48e7-d41f-44b1-b29f-68d586448332',

        ]
    # 'CE9BD14E-DBF1-4F82-BEE4-E1D61F0B211F'
    for session in sessions:
        data_provider.load_session_data(session)
        output = Output()
        JNJANZ_SANDCalculations(data_provider, output).run_project_calculations()
