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
        # '09d53186-1b6d-425e-bd6b-deded5a73b2a',
        # '0dab27f9-15b5-4b13-8bf6-3c453245c059',
        # '1246c2aa-e157-42a8-881d-6f0706afd714',
        # '16b06374-26d9-40cd-8e66-d6a22d7c6e51',
        # '16ea48e7-d41f-44b1-b29f-68d586448332',

        # New test sessions
        # 'b5b2469e-3487-4012-ad36-e3b90510e0e3',
        #'0ca6910d-cff7-41e0-8dc4-2aef898efcf5' # previous session - have issues

        # '4b68d2d7-1de4-4430-b9bd-f15324369adb',
        # '640bb7ff-91ba-4a67-b124-1b7fcb9d531e' #prev session

        'e7f1c2f6-e80b-4365-a0d6-b93ad8008929',
        #'4b68d2d7-1de4-4430-b9bd-f15324369adb' #prev session

        # '558802f2-f42e-4ce3-9b80-41ed3a7d0c0f',
        # 'ba2da77f-d0b9-409a-ae8a-2f357aedb844' # previous
        ]
    # 'CE9BD14E-DBF1-4F82-BEE4-E1D61F0B211F'
    for session in sessions:
        data_provider.load_session_data(session)
        output = Output()
        JNJANZ_SANDCalculations(data_provider, output).run_project_calculations()
