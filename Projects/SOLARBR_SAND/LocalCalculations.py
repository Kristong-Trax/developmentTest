
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Projects.SOLARBR_SAND.Calculations import Calculations


if __name__ == '__main__':
    LoggerInitializer.init('solarbr-sand calculations')
    Config.init()
    project_name = 'solarbr-sand'
    data_provider = KEngineDataProvider(project_name)
    session =  'a93ba740-dc13-4a49-8f14-ef9b4693c8a3'

#     sessions = ['00000000-06b7-b79b-0000-01548383645b',
# '00000000-06b7-b79b-0000-01548cbf7020',
# '00000000-06b7-b79b-0000-01548e75f3e4',
# '00000000-06b7-b79b-0000-0154900012be',
# '00000000-06b7-b79b-0000-01549038ecbe',
# '00000000-06b7-b79b-0000-015491b41cda',
# '00000000-06b7-b79b-0000-01549326a820',
# '00000000-06b7-b79b-0000-015494338fdb',
# '00000000-06b7-b79b-0000-0154b190f61e',
#     'a93ba740-dc13-4a49-8f14-ef9b4693c8a3']

    sessions = [ 'a93ba740-dc13-4a49-8f14-ef9b4693c8a3']

    for session in sessions:
        data_provider.load_session_data(session)
        output = Output()
        Calculations(data_provider, output).run_project_calculations()
