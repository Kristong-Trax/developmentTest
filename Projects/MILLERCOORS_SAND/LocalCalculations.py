
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Projects.MILLERCOORS_SAND.Calculations import Calculations


if __name__ == '__main__':
    LoggerInitializer.init('millercoors-sand calculations')
    Config.init()
    project_name = 'rinielsenus'
    # session = '079a7f14-5bfd-4e5a-ab1d-530dffb568ac'
    sessions = [
        '6eb387c9-d6ff-4e45-b0d1-55f097de6c69',
        '87ffa0aa-447f-4548-8de0-e5dfc7ff7670',
        '849dc2e9-9684-4aa3-b6c9-f87d90a252e3',
        '4a2a0734-4867-4aa2-bcf9-110abcb166d3',
        'ef286395-6f62-4f54-89b9-bc6057ae5f4b'
    ]
    for session in sessions:
        print('*******************************************************************')
        print('--------------{}-------------'.format(session))
        data_provider = KEngineDataProvider(project_name)
        data_provider.load_session_data(session)
        output = Output()
        Calculations(data_provider, output).run_project_calculations()
