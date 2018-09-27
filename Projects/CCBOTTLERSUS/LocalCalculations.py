
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Projects.CCBOTTLERSUS.Calculations import CCBOTTLERSUSCalculations

if __name__ == '__main__':
    LoggerInitializer.init('ccbottlersus calculations')
    Config.init()
    project_name = 'ccbottlersus'
    sessions = [
        'A6330A41-30F7-4EED-8D43-D61071FAF586',
        'F5333A3F-4AEF-4FC0-8647-053FCC58EC56',
        'ff68f1e0-417a-4372-a063-d8a4075b59a4',
        'a0818b88-b6eb-40b2-b50b-19fcedc10d4a',
        '996a0da6-d30c-4b9a-a0dc-69a25ae00ab5',
        'b3f81d5a-6374-4d5f-817e-67f5f8a745d9'
    ]
    for session in sessions:
        data_provider = KEngineDataProvider(project_name)
        data_provider.load_session_data(session)
        output = Output()
        CCBOTTLERSUSCalculations(data_provider, output).run_project_calculations()
