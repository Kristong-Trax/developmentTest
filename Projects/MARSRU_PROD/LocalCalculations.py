from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Projects.MARSRU_PROD.Calculations import MARSRU_PRODCalculations


if __name__ == '__main__':
    Config.init()
    LoggerInitializer.init('KEngine')
    project_name = 'marsru-prod'
    session_uids = [

        'fffe707c-55f6-4f57-b8d1-67c76638f654',
        'ffcd785d-c621-4f15-b202-581b8f73c13f'
        # 'b28a865f-d0e7-4752-a1a7-545ef727eb93'
    ]
    data_provider = KEngineDataProvider(project_name)
    output = Output()
    for session in session_uids:
        print session
        data_provider.load_session_data(session)
        MARSRU_PRODCalculations(data_provider, output).run_project_calculations()
