from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Projects.MARSRU_PROD.Calculations import MARSRU_PRODCalculations


if __name__ == '__main__':
    Config.init()
    LoggerInitializer.init('MARSRU local calculations')
    project_name = 'marsru-prod'
    session_uids = [

        'fffe707c-55f6-4f57-b8d1-67c76638f654',
        'ffcd785d-c621-4f15-b202-581b8f73c13f',

    ]
    data_provider = KEngineDataProvider(project_name)
    output = Output()
    for session in session_uids:
        print session
        data_provider.load_session_data(session)
        MARSRU_PRODCalculations(data_provider, output).run_project_calculations()
    #
    # data_provider = KEngineSessionDataProviderLive(project_name, None, None)
    # data_provider.load_session_data('4647a9e5-84c6-4c77-9a2f-11e62000f70a', [])
    # output = Output()  # calling live calculation (live data provider)
    # CalculateKpi(data_provider, output).calculate_session_live_kpi()
