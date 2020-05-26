from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Projects.MARSRU_PROD.Calculations import MARSRU_PRODCalculations


if __name__ == '__main__':
    Config.init()
    LoggerInitializer.init('MARSRU local calculations')
    project_name = 'marsru-prod'
    session_uids = [

        '470aac6c-1e9b-461f-924a-a562e5c42ff7',

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
