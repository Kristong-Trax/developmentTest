from Trax.Algo.Calculations.Core.LiveSessionDataProvider import KEngineSessionDataProviderLive
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Projects.MARSRU_PROD.LiveSessionKpis.Calculation import CalculateKpi

if __name__ == '__main__':
    Config.init()
    LoggerInitializer.init('MARSRU Live local calculations')
    project_name = 'marsru-prod'
    session_uids = [

        '50a37969-5b4d-4787-8cd3-4125e78e9038',
        '559b9f05-7d28-4aa0-8ca5-4a65a1833ef7',
        'df19c937-8216-435c-a5a9-563b40fa5c0f',
        'e9eef7bc-582a-49db-a5eb-51a0ae30c4fb'
    ]
    data_provider = KEngineSessionDataProviderLive(project_name, None, None)
    output = None
    for session in session_uids:
        print session
        data_provider.load_session_data(session, [])
        CalculateKpi(data_provider, output).calculate_session_live_kpi()

