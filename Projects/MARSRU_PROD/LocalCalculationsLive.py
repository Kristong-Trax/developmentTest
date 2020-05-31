from Trax.Algo.Calculations.Core.LiveSessionDataProvider import KEngineSessionDataProviderLive
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Projects.MARSRU_PROD.LiveSessionKpis.Calculation import CalculateKpi

if __name__ == '__main__':
    Config.init()
    LoggerInitializer.init('MARSRU Live local calculations')
    project_name = 'marsru-prod'
    session_uids = [

        'e0dfe73b-e33b-4b4c-99d6-0e43c85d18c8'
    ]
    data_provider = KEngineSessionDataProviderLive(project_name, None, None)
    output = None
    for session in session_uids:
        print session
        data_provider.load_session_data(session, [])
        CalculateKpi(data_provider, output).calculate_session_live_kpi()

