from Trax.Algo.Calculations.Core.LiveSessionDataProvider import KEngineSessionDataProviderLive
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Projects.DIAGEOPL.LiveSessionKpis.Calculation import CalculateKpi

if __name__ == '__main__':
    Config.init()
    LoggerInitializer.init('KEngine')
    project_name = 'diageopl'
    session_uids = [

        'fff1f912-b88a-4950-a845-08dba345ae48'
    ]
    data_provider = KEngineSessionDataProviderLive(project_name, None, None)
    output = None
    for session in session_uids:
        print session
        data_provider.load_session_data(session, [])
        CalculateKpi(data_provider, output).calculate_session_live_kpi()

