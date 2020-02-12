
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Trax.Algo.Calculations.Core.LiveSessionDataProvider import KEngineSessionDataProviderLive
from Projects.PNGJPPLAN.LiveSessionKpis.Calculation import CalculateKpi


if __name__ == '__main__':
    LoggerInitializer.init('pngjpplan calculations')
    Config.init()
    project_name = 'pngjpplan'
    session_uid = '8711C843-E71F-4B2E-A480-FDAE2EA1521A'
    data_provider = KEngineSessionDataProviderLive(project_name, None, None)
    data_provider.load_session_data(session_uid, [])
    output = Output()
    CalculateKpi(data_provider, output).calculate_session_live_kpi()