
# from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
# from Trax.Utils.Conf.Configuration import Config
# from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
# from Projects.GSKSG_SAND.Calculations import Calculations
# from Trax.Algo.Calculations.Core.LiveSessionDataProvider import KEngineSessionDataProviderLive
# from Projects.GSKSG_SAND.LiveSessionKpis.Calculation import CalculateKpi
#
#
# if __name__ == '__main__':
#     LoggerInitializer.init('gsksg-sand calculations')
#     Config.init()
#     project_name = 'gsksg-sand'
#     # data_provider = KEngineDataProvider(project_name)
#     # session = '2a3a70b4-31df-4090-9e14-3b4775fdf0a6'
#     # session = 'fdae28d2-6e49-45d2-b357-cd9ca7a13de6'
#     # data_provider.load_session_data(session)
#     # output = Output()
#     # Calculations(data_provider, output).run_project_calculations()
#
#     live_data_provider = KEngineSessionDataProviderLive(project_name, None, None)
#     live_data_provider.load_session_data('5ACFEC51-0A7B-4669-9FBB-6B88975AD3AF', [])
#     session = 'f90529ec-6ff4-4481-970a-546e0030a41b'
#     # data_provider = KEngineSessionDataProviderLive(project_name, None, None)
#     # data_provider.load_session_data('f90529ec-6ff4-4481-970a-546e0030a41b', [14049726])
#     #
#     # # data provider in server does the kpis loading automatically . locally need to add it
#     # data_provider.load_kpis_hierarchy(lambda x: x.loc[(x['calculation_stage'] == 'LIVE')
#     #                                                   & (x['live_session_relevance'] == 1)].copy(),
#     #                                   'session')
#     output = Output()
#     #
#     # # calling live calculation (live data provider)
#     CalculateKpi(live_data_provider, output).calculate_session_live_kpi()
