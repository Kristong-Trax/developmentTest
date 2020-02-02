
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
from Trax.Algo.Calculations.Core.DataProvider import Output, KEngineDataProvider
from Trax.Algo.Calculations.Core.LiveSessionDataProvider import KEngineSessionDataProviderLive
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Projects.CCUSLIVEDEMO.LiveSessionKpis.Calculation import CalculateKpi
from Projects.CCUSLIVEDEMO.Utils.KPIToolBox import CCUSLiveDemoToolBox
__author__ = 'limorc'


class Calculations(BaseCalculationsScript):
    def run_project_calculations(self):
        """  This function is activated during regular data provider"""
        self.timer.start()
        CCUSLiveDemoToolBox(self.data_provider,self.output).main_calculation()
        self.timer.stop('KPIGenerator.run_project_calculations')
        pass


if __name__ == '__main__':
    LoggerInitializer.init('ccuslivesdemo calculations')
    Config.init()
    project_name = 'expd-20191210-410-ccuslivedemo'
    session = '0c2decfc-8d2f-4b22-b9d0-8e2e97c9557b'
    data_provider = KEngineSessionDataProviderLive(project_name, None, None)
    data_provider.load_session_data('0c2decfc-8d2f-4b22-b9d0-8e2e97c9557b', [14049102])
    # session = 'f90529ec-6ff4-4481-970a-546e0030a41b'
    # data_provider = KEngineSessionDataProviderLive(project_name, None, None)
    # data_provider.load_session_data('f90529ec-6ff4-4481-970a-546e0030a41b', [14049726])

    # data provider in server does the kpis loading automatically . locally need to add it
    # data_provider.load_kpis_hierarchy(lambda x: x.loc[(x['calculation_stage'] == 'LIVE')
    #                                                   & (x['live_session_relevance'] == 1)].copy(),
    #                                   'session')
    output = Output()

    # calling live calculation (live data provider)
    CalculateKpi(data_provider, output).calculate_session_live_kpi()

    # calling regular data provider
    reg_data_provider = KEngineDataProvider(project_name)
    reg_data_provider.load_session_data(session)
    Calculations(reg_data_provider, output).run_project_calculations()

