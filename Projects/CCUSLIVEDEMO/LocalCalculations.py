
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
from Trax.Algo.Calculations.Core.DataProvider import Output
from Trax.Algo.Calculations.Core.LiveSessionDataProvider import KEngineSessionDataProviderLive
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Projects.CCUSLIVEDEMO.LiveSessionKpis.Calculation import CalculateKpi


__author__ = 'limorc'


class Calculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        CalculateKpi(self.data_provider).calculate_session_live_kpi()
        self.timer.stop('KPIGenerator.run_project_calculations')


if __name__ == '__main__':
    LoggerInitializer.init('ccuslivesdemo calculations')
    Config.init()
    project_name = 'expd-20191210-410-ccuslivedemo'
    session = 'dab0eebb-abbe-4bf8-a25e-2a2c1cd0b149'
    data_provider = KEngineSessionDataProviderLive(project_name, None, None)
    data_provider.load_session_data('dab0eebb-abbe-4bf8-a25e-2a2c1cd0b149', [14049604])
    output = Output()
    Calculations(data_provider, output).run_project_calculations()
