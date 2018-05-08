
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
# from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
# from Trax.Utils.Conf.Configuration import Config
# from Trax.Cloud.Services.Connector.Logger import LoggerInitializer


from Projects.HEINEKENCN_SAND.KPIGenerator import HEINEKENCNGenerator

__author__ = 'Yasmin'


class HEINEKENCN_SANDCalculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        HEINEKENCNGenerator(self.data_provider, self.output).main_function()
        self.timer.stop('KPIGenerator.run_project_calculations')


# if __name__ == '__main__':
#     LoggerInitializer.init('heinekencn-sand calculations')
#     Config.init()
#     project_name = 'heinekencn-sand'
#     data_provider = KEngineDataProvider(project_name)
#     session = '75F59BBB-F11D-498D-ACF5-F7273C70B9E7'
#     data_provider.load_session_data(session)
#     output = Output()
#     HEINEKENCNCalculations(data_provider, output).run_project_calculations()
