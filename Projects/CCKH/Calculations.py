
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer


from Projects.CCKH.KPIGenerator import CCKHGenerator

__author__ = 'Nimrod'


class CCKHCalculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        CCKHGenerator(self.data_provider, self.output).main_function()
        self.timer.stop('KPIGenerator.run_project_calculations')


if __name__ == '__main__':
    LoggerInitializer.init('cckh calculations')
    Config.init()
    project_name = 'cckh'
    data_provider = KEngineDataProvider(project_name)
    sessions = [
        # 'FD91D37D-9ACB-43E4-A5E5-339FDB3ADDD3',
        '48226871-6525-460E-ABF5-B1A1CB16268C'
    ]
    for session in sessions:
        print("Running for session: {}".format(session))
        data_provider.load_session_data(session)
        output = Output()
        CCKHCalculations(data_provider, output).run_project_calculations()
