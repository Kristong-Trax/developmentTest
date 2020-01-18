
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
    session = '93d3455f-ee82-430a-b068-dc0298dc85d3'
    data_provider.load_session_data(session)
    output = Output()
    CCKHCalculations(data_provider, output).run_project_calculations()
