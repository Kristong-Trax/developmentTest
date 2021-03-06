
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer


from Projects.CCKH_SAND.KPIGenerator import CCKH_SANDGenerator

__author__ = 'Nimrod'


class CCKH_SANDCalculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        CCKH_SANDGenerator(self.data_provider, self.output).main_function()
        self.timer.stop('KPIGenerator.run_project_calculations')


if __name__ == '__main__':
    LoggerInitializer.init('cckh calculations')
    Config.init()
    project_name = 'cckh-sand'
    data_provider = KEngineDataProvider(project_name)
    # session = 'BAD33A37-A17D-493F-B5A8-112D947062D1'
    session = 'ff0d57ad-a7ed-46ca-9488-1d28f4682500'
    # session = 'f073ac15-3121-4c86-bb59-cc6dc6b6e4b5'
    data_provider.load_session_data(session)
    output = Output()
    CCKH_SANDCalculations(data_provider, output).run_project_calculations()
