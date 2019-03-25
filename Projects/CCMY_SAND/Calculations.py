
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer

from Projects.CCMY_SAND.KPIGenerator import CCMY_SANDGenerator

__author__ = 'Nimrod'


class CCMY_SANDCalculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        CCMY_SANDGenerator(self.data_provider, self.output).main_function()
        self.timer.stop('KPIGenerator.run_project_calculations')

if __name__ == '__main__':
    LoggerInitializer.init('ccmy-sand calculations')
    Config.init()
    project_name = 'ccmy-sand'
    data_provider = KEngineDataProvider(project_name)
    # session = '0030b950-c998-4fe5-a579-eb9263caf683'
    # session = '01a7d79c-8062-4709-bd8c-5910c7e93aed'
    # session = '531a5188-88e8-4160-9391-aba4bb342466'
    # session ='b5e13d4f-4a67-4bc4-b7db-7dc867f043b4'
    session='4689b9e7-baf5-4ceb-961e-ab4b2bc61bd5'
    data_provider.load_session_data(session)
    output = Output()
    CCMY_SANDCalculations(data_provider, output).run_project_calculations()