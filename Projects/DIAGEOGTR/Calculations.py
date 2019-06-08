
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer

from Projects.DIAGEOGTR.KPIGenerator import DIAGEOGTRGenerator

__author__ = 'satya'


class DIAGEOGTRCalculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        DIAGEOGTRGenerator(self.data_provider, self.output).main_function()
        self.timer.stop('KPIGenerator.run_project_calculations')


if __name__ == '__main__':
    project_name = 'diageogtr'
    LoggerInitializer.init(project_name + ' calculations')
    Config.init()
    data_provider = KEngineDataProvider(project_name)
    session = '86897df1-2ca2-4175-b543-06f025266fc2'
    # session = 'CA39A0F2-62C8-44FD-92EA-76FCB6903DC2' #price
    # session = '13c43cbd-0204-417a-8f49-665b8ba5e592'
    # session = '5EBAE27C-3ACF-4DCE-801E-B7CB0FADE272'  # Price
    # session = '38d2ac08-4f44-49be-b7c7-3fd8c86e8715'
    data_provider.load_session_data(session)
    output = Output()
    DIAGEOGTRCalculations(data_provider, output).run_project_calculations()
