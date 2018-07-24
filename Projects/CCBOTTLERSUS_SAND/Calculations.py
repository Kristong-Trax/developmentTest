
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
from Trax.Algo.Calculations.Core.DataProvider import Output, KEngineDataProvider
from Trax.Cloud.Services.Connector.Keys import DbUsers
from Trax.Utils.Conf.Configuration import Config
#from Trax.Utils.Conf.Keys import DbUsers
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from mock import MagicMock, patch

from Projects.CCBOTTLERSUS_SAND.KPIGenerator import CCBOTTLERSUS_SANDCcbottlersGenerator

__author__ = 'ortal'


class CCBOTTLERSUS_SANDCCBOTTLERSCalculations(BaseCalculationsScript):
    def run_project_calculations(self):
        self.timer.start()
        CCBOTTLERSUS_SANDCcbottlersGenerator(self.data_provider, self.output).main_function()
        self.timer.stop('KPIGenerator.run_project_calculations')


if __name__ == '__main__':
    LoggerInitializer.init('bci calculations')
    Config.init()
    project_name = 'ccbottlersus'
    data_provider = KEngineDataProvider(project_name, monitor=MagicMock())

    # supermarket
    session_uids = [
                    'A9645825-68FF-4F19-8CE3-989B7284CB6A',
    ]
    for session in session_uids:
        data_provider.load_session_data(session)
        output = Output()
        CCBOTTLERSUS_SANDCCBOTTLERSCalculations(data_provider, output).run_project_calculations()


