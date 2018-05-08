
from mock import patch ,Mock
from unittest import TestCase

from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Utils.Conf.Configuration import Config
from Trax.Utils.Logging.Logger import Log
from Projects.DIAGEOUK_SAND.KPIGenerator import DIAGEOUK_SANDGenerator

from Projects.DIAGEOUK_SAND.Calculations import DIAGEOUK_SANDCalculations
from Projects.DIAGEOUK_SAND.Utils import KPIToolBox
from Trax.Apps.Services.KEngine.Tests.test_k_engine_base import TestKEngine

class TestDiageo(TestKEngine):

    # def test_commit(self):
    #     Mock_KPIToolBox = Mock(KPIToolBox.DIAGEOUK_SANDToolBox)
    #     Mock_KPIToolBox.commit_results_data.return_value = True
    #     assert Mock_KPIToolBox.commit_results_data() == True


    @property
    def config_file_path(self):
        return '/home/Ilan/dev/theGarageForPS/Trax/Apps/Services/KEngine/k-engine-prod.config'

    def test_calcoulation_db_moock(self):
        project_name = 'diageouk-sand'
        data_provider = KEngineDataProvider(project_name)
        session = '2F430E55-FD57-48D6-8BFA-2780BC7A7FD6'
        data_provider.load_session_data(session)
        output = Output()
    #     DIAGEOUK_SANDCalculations(data_provider, output).run_project_calculations()

# if __name__ == '__main__':
#     TestDiageo()
