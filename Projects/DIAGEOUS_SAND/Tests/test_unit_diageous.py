from Trax.Utils.Testing.Case import TestCase
from mock import MagicMock, mock
import Projects.DIAGEOUS_SAND.Tests.test_data as Data
from Projects.DIAGEOUS_SAND.Utils.KPIToolBox import DIAGEOUSSANDToolBox


__author__ = 'yoava'


class DIAGEOUS_SANDTestDiageous(TestCase):

    @mock.patch('Projects.DIAGEOUS_SAND.Utils.KPIToolBox.ProjectConnector')
    def setUp(self, x):
        self.data_provider_mock = MagicMock()
        self.data_provider_mock.project_name = 'diageous_sand'
        self.data_provider_mock.rds_conn = MagicMock()
        self.output = MagicMock()
        self.data = Data
        self.tool_box = DIAGEOUSSANDToolBox(self.data_provider_mock, self.output)


# if __name__ == '__main__':
#     tests = DIAGEOUS_SANDTestDiageous()
