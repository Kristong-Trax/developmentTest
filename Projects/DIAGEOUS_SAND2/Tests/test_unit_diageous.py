from Trax.Utils.Testing.Case import TestCase
from mock import MagicMock, mock
import Projects.DIAGEOUS_SAND2.Tests.test_data as Data
from Projects.DIAGEOUS_SAND2.Utils.KPIToolBox import ToolBox


__author__ = 'yoava'


class DIAGEOUS_SANDTestDiageous(TestCase):

    @mock.patch('Projects.DIAGEOUS_SAND2.Utils.KPIToolBox.ProjectConnector')
    def setUp(self, x):
        self.data_provider_mock = MagicMock()
        self.data_provider_mock.project_name = 'diageous-sand2'
        self.data_provider_mock.rds_conn = MagicMock()
        self.output = MagicMock()
        self.data = Data
        self.tool_box = ToolBox(self.data_provider_mock, self.output)


# if __name__ == '__main__':
#     tests = DIAGEOUS_SANDTestDiageous()
