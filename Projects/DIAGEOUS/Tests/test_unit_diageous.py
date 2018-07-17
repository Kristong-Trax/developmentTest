from Trax.Utils.Testing.Case import TestCase
from mock import MagicMock, mock
import Projects.DIAGEOUS.Tests.test_data as Data
from Projects.DIAGEOUS.Utils.KPIToolBox import DIAGEOUSToolBox


__author__ = 'yoava'


class DIAGEOUSTestDiageous(TestCase):

    @mock.patch('Projects.DIAGEOUS.Utils.KPIToolBox.ProjectConnector')
    def setUp(self, x):
        self.data_provider_mock = MagicMock()
        self.data_provider_mock.project_name = 'diageous'
        self.data_provider_mock.rds_conn = MagicMock()
        self.output = MagicMock()
        self.data = Data
        self.tool_box = DIAGEOUSToolBox(self.data_provider_mock, self.output)


# if __name__ == '__main__':
#     tests = DIAGEOUSTestDiageous()
