from Trax.Utils.Testing.Case import TestCase
from mock import MagicMock, mock
import Projects.INBEVTRADMX_SAND.Tests.test_data as Data
from Projects.INBEVTRADMX_SAND.Utils.KPIToolBox import INBEVTRADMXToolBox


__author__ = 'yoava'


class TestInbevTradmxSand(TestCase):

    @mock.patch('Projects.RBUS_SAND.Utils.KPIToolBox.ProjectConnector')
    def setUp(self, x):
        self.data_provider_mock = MagicMock()
        self.data_provider_mock.project_name = 'inbevtradmx-sand'
        self.data_provider_mock.rds_conn = MagicMock()
        self.output = MagicMock()
        # self.test_objects = TestRbusSand()
        # self.calc = RBUSCalculations(self.data_provider_mock, self.output)
        # self.generator = RBUSGenerator(self.data_provider_mock, self.output)
        # self.tool = RBUSToolBox(self.data_provider_mock, self.output)
        self.data = Data
        self.tool_box = INBEVTRADMXToolBox(self.data_provider_mock, self.output)
        # self.checks = Checks(self.data_provider_mock)


# if __name__ == '__main__':
#     tests = TestInbevTradmxSand()
