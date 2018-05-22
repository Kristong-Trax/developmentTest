from Trax.Utils.Testing.Case import TestCase
from mock import MagicMock, mock
import Projects.INBEVTRADMX.Tests.test_data as Data
from Projects.INBEVTRADMX.Utils.KPIToolBox import INBEVTRADMXToolBox


__author__ = 'yoava'


class INBEVTRADMXTestInbevTradmxSand(TestCase):

    @mock.patch('Projects.RBUS_SAND.Utils.KPIToolBox.ProjectConnector')
    def setUp(self, x):
        self.data_provider_mock = MagicMock()
        self.data_provider_mock.project_name = 'inbevtradmx-sand'
        self.data_provider_mock.rds_conn = MagicMock()
        self.output = MagicMock()
        # self.test_objects = TestRbusSand()
        # self.calc = RBUSINBEVTRADMXCalculations(self.data_provider_mock, self.output)
        # self.generator = RBUSINBEVTRADMXGenerator(self.data_provider_mock, self.output)
        # self.tool = RBUSToolBox(self.data_provider_mock, self.output)
        self.data = Data
        self.tool_box = INBEVTRADMXToolBox(self.data_provider_mock, self.output)
        # self.checks = Checks(self.data_provider_mock)


# if __name__ == '__main__':
#     tests = INBEVTRADMXTestInbevTradmxSand()
