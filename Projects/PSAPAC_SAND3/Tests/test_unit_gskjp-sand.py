from Trax.Utils.Conf.Configuration import Config
from Trax.Utils.Testing.Case import TestUnitCase
from mock import MagicMock, Mock
from Projects.GSKJP_SAND.Utils.KPIToolBox import GSKJP_SANDToolBox


__author__ = 'limorc'


class TestGSKJP_SAND(TestUnitCase):

    @property
    def import_path(self):
        return 'Projects.GSKJP_SAND.Utils.KPIToolBox'

    def set_up(self):
        self.data_provider_mock = MagicMock()
        self.data_provider_mock.project_name = 'gskjp-sand'
        self.data_provider_mock.rds_conn = MagicMock()
        self.output = MagicMock()
        self.tool_box = GSKJP_SANDToolBox(self.data_provider_mock, MagicMock())
