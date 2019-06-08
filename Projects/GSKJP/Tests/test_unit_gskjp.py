
from Trax.Utils.Conf.Configuration import Config
from Trax.Utils.Testing.Case import TestCase, MockingTestCase
from mock import MagicMock, Mock
import pandas as pd
from Projects.GSKSG.Utils.KPIToolBox import GSKSGToolBox
from KPIUtils.GlobalProjects.GSK.KPIGenerator import GSKGenerator
import os
from KPIUtils.GlobalProjects.GSK.Utils.KPIToolBox import GSKToolBox


__author__ = 'limorc'


class TestGSKJP(MockingTestCase):

    @property
    def import_path(self):
        return 'Projects.GSKJP.Utils.KPIToolBox'

    def set_up(self):
        self.data_provider_mock = MagicMock()
        self.data_provider_mock.project_name = 'gskjp'
        self.data_provider_mock.rds_conn = MagicMock()
        self.output = MagicMock()
        self.tool_box = GSKSGToolBox(self.data_provider_mock, MagicMock())
