
from Trax.Utils.Conf.Configuration import Config
from Trax.Utils.Testing.Case import TestUnitCase
from mock import MagicMock, Mock
import pandas as pd
from Projects.GSKJP.Utils.KPIToolBox import GSKJPToolBox
from KPIUtils.GlobalProjects.GSK.KPIGenerator import GSKGenerator
import os
from KPIUtils.GlobalProjects.GSK.Utils.KPIToolBox import GSKToolBox


__author__ = 'limorc'


class TestGSKJP(TestUnitCase):

    @property
    def import_path(self):
        return 'Projects.GSKJP.Utils.KPIToolBox'

    def set_up(self):
        self.data_provider_mock = MagicMock()
        self.data_provider_mock.project_name = 'gskjp'
        self.data_provider_mock.rds_conn = MagicMock()
        self.output = MagicMock()
        self.tool_box = GSKJPToolBox(self.data_provider_mock, MagicMock())
