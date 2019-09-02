from KPIUtils.GlobalProjects.GSK.KPIGenerator import GSKGenerator
from Projects.GSKSG.Utils.KPIToolBox import GSKSGToolBox
from Trax.Utils.Conf.Configuration import Config
from KPIUtils.GlobalProjects.GSK.Utils.KPIToolBox import GSKToolBox
from mock import MagicMock, Mock
from Trax.Utils.Testing.Case import TestUnitCase

import pandas as pd
import os


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
        self.tool_box = GSKSGToolBox(self.data_provider_mock, MagicMock())
