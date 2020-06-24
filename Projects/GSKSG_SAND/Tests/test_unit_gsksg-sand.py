
from Trax.Utils.Conf.Configuration import Config
from Trax.Utils.Testing.Case import TestUnitCase
from mock import MagicMock, Mock
import pandas as pd
from Projects.GSKSG_SAND.Utils.KPIToolBox import GSKSGToolBox
from KPIUtils.GlobalProjects.GSK.KPIGenerator import GSKGenerator
import os
from KPIUtils.GlobalProjects.GSK.Utils.KPIToolBox import GSKToolBox


__author__ = 'limorc'


class TestGSKSG_SAND(TestUnitCase):

    @property
    def import_path(self):
        return 'Projects.GSKSG_SAND.Utils.KPIToolBox'

    def set_up(self):
        self.data_provider_mock = MagicMock()
        self.data_provider_mock.project_name = 'gsksg-sand'
        self.data_provider_mock.rds_conn = MagicMock()
        self.output = MagicMock()
        self.tool_box = GSKSGToolBox(self.data_provider_mock, MagicMock())
