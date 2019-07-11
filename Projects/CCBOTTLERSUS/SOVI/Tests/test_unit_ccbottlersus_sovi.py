# -*- coding: utf-8 -*-
import random
import os
import pandas as pd
from Trax.Utils.Testing.Case import TestUnitCase
from mock import MagicMock
from Projects.CCBOTTLERSUS.SOVI.KPIToolBox import SOVIToolBox
from Tests.TestUtils import remove_cache_and_storage

__author__ = 'avrahama'


class TestSOVI(TestUnitCase):
    @property
    def import_path(self):
        return 'Projects.CCBOTTLERSUS.SOVI.KPIToolBox'

    def set_up(self):
        """
        test data are from session: 019B32A0-FE74-43A3-B375-796CCBD797CB
        """
        super(TestSOVI, self).set_up()
        remove_cache_and_storage()

        # mock 'Common' object used in toolbox
        self.common_mock = self.mock_object(
            'Common', path='KPIUtils_v2.DB.CommonV2')

        data_provider_dict = {'store_fk': 224259,
                              'visit_date': '2019-5-21'}
        # mock 'data provider' object giving to the toolbox
        self.data_provider_mock = MagicMock()
        # making data_provider_mock behave like a dict
        self.data_provider_mock.__getitem__.side_effect = data_provider_dict.__getitem__
        self.data_provider_mock.__iter__.side_effect = data_provider_dict.__iter__
        self.data_provider_mock.project_name = 'CCBOTTLERSUS'
        self.data_provider_mock.session_uid = '019B32A0-FE74-43A3-B375-796CCBD797CB'
        self.output_mock = MagicMock()

        self.toolbox = SOVIToolBox(self.data_provider_mock, self.output_mock, self.common_mock)


    def test_test(self):
        x = 1
        pass
