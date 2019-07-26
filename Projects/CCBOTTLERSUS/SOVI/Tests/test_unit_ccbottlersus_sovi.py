# -*- coding: utf-8 -*-
import random
import os
import pandas as pd
from Trax.Utils.Testing.Case import TestUnitCase
from mock import MagicMock
from Projects.CCBOTTLERSUS.SOVI.KPIToolBox import SOVIToolBox
from Tests.TestUtils import remove_cache_and_storage
from Projects.CCBOTTLERSUS.SOVI.Tests.Data import records
from Projects.CCBOTTLERSUS.SOVI.Tests.Data import expected_result

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

        self.common_get_kpi_fk_by_kpi_type = self.mock_object(
            'Common.get_kpi_fk_by_kpi_type', path='KPIUtils_v2.DB.CommonV2')

        self.common_get_kpi_fk_by_kpi_type.return_value = 3000

        data_provider_dict = {'store_fk': 224259,
                              'visit_date': '2019-5-21',
                              'products': pd.DataFrame(records.products),
                              'all_products': pd.DataFrame(records.all_products),
                              'matches': pd.DataFrame(records.matches),
                              'session_info': pd.DataFrame(records.session_info),
                              'scenes_info': pd.DataFrame(records.scenes_info),
                              'store_info': pd.DataFrame(records.store_info),
                              'scene_item_facts': pd.DataFrame(records.scene_item_facts)
                              }

        # mock 'data provider' object giving to the toolbox
        self.data_provider_mock = MagicMock()
        # making data_provider_mock behave like a dict
        self.data_provider_mock.__getitem__.side_effect = data_provider_dict.__getitem__
        self.data_provider_mock.__iter__.side_effect = data_provider_dict.__iter__
        self.data_provider_mock.project_name = 'CCBOTTLERSUS'
        self.data_provider_mock.session_uid = '019B32A0-FE74-43A3-B375-796CCBD797CB'
        self.output_mock = MagicMock()

        self.toolbox = SOVIToolBox(self.data_provider_mock, self.output_mock, self.common_mock)

    @staticmethod
    def sum_results(kpi_results):
        """
        sum all the actual_results gotten by the KPI and return the result of the sum
        """
        sum_results = 0
        for i in range(0, len(kpi_results)):
            sum_results = sum_results + float(kpi_results[i][2]['result'])
        return sum_results

    @staticmethod
    def sum_results_list(input_list):
        """
        sum all actual results
        :param input_list: a list contains lists
        :return: the sum of the lest value of every list (result)
        """
        sum_results = 0.0
        for l in input_list:
            sum_results += l[-1]
        return sum_results

    @staticmethod
    def convert_results_to_dict(kpi_results):
        """
        convert a sample of actual results to a dictionary
        :param kpi_results: the mock_calls actual results
        :return: a dictionary contains the sampled result
        """
        d = {}
        i = 0
        d['numerator_id'] = kpi_results[i][2]['numerator_id']
        d['numerator_result'] = kpi_results[i][2]['numerator_result']
        d['result'] = kpi_results[i][2]['result']
        d['denominator_id'] = kpi_results[i][2]['denominator_id']
        d['denominator_result'] = kpi_results[i][2]['denominator_result']
        d['identifier_parent'] = kpi_results[i][2]['identifier_parent']
        d['should_enter'] = kpi_results[i][2]['should_enter']
        return d

    def test_calculate_product_name_sos(self):
        """
            calculate a result sample for the current KPI with a giving input
        """
        self.toolbox.common.write_to_db_result = MagicMock()
        self.toolbox.calculate_product_name_sos(u'Display', u'Still', u'Water', u'CCNA', u'SMARTWATER',
                                                u'Glaceau Smart Water 1 ltr bottle', 5)
        expected_results = {'numerator_id': 3340,
                            'numerator_result': 21.0, 'result': 7.42,
                            'denominator_id': 83, 'denominator_result': 283.0,
                            'identifier_parent': 5, 'should_enter': True}
        kpi_results = self.toolbox.common.write_to_db_result.mock_calls
        actual_results = self.convert_results_to_dict(kpi_results)
        self.assertEqual(expected_results, actual_results)

    def test_calculate_brand_sos(self):
        """
            calculate a result sample for the current KPI with a giving input
        """
        self.toolbox.common.write_to_db_result = MagicMock()
        self.toolbox.calculate_brand_sos(u'Display', u'Still', u'Water', u'CCNA', u'SMARTWATER', 4)
        expected_results = {'denominator_id': 1, 'denominator_result': 283.0, 'identifier_parent': 4,
                            'numerator_id': 83, 'numerator_result': 50.0,
                            'result': 17.67, 'should_enter': True}
        kpi_results = self.toolbox.common.write_to_db_result.mock_calls
        actual_results = self.convert_results_to_dict(kpi_results)
        self.assertEqual(expected_results, actual_results)

    def test_calculate_manufacturer_sos(self):
        """
            calculate a result sample for the current KPI with a giving input
        """
        self.toolbox.common.write_to_db_result = MagicMock()
        self.toolbox.calculate_manufacturer_sos(u'Display', u'Still', u'Water', u'Otsuka Holdings', 3)
        expected_results = {'denominator_id': 2, 'denominator_result': 283.0, 'identifier_parent': 3,
                            'numerator_id': 3, 'numerator_result': 4.0,
                            'result': 1.41, 'should_enter': True}
        kpi_results = self.toolbox.common.write_to_db_result.mock_calls
        actual_results = self.convert_results_to_dict(kpi_results)
        self.assertEqual(expected_results, actual_results)

    def test_calculate_category_sos(self):
        """
            calculate a result sample for the current KPI with a giving input
        """
        self.toolbox.common.write_to_db_result = MagicMock()
        self.toolbox.calculate_category_sos(u'Display', u'Still', u'Tea', 2, 21)
        expected_results = {'denominator_id': 2, 'denominator_result': 7888.0, 'identifier_parent': 2,
                            'numerator_id': 12, 'numerator_result': 255.0,
                            'result': 3.23, 'should_enter': True}
        kpi_results = self.toolbox.common.write_to_db_result.mock_calls
        actual_results = self.convert_results_to_dict(kpi_results)
        self.assertEqual(expected_results, actual_results)

    def test_calculate_att4_sos(self):
        """
            calculate a result sample for the current KPI with a giving input
        """
        self.toolbox.common.write_to_db_result = MagicMock()
        self.toolbox.calculate_att4_sos(u'Display', u'SSD', 1)
        expected_results = {'denominator_id': 21, 'denominator_result': 7888.0, 'identifier_parent': 1,
                            'numerator_id': 1, 'numerator_result': 1500.0,
                            'result': 19.02, 'should_enter': True}
        kpi_results = self.toolbox.common.write_to_db_result.mock_calls
        actual_results = self.convert_results_to_dict(kpi_results)
        self.assertEqual(expected_results, actual_results)

    def test_calculate_template_group_sos(self):
        """
            calculate a result sample for the current KPI with a giving input
        """
        self.toolbox.common.write_to_db_result = MagicMock()
        self.toolbox.calculate_template_group_sos(u'Cold', 0)
        expected_results = {'denominator_id': 224259, 'denominator_result': 7888.0, 'identifier_parent': 0,
                            'numerator_id': 20, 'numerator_result': 183.0,
                            'result': 2.32, 'should_enter': True}
        kpi_results = self.toolbox.common.write_to_db_result.mock_calls
        actual_results = self.convert_results_to_dict(kpi_results)
        self.assertEqual(expected_results, actual_results)

    def test_calculate_entire_store_sos(self):
        """
            calculate the sum of all the result for the current KPI with a giving input of
            session_uid: '019B32A0-FE74-43A3-B375-796CCBD797CB'
        """
        self.toolbox.common.write_to_db_result = MagicMock()
        self.toolbox.calculate_entire_store_sos()
        kpi_results = self.toolbox.common.write_to_db_result.mock_calls
        expected_results = self.sum_results_list(expected_result.expect)
        actual_results = self.sum_results(kpi_results)
        self.assertEqual(expected_results, actual_results)

