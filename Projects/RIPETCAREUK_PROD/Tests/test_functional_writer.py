import os
import pandas as pd
from datetime import date, datetime
from pandas.util.testing import assert_frame_equal
from Trax.Cloud.Services.Connector.Keys import DbUsers
from Trax.Data.Projects.ProjectConnector import AwsProjectConnector
from Trax.Data.Testing.Seed import Seeder
from Trax.Utils.Testing.Case import TestCase
from mock import patch

from Projects.RIPETCAREUK_PROD.Exceptions import AtomicKpiNotInStaticException, KpiNotInStaticException, \
    KpiSetNotInStaticException
from Projects.RIPETCAREUK_PROD.Tests.data_test_functional_marsuk import DataTestMarsuk
from Projects.RIPETCAREUK_PROD.Tools.UpdataStaticData import UpdateStaticData
from Projects.RIPETCAREUK_PROD.Utils.Writer import KpiResultsWriter


class TestMarsuk(TestCase):

    seeder = Seeder()
    config_file = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'k-engine-test.config')
    mock_path = 'Projects.RIPETCAREUK_PROD.Utils.Writer'

    def setUp(self):
        super(TestCase, self).setUp()
        self.patchers = []
        self.mock_template_path()
        self.mock_utc_now()

    def tearDown(self):
        super(TestCase, self).tearDown()
        for patcher in self.patchers:
            patcher.stop()

    def mock_utc_now(self):
        patcher = patch('{}.datetime'.format(self.mock_path))
        mock = patcher.start()
        mock.utcnow.return_value = datetime(2017, 5, 31, 18, 55, 30)
        self.patchers.append(patcher)

    def mock_template_path(self):
        template_patcher = patch('Projects.RIPETCAREUK_PROD.Utils.ParseTemplates.TEMPLATE_NAME',
                                 os.path.join(os.path.dirname(os.path.realpath(__file__)), 'Test_Template.xlsx'))
        template_patcher.start()
        self.patchers.append(template_patcher)

    @staticmethod
    def _get_kpk_results():
        rds_conn = AwsProjectConnector('test_project_1', DbUsers.ReadOnly)
        query = 'select * from report.kpk_results;'
        return pd.read_sql_query(query, rds_conn.db)

    @staticmethod
    def _get_kpi_results():
        rds_conn = AwsProjectConnector('test_project_1', DbUsers.ReadOnly)
        query = 'select * from report.kpi_results;'
        return pd.read_sql_query(query, rds_conn.db)

    @staticmethod
    def _get_kps_results():
        rds_conn = AwsProjectConnector('test_project_1', DbUsers.ReadOnly)
        query = 'select * from report.kps_results;'
        return pd.read_sql_query(query, rds_conn.db)

    @seeder.seed(["sql_seed"], DataTestMarsuk())
    def test_writer_level_1_results(self):
        writer = self.initialize_writer()
        writer.write_to_db_level_1_result('ASSORTMENT SCORE', 50)
        writer.write_to_db_level_1_result('PERFECT STORE', 50)
        writer.commit_results_data()
        kps_results = self._get_kps_results()
        assert_frame_equal(
            kps_results,
            pd.DataFrame(columns=['session_uid', 'store_fk', 'visit_date', 'kpi_set_fk', 'kps_name',
                                  'kps_result', 'score_1', 'score_2', 'score_3', 'review_status_fk'],
                         data={
                             'kpi_set_fk': [7, 6],
                             'kps_name': [u'ASSORTMENT SCORE', u'PERFECT STORE'],
                             'kps_result': [None, None],
                             'review_status_fk': [None, None],
                             'score_1': [u'50.0', u'50.0'],
                             'score_2': [None, None],
                             'score_3': [None, None],
                             'session_uid': [u'2459490b-8333-41fc-be40-3ac046dfb885',
                                             u'2459490b-8333-41fc-be40-3ac046dfb885'],
                             'store_fk': [1365, 1365],
                             'visit_date': [date(2017, 5, 23), date(2017, 5, 23)]
                         }))
        with self.assertRaises(KpiSetNotInStaticException):
            writer.write_to_db_level_1_result('FAKE', 50)

    @seeder.seed(["sql_seed"], DataTestMarsuk())
    def test_writer_level_2_results(self):
        writer = self.initialize_writer()
        writer.write_to_db_level_2_result(set_name='PERFECT STORE', kpi_name='ASSORTMENT SCORE', score=50)
        writer.write_to_db_level_2_result(set_name='ASSORTMENT SCORE', kpi_name='Sheba', score=55)
        writer.commit_results_data()
        kpk_results = self._get_kpk_results()
        assert_frame_equal(
            kpk_results,
            pd.DataFrame(columns=['pk', 'session_uid', 'store_fk', 'visit_date', 'kpi_fk', 'kpk_name', 'score',
                                  'score_2', 'score_3', 'presentation_order'],
                         data={'kpi_fk': [95, 96],
                               'kpk_name': [u'ASSORTMENT SCORE', u'Sheba'],
                               'pk': [1, 2],
                               'presentation_order': [None, None],
                               'score': [50, 55],
                               'score_2': [None, None],
                               'score_3': [None, None],
                               'session_uid': [u'2459490b-8333-41fc-be40-3ac046dfb885',
                                               u'2459490b-8333-41fc-be40-3ac046dfb885'],
                               'store_fk': [1365, 1365],
                               'visit_date': [date(2017, 5, 23), date(2017, 5, 23)]}
                         ))
        with self.assertRaises(KpiNotInStaticException):
            writer.write_to_db_level_2_result(set_name='FAKE', kpi_name='ASSORTMENT SCORE', score=50)
        with self.assertRaises(KpiNotInStaticException):
            writer.write_to_db_level_2_result(set_name='PERFECT STORE', kpi_name='FAKE', score=50)

    @seeder.seed(["sql_seed"], DataTestMarsuk())
    def test_writer_level_3_results(self):
        writer = self.initialize_writer()
        writer.write_to_db_level_3_result(kpi_set_name='ASSORTMENT SCORE', kpi_name='Sheba',
                                          atomic_kpi_name='SHEBA FRESH CHOICE FISH SELECTION IN GRAVY 50G X6,',
                                          score=50, threshold=4, result=1)
        writer.write_to_db_level_3_result(kpi_set_name='ASSORTMENT SCORE', kpi_name='Sheba',
                                          atomic_kpi_name='SHEBA POUCH FINE FLAKES POULTRY IN JELLY 85GX12,',
                                          score=55, threshold=4, result=1)
        writer.commit_results_data()
        kpi_results = self._get_kpi_results()
        assert_frame_equal(
            kpi_results,
            pd.DataFrame(columns=['pk', 'session_uid', 'store_fk', 'visit_date', 'calculation_time', 'kps_name',
                                  'scope_value', 'missing_kpi_score', 'kps_score_display', 'kpi_fk',
                                  'kpi_presentation_order', 'kpi_weight', 'kpi_logical_operator', 'atomic_kpi_fk',
                                  'atomic_kpi_negate', 'display_text', 'atomic_kpi_presentation_order', 'result',
                                  'style', 'score', 'score_2', 'score_3', 'vs_1_facings', 'vs_2_facings', 'threshold',
                                  'threshold_2', 'threshold_3', 'result_2', 'result_3'],
                         data={
                             'atomic_kpi_fk': [465, 466],
                             'atomic_kpi_negate': [None, None],
                             'atomic_kpi_presentation_order': [None, None],
                             'calculation_time': [datetime(2017, 5, 31, 18, 55, 30),
                                                  datetime(2017, 5, 31, 18, 55, 30)],
                             'display_text': [u'SHEBA FRESH CHOICE FISH SELECTION IN GRAVY 50G X6,',
                                              u'SHEBA POUCH FINE FLAKES POULTRY IN JELLY 85GX12,'],
                             'kpi_fk': [96, 96],
                             'kpi_logical_operator': [None, None],
                             'kpi_presentation_order': [None, None],
                             'kpi_weight': [None, None],
                             'kps_name': [u'ASSORTMENT SCORE', u'ASSORTMENT SCORE'],
                             'kps_score_display': [None, None],
                             'missing_kpi_score': [None, None],
                             'pk': [1, 2],
                             'result': [u'1', u'1'],
                             'result_2': [None, None],
                             'result_3': [None, None],
                             'scope_value': [None, None],
                             'score': [50, 55],
                             'score_2': [None, None],
                             'score_3': [None, None],
                             'session_uid': [u'2459490b-8333-41fc-be40-3ac046dfb885',
                                             u'2459490b-8333-41fc-be40-3ac046dfb885'],
                             'store_fk': [1365, 1365],
                             'style': [None, None],
                             'threshold': [u'4', u'4'],
                             'threshold_2': [None, None],
                             'threshold_3': [None, None],
                             'visit_date': [date(2017, 5, 23), date(2017, 5, 23)],
                             'vs_1_facings': [None, None],
                             'vs_2_facings': [None, None]}
                         ))
        with self.assertRaises(AtomicKpiNotInStaticException):
            writer.write_to_db_level_3_result(kpi_set_name='FAKE', kpi_name='Sheba',
                                              atomic_kpi_name='SHEBA POUCH FINE FLAKES POULTRY IN JELLY 85GX12,',
                                              score=55, threshold=4, result=1)
        with self.assertRaises(AtomicKpiNotInStaticException):
            writer.write_to_db_level_3_result(kpi_set_name='ASSORTMENT SCORE', kpi_name='FAKE',
                                              atomic_kpi_name='SHEBA POUCH FINE FLAKES POULTRY IN JELLY 85GX12,',
                                              score=55, threshold=4, result=1)
        with self.assertRaises(AtomicKpiNotInStaticException):
            writer.write_to_db_level_3_result(kpi_set_name='ASSORTMENT SCORE', kpi_name='Sheba',
                                              atomic_kpi_name='FAKE',
                                              score=55, threshold=4, result=1)

    def initialize_writer(self):
        project_name = 'test_project_1'
        rds_conn = AwsProjectConnector(project_name, DbUsers.CalculationEng)
        UpdateStaticData(project_name).update_static_data()
        writer = KpiResultsWriter(session_uid='2459490b-8333-41fc-be40-3ac046dfb885',
                                  store_id=1365,
                                  visit_date=date(2017, 5, 23),
                                  rds_conn=rds_conn)
        return writer
