from datetime import datetime
import os
import pandas as pd

from Trax.Algo.Calculations.Core.DataProvider import Data
from Projects.GOOGLEKR_SAND.Utils.Const import Const
from KPIUtils.GlobalDataProvider.PsDataProvider import PsDataProvider

__author__ = 'Eli_Sam_Shivi'

FIXTURE_TEMPLATE_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)), '../Data',
                                     'KR - Google Fixture Targets Test.csv')


class GOOGLEToolBox:

    def __init__(self, data_provider, output, common_v2):
        self.common_v2 = common_v2
        self.output = output
        self.data_provider = data_provider
        self.project_name = self.data_provider.project_name
        self.session_uid = self.data_provider.session_uid
        self.templates = self.data_provider[Data.TEMPLATES]
        self.products = self.data_provider[Data.PRODUCTS]
        self.all_products = self.data_provider[Data.ALL_PRODUCTS]
        self.match_product_in_scene = self.data_provider[Data.MATCHES]
        self.visit_date = self.data_provider[Data.VISIT_DATE]
        self.session_info = self.data_provider[Data.SESSION_INFO]
        self.scene_info = self.data_provider[Data.SCENES_INFO]
        self.store_id = self.data_provider[Data.STORE_FK]
        self.scif = self.data_provider[Data.SCENE_ITEM_FACTS]
        self.ps_data_provider = PsDataProvider(self.data_provider, self.output)
        self.all_products = self.ps_data_provider.get_sub_category(self.all_products)
        self.store_assortment = self.ps_data_provider.get_store_assortment()
        self.store_sos_policies = self.ps_data_provider.get_store_policies()
        self.labels = self.ps_data_provider.get_labels()
        self.store_info = self.data_provider[Data.STORE_INFO]
        self.store_info = self.ps_data_provider.get_ps_store_info(self.store_info)
        self.fixture_template = pd.read_csv(FIXTURE_TEMPLATE_PATH)
        self.current_date = datetime.now()

    def main_calculation(self, *args, **kwargs):
        """
        This function calculates the KPI results.
        """
        score = 0
        return score

    @staticmethod
    def filter_df(df, exclude=0):
        for exclude_filter in Const.EXCLUDE_FILTERS:
            if exclude:
                df = df[df['product_type'].isin(Const.EXCLUDE_FILTERS[exclude_filter])]
            else:
                df = df[~df['product_type'].isin(Const.EXCLUDE_FILTERS[exclude_filter])]
        return df

    @staticmethod
    def division(num, den):
        if den:
            ratio = float(num) / den
        else:
            ratio = 0
        return ratio

    def google_global_SOS(self):
        scif = self.filter_df(self.scif.copy())
        brands_scif = scif[~scif[Const.BRAND].isin(Const.NOTABRAND)]
        if brands_scif.empty:
            return

        Const.SOS_KPIs['SOS BRAND out of SCENE']['den'] = scif[Const.FACINGS].sum()
        Const.SOS_KPIs['SOS BRAND out of BRANDS in SCENE']['den'] = brands_scif[Const.FACINGS].sum()
        brand_totals = brands_scif.set_index(['brand_fk', Const.BRAND])\
                                  .groupby([Const.BRAND, 'brand_fk'])[Const.FACINGS]\
                                  .sum()

        for (brand_name, brand_fk), numerator in brand_totals.iteritems():
            for kpi in Const.SOS_KPIs:
                ratio = self.division(numerator, Const.SOS_KPIs[kpi]['den'])
                kpi_fk = self.common_v2.get_kpi_fk_by_kpi_name(kpi)
                self.common_v2.write_to_db_result(
                    fk=kpi_fk, numerator_id=brand_fk, numerator_result=numerator, result=ratio*100, by_scene=True,
                    denominator_id=self.common_v2.scene_id, denominator_result=Const.SOS_KPIs[kpi]['den'])

    def google_global_fixture_compliance(self):
        store_num = self.store_info['store_number_1'][0]
        relevant_fixtures = self.fixture_template[self.fixture_template['Store Number'] == store_num]
        relevant_fixtures = relevant_fixtures.set_index('New Task Name (unique)')\
                                             .groupby('New Task Name (unique)')\
                                             ['Number of Fixtures(Task)'].sum()

        for fixture, denominator in relevant_fixtures.iteritems():
            fixture_pk = self.templates.set_index(['template_name']).loc[fixture, 'template_fk']
            numerator = self.scene_info[self.scene_info['template_fk'] == fixture_pk].shape[0]
            ratio = self.division(numerator, denominator)
            score = 0
            if ratio >= 1:
                ratio = 1
                score = 1

            kpi_fk = self.common_v2.get_kpi_fk_by_kpi_name('FIXTURE COMPLIANCE')
            self.common_v2.write_to_db_result(
                fk=kpi_fk, numerator_id=fixture_pk, numerator_result=numerator, denominator_id=fixture_pk,
                denominator_result=denominator, score=score, result=ratio*100)

    def google_global_survey(self):
        'No Mock Survey Data Yet'
        pass

    def get_planogram_details(self):
        match_planogram_in_probe = {}
        match_planogram_in_scene = {}
        planogram_products = []
        denominator = match_planogram_in_probe[match_planogram_in_probe['product_fk'].isin(planogram_products)]
        numerator = match_planogram_in_scene[match_planogram_in_scene['compiance_status_fk'] == 3]
        return numerator, denominator

    def get_visit_osa(self):
        list_of_products = []
