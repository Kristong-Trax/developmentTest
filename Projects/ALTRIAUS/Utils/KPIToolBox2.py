import pandas as pd
from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Cloud.Services.Connector.Keys import DbUsers
from Trax.Data.Projects.Connector import ProjectConnector
# from Trax.Utils.Logging.Logger import Log

from KPIUtils_v2.DB.Common import Common
from KPIUtils_v2.DB.CommonV2 import Common as CommonV2
import os
from Trax.Utils.Logging.Logger import Log
import datetime
from Projects.ALTRIAUS.Utils.AltriaDataProvider import AltriaDataProvider
from Projects.ALTRIAUS.Utils.AltriaGraphUtil import AltriaGraphBuilder
from KPIUtils_v2.GlobalDataProvider.PsDataProvider import PsDataProvider


__author__ = 'huntery'

FIXTURE_WIDTH_TEMPLATE = \
    os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', 'Data', 'Fixture_Width_Dimensions_v3.xlsx')

NO_FLIP_SIGN_PK = 11161

MATCH_PRODUCT_IN_PROBE_FK = 'match_product_in_probe_fk'
MATCH_PRODUCT_IN_PROBE_STATE_REPORTING_FK = 'match_product_in_probe_state_reporting_fk'


def log_runtime(description, log_start=False):
    def decorator(func):
        def wrapper(*args, **kwargs):
            calc_start_time = datetime.datetime.utcnow()
            if log_start:
                Log.info('{} started at {}'.format(description, calc_start_time))
            result = func(*args, **kwargs)
            calc_end_time = datetime.datetime.utcnow()
            Log.info('{} took {}'.format(description, calc_end_time - calc_start_time))
            return result

        return wrapper

    return decorator


class ALTRIAUSToolBox:
    def __init__(self, data_provider, output):
        self.output = output
        self.data_provider = data_provider
        self.common = Common(self.data_provider)
        self.common_v2 = CommonV2(self.data_provider)
        self.project_name = self.data_provider.project_name
        self.session_uid = self.data_provider.session_uid
        self.products = self.data_provider[Data.PRODUCTS]
        self.all_products = self.data_provider[Data.ALL_PRODUCTS]
        self.match_product_in_scene = self.data_provider[Data.MATCHES]
        self.visit_date = self.data_provider[Data.VISIT_DATE]
        self.session_info = self.data_provider[Data.SESSION_INFO]
        self.scene_info = self.data_provider[Data.SCENES_INFO]
        self.store_id = self.data_provider[Data.STORE_FK]
        self.scif = self.data_provider[Data.SCENE_ITEM_FACTS]
        self.template_info = self.data_provider.all_templates
        self.rds_conn = ProjectConnector(self.project_name, DbUsers.CalculationEng)
        self.ps_data_provider = PsDataProvider(self.data_provider)
        self.match_product_in_probe_state_reporting = self.ps_data_provider.get_match_product_in_probe_state_reporting()
        self.kpi_results_queries = []
        self.fixture_width_template = pd.read_excel(FIXTURE_WIDTH_TEMPLATE, "Fixture Width", dtype=pd.Int64Dtype())
        self.facings_to_feet_template = pd.read_excel(FIXTURE_WIDTH_TEMPLATE, "Conversion Table", dtype=pd.Int64Dtype())
        self.header_positions_template = pd.read_excel(FIXTURE_WIDTH_TEMPLATE, "Header Positions")
        self.flip_sign_positions_template = pd.read_excel(FIXTURE_WIDTH_TEMPLATE, "Flip Sign Positions")
        self.custom_entity_data = self.ps_data_provider.get_custom_entities(1005)
        self.ignore_stacking = False
        self.facings_field = 'facings' if not self.ignore_stacking else 'facings_ign_stack'
        self.kpi_new_static_data = self.common.get_new_kpi_static_data()
        try:
            self.mpis = self.match_product_in_scene.merge(self.products, on='product_fk', suffixes=['', '_p']) \
                        .merge(self.scene_info, on='scene_fk', suffixes=['', '_s']) \
                          .merge(self.template_info, on='template_fk', suffixes=['', '_t'])
        except KeyError:
            Log.warning('MPIS cannot be generated!')
            return
        self.adp = AltriaDataProvider(self.data_provider)
        self.active_kpis = self._get_active_kpis()
        self.external_targets = self.ps_data_provider.get_kpi_external_targets()
        self.scene_graphs = {}

    def main_calculation(self, *args, **kwargs):
        """
               This function calculates the KPI results.
               """
        self.generate_graph_per_scene()
        self.calculate_register_type()
        self.calculate_age_verification()
        self.calculate_facings_by_scene_type()

        return

    def generate_graph_per_scene(self):
        relevant_scif = self.scif[self.scif['template_name'] == 'Tobacco Merchandising Space']
        for scene in relevant_scif['scene_id'].unique().tolist():
            self.scene_graphs[scene] = AltriaGraphBuilder(self.data_provider, scene)
        return

    def _get_active_kpis(self):
        active_kpis = self.kpi_new_static_data[(self.kpi_new_static_data['kpi_calculation_stage_fk'] == 3) &
                                           (self.kpi_new_static_data['valid_from'] <= self.visit_date) &
                                           ((self.kpi_new_static_data['valid_until']).isnull() |
                                            (self.kpi_new_static_data['valid_until'] >= self.visit_date))]
        return active_kpis

    def calculate_facings_by_scene_type(self):
        kpi_name = 'FACINGS_BY_SCENE_TYPE'
        kpi_fk = self.common_v2.get_kpi_fk_by_kpi_type(kpi_name)
        if kpi_name not in self.active_kpis['type'].unique().tolist():
            return

        config = self.get_external_target_data_by_kpi_fk(kpi_fk)

        product_types = config.product_type
        template_names = config.template_name

        relevant_mpis = self.mpis[(self.mpis['product_type'].isin(product_types)) &
                                  (self.mpis['template_name'].isin(template_names))]
        smart_attribute_data = \
            self.adp.get_match_product_in_probe_state_values(relevant_mpis['probe_match_fk'].unique().tolist())

        relevant_mpis = pd.merge(relevant_mpis, smart_attribute_data, on='probe_match_fk', how='left')
        relevant_mpis['match_product_in_probe_state_fk'].fillna(0, inplace=True)

        relevant_mpis = relevant_mpis.groupby(['product_fk',
                                               'template_fk',
                                               'match_product_in_probe_state_fk'],
                                              as_index=False)['scene_match_fk'].count()
        relevant_mpis.rename(columns={'scene_match_fk': 'facings'}, inplace=True)

        for row in relevant_mpis.itertuples():
            self.common_v2.write_to_db_result(kpi_fk, numerator_id=row.product_fk, denominator_id=row.template_fk,
                                              context_id=row.match_product_in_probe_state_fk, result=row.facings)

        return

    def calculate_register_type(self):
        relevant_scif = self.scif[(self.scif['product_type'].isin(['POS', 'Other'])) &
                                  (self.scif['category'] == 'POS Machinery')]
        if relevant_scif.empty:
            result = 0
            product_fk = 0
        else:
            result = 1
            product_fk = relevant_scif['product_fk'].iloc[0]

        kpi_fk = self.common_v2.get_kpi_fk_by_kpi_type('Register Type')
        self.common_v2.write_to_db_result(kpi_fk, numerator_id=product_fk, denominator_id=self.store_id,
                                          result=result)

    def calculate_age_verification(self):
        kpi_fk = self.common_v2.get_kpi_fk_by_kpi_type('Age Verification')
        relevant_scif = self.scif[self.scif['brand_name'].isin(['Age Verification'])]

        if relevant_scif.empty:
            result = 0
            product_fk = 0

            self.common_v2.write_to_db_result(kpi_fk, numerator_id=product_fk, denominator_id=self.store_id,
                                              result=result)
        else:
            result = 1
            for product_fk in relevant_scif['product_fk'].unique().tolist():
                self.common_v2.write_to_db_result(kpi_fk, numerator_id=product_fk, denominator_id=self.store_id,
                                                  result=result)
        return

    def mark_tags_in_explorer(self, probe_match_fk_list, mpipsr_name):
        if not probe_match_fk_list:
            return
        try:
            match_type_fk = \
                self.match_product_in_probe_state_reporting[
                    self.match_product_in_probe_state_reporting['name'] == mpipsr_name][
                    'match_product_in_probe_state_reporting_fk'].values[0]
        except IndexError:
            Log.warning('Name not found in match_product_in_probe_state_reporting table: {}'.format(mpipsr_name))
            return

        match_product_in_probe_state_values_old = self.common_v2.match_product_in_probe_state_values
        match_product_in_probe_state_values_new = pd.DataFrame(columns=[MATCH_PRODUCT_IN_PROBE_FK,
                                                                        MATCH_PRODUCT_IN_PROBE_STATE_REPORTING_FK])
        match_product_in_probe_state_values_new[MATCH_PRODUCT_IN_PROBE_FK] = probe_match_fk_list
        match_product_in_probe_state_values_new[MATCH_PRODUCT_IN_PROBE_STATE_REPORTING_FK] = match_type_fk

        self.common_v2.match_product_in_probe_state_values = pd.concat([match_product_in_probe_state_values_old,
                                                                        match_product_in_probe_state_values_new])

        return

    def get_category_fk_by_name(self, category_name):
        return self.all_products[self.all_products['category'] == category_name]['category_fk'].iloc[0]

    def get_custom_entity_pk(self, name):
        return self.custom_entity_data[self.custom_entity_data['name'] == name]['pk'].iloc[0]

    def get_external_target_data_by_kpi_fk(self, kpi_fk):
        return self.external_targets[self.external_targets['kpi_fk'] == kpi_fk].iloc[0]

    def commit(self):
        self.common_v2.commit_results_data()
