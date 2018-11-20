
import pandas as pd
from datetime import datetime

from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Utils.Conf.Keys import DbUsers
from KPIUtils_v2.DB.PsProjectConnector import PSProjectConnector
from Trax.Utils.Logging.Logger import Log
from Trax.Data.Utils.MySQLservices import get_table_insertion_query as insert
from Trax.Algo.Calculations.Core.Shortcuts import BaseCalculationsGroup
from Projects.CUBAU_SAND.Utils.Fetcher import CUBAUQueries
from Projects.CUBAU_SAND.Utils.GeneralToolBox import CUBAUGENERALToolBox
from KPIUtils.DB.Common import Common
from KPIUtils.Calculations.SOS import SOS

__author__ = 'Shani'

KPI_RESULT = 'report.kpi_results'
KPK_RESULT = 'report.kpk_results'
KPS_RESULT = 'report.kps_results'


def log_runtime(description, log_start=False):
    def decorator(func):
        def wrapper(*args, **kwargs):
            calc_start_time = datetime.utcnow()
            if log_start:
                Log.info('{} started at {}'.format(description, calc_start_time))
            result = func(*args, **kwargs)
            calc_end_time = datetime.utcnow()
            Log.info('{} took {}'.format(description, calc_end_time - calc_start_time))
            return result
        return wrapper
    return decorator


class CUBAUToolBox:
    LEVEL1 = 1
    LEVEL2 = 2
    LEVEL3 = 3
    OwnMan = 'CUB'
    OwnManFK = 1
    SOS = ['TAP', 'CUB']
    ON_PREMISE = 'On Premise'
    OFF_PREMISE = 'Off Premise'
    TOP_PALLET = 'Cool Room Stacks Top'
    FRONT_PALLET = 'Cool Room Stacks Front'
    PRODUCT_TYPES_TO_EXCLUDE = ['POS', 'Irrelevant']


    def __init__(self, data_provider, output):
        self.output = output
        self.data_provider = data_provider
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
        self.rds_conn = PSProjectConnector(self.project_name, DbUsers.CalculationEng)
        self.tools = CUBAUGENERALToolBox(self.data_provider, self.output, rds_conn=self.rds_conn)
        self.common = Common(self.data_provider)
        self.common_sos = SOS(self.data_provider, self.output)
        self.kpi_results_queries = []
        self.k_engine = BaseCalculationsGroup(data_provider, output)
        # self.channel = self.get_channel(self.store_id)
        # self.scene_to_exclude = self.get_exclude_scene()
        self.store_type = data_provider.store_type

    def main_calculation(self):
        """
        This function calculates the KPI results.
        """
        if self.store_type == self.ON_PREMISE:
            sos_types = ['TAP', 'TRAX']
        else:
            sos_types = ['CUB', 'TRAX']
        self.update_custom_scif()

        for sos in sos_types:
            exclude_scene = self.get_exclude_scene(sos)
            product_type = self.get_product_filter(sos)
            self.own_manufacturer_out_store(sos, product_type, exclude_scene)
            self.all_param_out_category('manufacturer_fk', 'category_fk', sos, product_type, exclude_scene,
                                        context=None)
            self.all_param_out_category('manufacturer_fk', 'category_fk', sos, product_type, exclude_scene,
                                        context='template_fk')
            self.all_param_out_category('brand_fk', 'category_fk', sos, product_type, exclude_scene,
                                        context='template_fk')
        return

    def get_product_filter(self,sos):
        if sos == 'TAP':
            product_type = 'POS'
        else:
            product_type = (['POS', 'Irrelevant'], 0)
        return product_type

    def get_exclude_scene(self, sos):
        if self.store_type == self.ON_PREMISE:
            exclude_scene = None
        else:
            if sos == 'CUB':
                exclude_scene = ['Cool Room Stacks Front']
            else:
                exclude_scene = ['Cool Room Stacks Top']
        return exclude_scene

    def own_manufacturer_out_store(self, sos_type, product_type, exclude_scene = None):
        kpi_type = '{}_SHARE_MANUFACTURER_OUT_OF_STORE'.format(sos_type)
        kpi_fk = self.common.New_kpi_static_data[self.common.New_kpi_static_data['type'] == kpi_type]['pk'].values[0]
        subset_filters = {'manufacturer_name': self.OwnMan}
        pop_filters = {'template_display_name': (exclude_scene, 0), 'product_type': product_type}
        self.calculate_sos(sos_type, kpi_fk, self.OwnManFK, '999', subset_filters, pop_filters)
        return

    def all_param_out_category(self, param_1, param_2, sos_type, product_type, exclude_scene=None, context=None):
        if not context:  # only for own manufacturer out of category
            df = self.scif.loc[:, [param_1, param_2]]
            relevant_df = df.drop_duplicates(subset=param_2)
            kpi_type = '{}_SHARE_OWN_{}_OUT_OF_CATEGORY'.format(sos_type, (param_1.replace('_fk', '')).upper())
        else:
            df = self.scif.loc[:, [param_1, param_2, context]]
            kpi_type = '{}_SHARE_{}_OUT_OF_CATEGORY'.format(sos_type, (param_1.replace('_fk', '')).upper())
            relevant_df = df.drop_duplicates()
        kpi_fk = self.common.New_kpi_static_data[self.common.New_kpi_static_data['type'] == kpi_type]['pk'].values[0]
        for i, row in relevant_df.iterrows():
            if not context:
                pop_filters = {'template_display_name': (exclude_scene, 0), 'product_type': product_type,
                               param_2: row[param_2]}
                subset_filters = {param_1: self.OwnManFK}
            else:
                pop_filters = {'template_display_name': (exclude_scene, 0), 'product_type': product_type,
                               param_2: row[param_2], context: row[context]}
                subset_filters = {param_1: row[param_1]}
            if context:
                self.calculate_sos(sos_type, kpi_fk, row[param_1], row[param_2], subset_filters, pop_filters,
                                   row[context])
            else:
                self.calculate_sos(sos_type, kpi_fk, self.OwnManFK, row[param_2], subset_filters, pop_filters)
        return

    def calculate_sos(self, sos_type, kpi_fk, numerator_fk, denominator_fk, subset_filters, pop_filters, context=None):
        ratio = 0
        #  denominator
        pop_filter = self.common_sos.get_filter_condition(self.scif, **pop_filters)
        #  numerator
        subset_filter = self.common_sos.get_filter_condition(self.scif, **subset_filters)
        try:
            ratio = self.calculate_sos_by_policy(sos_type, kpi_fk, numerator_fk, denominator_fk,
                                                 subset_filter=subset_filter, pop_filter=pop_filter, context=context)
        except Exception as e:
            Log.debug('calculate_sos_facing_by_scene can not be calculated for scene {}'.format(e.message))
        return ratio

    def calculate_sos_by_policy(self, sos_type, kpi_fk, numerator_fk, denominator_fk,
                                subset_filter, pop_filter, context=None):
        den_df = self.scif[pop_filter]
        nom_df = den_df[subset_filter]
        if sos_type == 'CUB':
            denominator = den_df.loc[den_df['template_display_name'] == self.TOP_PALLET]['facings'].sum() + \
                          den_df.loc[~(den_df['template_display_name'] == self.TOP_PALLET)]['facings_ign_stack'].sum()
            numerator = nom_df.loc[nom_df['template_display_name'] == self.TOP_PALLET]['facings'].sum() + \
                          nom_df.loc[~(nom_df['template_display_name'] == self.TOP_PALLET)]['facings_ign_stack'].sum()
        else:
            denominator = den_df['facings'].sum()
            numerator = nom_df['facings'].sum()
        if denominator != 0:
            ratio = (numerator / float(denominator))
            self.common.write_to_db_result_new_tables(kpi_fk, numerator_fk, numerator, round(ratio, 2), denominator_fk,
                                                  denominator, round(ratio, 2), context_id=context)
        return

    def update_custom_scif(self):  # only for 'CUB Share' and 'TRAX Share'

        scenes = self.scif['scene_id'].unique().tolist()
        for scene in scenes:
            template_name = self.scif.loc[self.scif['scene_fk'] == scene]['template_display_name'].values[0]
            products = self.scif[(self.scif['scene_id'] == scene) &
                                 (~self.scif['product_type'].isin(self.PRODUCT_TYPES_TO_EXCLUDE))]['product_fk'].unique().tolist()
            for product in products:
                facings_cub_share = None
                facings_trax_share = None
                if template_name == self.FRONT_PALLET:
                    facings_trax_share = self.scif[(self.scif['scene_id'] == scene) &
                                                   (self.scif['product_fk'] == product)]['facings'].values[0]
                elif template_name == self.TOP_PALLET:
                    if self.store_type == self.OFF_PREMISE:
                        facings_cub_share = self.scif[(self.scif['scene_id'] == scene) &
                                                (self.scif['product_fk'] == product)]['facings'].values[0]
                else:
                    if self.store_type == self.OFF_PREMISE:
                        facings_cub_share = self.scif[(self.scif['scene_id'] == scene) &
                                                    (self.scif['product_fk'] == product)]['facings_ign_stack'].values[0]
                    facings_trax_share = self.scif[(self.scif['scene_id'] == scene) &
                                                  (self.scif['product_fk'] == product)]['facings'].values[0]
                self.common.get_custom_scif_query(scene_fk=scene, product_fk=product,
                                                  in_assortment_OSA=facings_cub_share,
                                                  oos_osa=facings_trax_share, mha_in_assortment=None,
                                                  mha_oos=None, length_mm_custom=None)
        self.common.commit_custom_scif()
        return

