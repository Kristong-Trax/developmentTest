import os

import pandas as pd
from collections import defaultdict

from Trax.Utils.Logging.Logger import Log
from KPIUtils_v2.DB.CommonV2 import Common
from Trax.Cloud.Services.Connector.Keys import DbUsers
from Trax.Algo.Calculations.Core.DataProvider import Data
from KPIUtils_v2.DB.PsProjectConnector import PSProjectConnector
from KPIUtils.GlobalProjects.GSK.KPIGenerator import GSKGenerator
from KPIUtils_v2.GlobalDataProvider.PsDataProvider import PsDataProvider

__author__ = 'limorc'

# The KPIs
GSK_DISPLAY_PRESENCE = 'GSK_DISPLAY_PRESENCE'
GSK_DISPLAY_SKU_COMPLIANCE = 'GSK_DISPLAY_SKU_COMPLIANCE'
GSK_DISPLAY_PRICE_COMPLIANCE = 'GSK_DISPLAY_PRICE_COMPLIANCE'
GSK_DISPLAY_BAY_PURITY = 'GSK_DISPLAY_BAY_PURITY'
# Other Constants
POS_TYPE = 'POS'
KPI_TYPE_COL = 'type'
POSM_KEY = 'posm_pk'
# Constant IDs
GSK_MANUFACTURER_ID = 2
EMPTY_PRODUCT_ID = 0
# the keys are named as per the config file
# ..ExternalTargetsTemplateLoader/ProjectsDetails/gskau-sand.py
STORE_IDENTIFIERS = [
    'additional_attribute_1', 'additional_attribute_2',
    'region_pk', 'store_type', 'store_number', 'city',
    'retailer_fk',
]
TEMPLATE_KEY = 'template_pk'
POSM_PK_KEY = 'posm_pk'
ALLOWED_POSM_EAN_KEY = 'allowed_posm_eans'
OPTIONAN_EAN_KEY = 'optional_eans'
MANDATORY_EANS_KEY = 'mandatory_eans'
POSM_IDENTIFIERS = [
    POSM_PK_KEY,
    ALLOWED_POSM_EAN_KEY,
    OPTIONAN_EAN_KEY,
    MANDATORY_EANS_KEY,
]

class GSKAUToolBox:
    LEVEL1 = 1
    LEVEL2 = 2
    LEVEL3 = 3

    def __init__(self, data_provider, output):
        self.output = output
        self.data_provider = data_provider
        self.common = Common(self.data_provider)
        self.project_name = self.data_provider.project_name
        self.session_uid = self.data_provider.session_uid
        self.products = self.data_provider[Data.PRODUCTS]
        self.all_products = self.data_provider[Data.ALL_PRODUCTS]
        self.match_product_in_scene = self.data_provider[Data.MATCHES]
        self.visit_date = self.data_provider[Data.VISIT_DATE]
        self.session_info = self.data_provider[Data.SESSION_INFO]
        self.scene_info = self.data_provider[Data.SCENES_INFO]
        self.store_info = self.data_provider[Data.STORE_INFO]
        self.store_id = self.data_provider[Data.STORE_FK]
        self.scif = self.data_provider[Data.SCENE_ITEM_FACTS]
        self.rds_conn = PSProjectConnector(self.project_name, DbUsers.CalculationEng)
        self.kpi_static_data = self.common.get_kpi_static_data()
        self.ps_data_provider = PsDataProvider(self.data_provider, self.output)
        self.targets = self.ps_data_provider.get_kpi_external_targets()
        self.kpi_results_queries = []

        self.set_up_template = pd.read_excel(os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', 'Data',
                                                          'gsk_set_up.xlsx'), sheet_name='Functional KPIs',
                                             keep_default_na=False)
        self.gsk_generator = GSKGenerator(self.data_provider, self.output, self.common, self.set_up_template)

    def main_calculation(self, *args, **kwargs):
        """
        This function calculates the KPI results.
        """
        # assortment_store_dict = self.gsk_generator.availability_store_function()
        # self.common.save_json_to_new_tables(assortment_store_dict)
        #
        # assortment_category_dict = self.gsk_generator.availability_category_function()
        # self.common.save_json_to_new_tables(assortment_category_dict)
        #
        # assortment_subcategory_dict = self.gsk_generator.availability_subcategory_function()
        # self.common.save_json_to_new_tables(assortment_subcategory_dict)
        #
        # facings_sos_dict = self.gsk_generator.gsk_global_facings_sos_whole_store_function()
        # self.common.save_json_to_new_tables(facings_sos_dict)
        #
        # linear_sos_dict = self.gsk_generator.gsk_global_linear_sos_whole_store_function()
        # self.common.save_json_to_new_tables(linear_sos_dict)
        #
        # linear_sos_dict = self.gsk_generator.gsk_global_linear_sos_by_sub_category_function()
        # self.common.save_json_to_new_tables(linear_sos_dict)
        #
        # facings_sos_dict = self.gsk_generator.gsk_global_facings_by_sub_category_function()
        # self.common.save_json_to_new_tables(facings_sos_dict)
        #
        # facings_sos_dict = self.gsk_generator.gsk_global_facings_sos_by_category_function()
        # self.common.save_json_to_new_tables(facings_sos_dict)
        #
        # linear_sos_dict = self.gsk_generator.gsk_global_linear_sos_by_category_function()
        # self.common.save_json_to_new_tables(linear_sos_dict)

        self.calculate_secondary_display_compliance()
        # self.common.commit_results_data()
        return 

    def calculate_secondary_display_compliance(self):
        kpi_display_presence = self.kpi_static_data[
            (self.kpi_static_data[KPI_TYPE_COL] == GSK_DISPLAY_PRESENCE)
            & (self.kpi_static_data['delete_time'].isnull())]
        kpi_display_sku_compliance = self.kpi_static_data[
            (self.kpi_static_data[KPI_TYPE_COL] == GSK_DISPLAY_SKU_COMPLIANCE)
            & (self.kpi_static_data['delete_time'].isnull())]
        kpi_display_price_compliance = self.kpi_static_data[
            (self.kpi_static_data[KPI_TYPE_COL] == GSK_DISPLAY_PRICE_COMPLIANCE)
            & (self.kpi_static_data['delete_time'].isnull())]
        kpi_display_bay_purity = self.kpi_static_data[
            (self.kpi_static_data[KPI_TYPE_COL] == GSK_DISPLAY_BAY_PURITY)
            & (self.kpi_static_data['delete_time'].isnull())]
        secondary_display_targets = self.targets[
            self.targets['kpi_fk'] == kpi_display_presence['pk'].iloc[0]]
        # if no targets return
        if secondary_display_targets.empty:
            Log.warning('There is no target policy for calculating secondary display compliance.')
            return False
        else:
            has_posm_recognized = False
            multi_posm = False
            multi_bay = False
            sku_compliance = False
            price_compliance = False
            for idx, each_target in secondary_display_targets.iterrows():
                # loop through each external target to fit the current store
                # calculate display compliance for all the matching external targets
                store_relevant_targets = each_target[STORE_IDENTIFIERS].dropna()
                relevant_store_attributes = each_target[STORE_IDENTIFIERS].dropna()
                _bool_check_df = self.store_info[list(store_relevant_targets.keys())] == relevant_store_attributes.values
                is_store_relevant = _bool_check_df.all(axis=None)
                if is_store_relevant:
                    Log.info('The session: {sess} is relevant for calculating secondary display compliance.'
                             .format(sess=self.session_uid))
                    # check if there are multiple POSMs or multiple bays
                    # in both case the compliance fails plus associated flags fails
                    # then only purity at bay level needs to be calculated

                    # FIND THE SCENES WHICH HAS THE POSM to check for multiposm or multibays
                    posm_to_check = each_target[POSM_PK_KEY]
                    valid_scenes = self.scif[self.scif['product_fk'] == posm_to_check]['scene_id'].unique()
                    valid_scifs_group = self.scif[self.scif['scene_id'].isin(valid_scenes)].groupby('scene_id')
                    posm_relevant_targets = each_target[POSM_IDENTIFIERS].dropna()

                    def _sanitize_csv(data):
                        if type(data) == list:
                            # not handling ["val1", "val2,val3", "val4"]
                            return [x.strip() for x in data if x.strip()]
                        else:
                            return [each.strip() for each in data.split(',') if each.strip()]
                    mandatory_eans = _sanitize_csv(posm_relevant_targets[MANDATORY_EANS_KEY])
                    allowed_posm_eans = _sanitize_csv(posm_relevant_targets[ALLOWED_POSM_EAN_KEY])
                    for scene_id, scene_data in valid_scifs_group:
                        if not len(scene_data[scene_data['product_type'] == POS_TYPE]):
                            Log.info('The scene: {scene_id} is relevant but no POSMs found.'
                                     .format(scene_id=scene_id))
                            self.save_display_compliance_data(
                                [
                                    {'pk': kpi_display_presence.fk, 'result': int(has_posm_recognized),
                                     'score': int(multi_posm), 'scene_id': scene_id},
                                    {'pk': kpi_display_sku_compliance.fk, 'result': int(sku_compliance),
                                     'scene_id': scene_id},
                                    {'pk': kpi_display_price_compliance.fk, 'result': int(price_compliance),
                                     'scene_id': scene_id},
                                ]
                            )
                            continue
                        elif len(scene_data[scene_data['product_type'] == POS_TYPE]) == 1:
                            Log.info('The session: {sess} is relevant with single POSM.'
                                     .format(sess=self.session_uid))
                        elif len(scene_data[scene_data['product_type'] == POS_TYPE]) > 1:
                            Log.info('The session: {sess} is relevant with multiple POSM.'
                                     .format(sess=self.session_uid))
                            multi_posm = True
                        valid_match_products_in_scene = self.match_product_in_scene[
                            self.match_product_in_scene['scene_fk'].isin(valid_scenes)]
                        if len(valid_match_products_in_scene['bay_number'].unique()) > 1:
                            multi_bay = True
                        # save purity per bay
                        self.save_purity_per_bay(kpi_display_bay_purity, valid_match_products_in_scene)
                        self.save_display_compliance_data(
                            [
                                {'pk': kpi_display_presence.fk, 'result': int(has_posm_recognized),
                                 'score': int(multi_posm), 'scene_id': scene_id},
                                {'pk': kpi_display_sku_compliance.fk, 'result': int(sku_compliance),
                                 'scene_id': scene_id},
                                {'pk': kpi_display_price_compliance.fk, 'result': int(price_compliance),
                                 'scene_id': scene_id},
                            ]
                        )
                else:
                    # the session/store is not part of the KPI targets
                    Log.info('For the session: {sess}, the current kpi target is not valid. Keep Looking...'
                             .format(sess=self.session_uid))
                    continue
            pass
        pass

    def save_purity_per_bay(self, kpi_display_bay_purity, valid_match_products_in_scene):
        output_dict = defaultdict(dict)
        total_prod_in_scene_dict = {}
        for scene_fk, scene_data in valid_match_products_in_scene.groupby('scene_fk'):
            total_prod_in_scene_dict[scene_fk] = len(
                scene_data[scene_data['product_fk'] != 0]
            )
        mpis_grouped_by_bay = valid_match_products_in_scene.groupby(['scene_fk', 'bay_number'])
        for scene_bay_tup, mpis in mpis_grouped_by_bay:
            scene_fk, bay_number = scene_bay_tup
            mpis_with_prod = mpis.merge(self.products, how='left', on=['product_fk'], suffixes=('', '_prod'))
            gsk_prod_count = len(mpis_with_prod[mpis_with_prod['manufacturer_fk'] == 2])
            total_prod_in_scene_count = total_prod_in_scene_dict.get(scene_fk)
            if total_prod_in_scene_count:
                output_dict[scene_fk][bay_number] = gsk_prod_count / float(total_prod_in_scene_count)
        # self.common.write_to_db_result(
        #     fk=int(kpi_display_bay_purity.fk),
        #     numerator_id=int(data_to_write.numerator_id),
        #     denominator_id=int(data_to_write.denominator_id),
        #     result=int(result),
        #     score=1,
        #     identifier_result=str(data_to_write.kpi_name),
        #     context_id=int(data_to_write.zone_number),
        # )
        return output_dict

    def save_display_compliance_data(self, data):
        for each_kpi_data in data:
            Log.info("Saving kpi_fk {pk} for session: {ses} - scene {scene}".format(
                pk=each_kpi_data.get('pk'),
                sess=self.session_uid,
                scene=each_kpi_data.get('scene_id'),
            ))
            self.common.write_to_db_result(
                fk=each_kpi_data.get('pk'),
                numerator_id=self.store_id,
                denominator_id=self.store_id,
                result=each_kpi_data.get('result', None),
                score=float('{scene_id}.{score}'.format(
                    scene_id=each_kpi_data.get('scene_id', 0),  # this must be there!
                    score=each_kpi_data.get('score', 0))),
                context_id=self.store_id,
            )