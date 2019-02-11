
import pandas as pd
from Trax.Utils.Logging.Logger import Log
from Trax.Algo.Calculations.Core.DataProvider import Data
from Projects.CCBOTTLERSUS.WAREHOUSE_JUICE.Const import Const

__author__ = 'Hunter'

REGION = 'Warehouse Juice'

class CCBOTTLERSUSWAREHOUSEJUICEToolBox:
    EXCLUDE_FILTER = 0
    INCLUDE_FILTER = 1
    CONTAIN_FILTER = 2

    def __init__(self, data_provider, output, common_v2):
        self.output = output
        self.data_provider = data_provider
        self.project_name = self.data_provider.project_name
        self.session_uid = self.data_provider.session_uid
        self.products = self.data_provider[Data.PRODUCTS]
        self.all_products = self.data_provider[Data.ALL_PRODUCTS]
        self.mpis = self.data_provider[Data.MATCHES]
        self.visit_date = self.data_provider[Data.VISIT_DATE]
        self.session_info = self.data_provider[Data.SESSION_INFO]
        self.scene_info = self.data_provider[Data.SCENES_INFO]
        self.store_id = self.data_provider[Data.STORE_FK]
        self.store_info = self.data_provider[Data.STORE_INFO]
        self.retailer = self.store_info['retailer_name'].iloc[0]
        self.scif = self.data_provider[Data.SCENE_ITEM_FACTS]
        self.templates = {}
        for sheet in Const.RETAILERS:
            self.templates[sheet] = pd.read_excel(Const.TEMPLATE_PATH, sheetname=sheet).fillna('')
        self.common_v2 = common_v2
        self.new_kpi_static_data = self.common_v2.get_kpi_static_data()
        self.region = self.store_info['region_name'].iloc[0].replace(u'\xa0', u' ')  # fix non-breaking spaces
        self.store_type = self.store_info['store_type'].iloc[0]
        self.program = self.store_info['additional_attribute_3'].iloc[0]
        self.total_score = 0
        self.ignore_stacking = False
        self.facings_field = 'facings' if not self.ignore_stacking else 'facings_ign_stack'
        self.completed_scene_types = {}
        self.set_size = {}

    # main functions:
    def main_calculation(self, *args, **kwargs):
        if self.region != Const.WAREHOUSE_JUICE:
            return
        relevant_scif = self.scif
        self.calculate_set_size(relevant_scif)
        if self.retailer in Const.RETAILERS:
            self.calculate_assortment()

    # set size functions
    def calculate_set_size(self, relevant_scif):
        threshold = 0.75
        relevant_scenes = self.get_relevant_scenes(relevant_scif)

        for scene in relevant_scenes:
            set_size = 0
            scene_type = relevant_scif[relevant_scif['scene_id'] == scene]['template_name'].iloc[0]
            if scene_type in self.completed_scene_types.iterkeys():
                continue

            scene_mpis = self.mpis[self.mpis['scene_fk'] == scene]
            bays_in_scene = scene_mpis['bay_number'].unique().tolist()

            for bay in bays_in_scene:
                bay_mpis = scene_mpis[scene_mpis['bay_number'] == bay]
                total_space = bay_mpis['width_mm'].sum()
                tested_group_skus = self.get_product_fks_from_total_category(Const.RELEVANT_CATEGORIES[scene_type])
                tested_group_space = bay_mpis[bay_mpis['product_fk'].isin(tested_group_skus)]['width_mm'].sum()
                if tested_group_space / float(total_space) > threshold:
                    # shelf_length = self.get_normalized_shelf_length(bay_mpis)
                    set_size += 4

            self.completed_scene_types.update({scene_type: scene})
            self.set_size.update({scene_type: set_size})

            kpi_fk = self.get_kpi_fk_from_kpi_name(Const.SET_SIZE_KPI_NAME)
            template_fk = self.get_template_fk(scene_type)

            # self.common_db.write_to_db_result_new_tables(kpi_fk, template_fk, set_size, set_size,
            #                                              denominator_id=self.store_id, denominator_result=1,
            #                                              score=set_size)
            self.common_v2.write_to_db_result(kpi_fk, numerator_id=template_fk, numerator_result=set_size,
                                              denominator_id=self.store_id, denominator_result=1,
                                              score=set_size)

    def get_relevant_scenes(self, relevant_scif):
        try:
            relevant_scenes = relevant_scif[relevant_scif['template_name'].isin(Const.RELEVANT_SCENE_TYPES)][
                'scene_id'].unique().tolist()
        except IndexError:
            relevant_scenes = []
        return relevant_scenes

    # def get_normalized_shelf_length(self, bay_mpis):
    #     shelves = bay_mpis['shelf_number'].unique().tolist()
    #     max_shelf_length = 0
    #     for shelf in shelves:
    #         shelf_length = bay_mpis[bay_mpis['shelf_number'] == shelf]['width_mm_net'].sum()
    #         max_shelf_length += shelf_length
    #
    #     # convert shelf length from mm to ft
    #     max_shelf_length = (max_shelf_length / len(shelves)) * 0.00328084
    #
    #     # convert shelf length to base 4
    #     return int(round(max_shelf_length / 4) * 4)

    # assortment functions
    def calculate_assortment(self):
        if self.set_size is None:
            return
        kpi_fk = self.get_kpi_fk_from_kpi_name(Const.ASSORTMENT_KPI_NAME)
        for scene_type, set_size in self.set_size.iteritems():
            if scene_type != Const.DRINK_JUICE_TEA:
                continue
            scene_id = self.completed_scene_types[scene_type]
            template_fk = self.get_template_fk(scene_type)
            relevant_template = self.templates[self.retailer]
            products_in_assortment = relevant_template[relevant_template[Const.SET_SIZE] <= set_size][Const.UPC].tolist()
            products_in_scene = self.scif[(self.scif['scene_id'] == scene_id) &
                                          (self.scif['facings'] > 0)]['product_fk'].tolist()
            for upc in products_in_assortment:
                try:
                    product_data = self.all_products[self.all_products[Const.NIELSEN_UPC].str.contains(unicode(upc), na=False)].iloc[0]
                    product_fk = product_data['product_fk']
                except IndexError:
                    Log.warning('UPC {} for {} does not exist in the database'.format(upc, self.retailer))
                    continue
                result = 2 if product_fk in products_in_scene else 1
                # self.common_db.write_to_db_result_new_tables(kpi_fk, product_fk, result, result,
                #                                              denominator_id=template_fk, denominator_result=1,
                #                                              score=scene_id)
                self.common_v2.write_to_db_result(kpi_fk, numerator_id=product_fk, numerator_result=result,
                                                  denominator_id=template_fk, denominator_result=1,
                                                  score=scene_id)

    # helpers
    def get_kpi_fk_from_kpi_name(self, kpi_name):
        try:
            return self.new_kpi_static_data[self.new_kpi_static_data['client_name'] == kpi_name]['pk'].iloc[0]
        except IndexError:
            Log.error('KPI {} does not exist in the database!'.format(kpi_name))
            return None

    def get_template_fk(self, template_name):
        try:
            return self.scif[self.scif['template_name'] == template_name]['template_fk'].iloc[0]
        except IndexError:
            Log.error('Template FK for {} does not exist in the database!'.format(template_name))
            return None

    def get_product_fks_from_total_category(self, category_list):
        return self.scif[self.scif[Const.TOTAL_CATEGORY].isin(category_list)]['product_fk'].unique().tolist()

    # def commit_results_without_delete(self):
    #     self.common_db.commit_results_data_without_delete_version2()

    def commit_results(self):
        self.common_v2.commit_results_data()
