
from Trax.Utils.Logging.Logger import Log
from Trax.Algo.Calculations.Core.DataProvider import Data
from Projects.CCBOTTLERSUS.WAREHOUSE_JUICE.Const import Const
from KPIUtils_v2.DB.Common import Common as Common
from KPIUtils_v2.DB.CommonV2 import Common as CommonV2

__author__ = 'Hunter'

REGION = 'Warehouse Juice'


class CCBOTTLERSUSWAREHOUSEJUICEToolBox:
    EXCLUDE_FILTER = 0
    INCLUDE_FILTER = 1
    CONTAIN_FILTER = 2

    def __init__(self, data_provider, output):
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
        self.scif = self.data_provider[Data.SCENE_ITEM_FACTS]
        self.templates = {}
        self.common_db = Common(self.data_provider)
        self.common_db2 = CommonV2(self.data_provider)
        self.common_scene = CommonV2(self.data_provider)
        self.new_kpi_static_data = self.common_db.get_new_kpi_static_data()
        self.region = self.store_info['region_name'].iloc[0].replace(u'\xa0', u' ')  # fix non-breaking spaces
        self.store_type = self.store_info['store_type'].iloc[0]
        self.program = self.store_info['additional_attribute_3'].iloc[0]
        self.total_score = 0
        self.ignore_stacking = False
        self.facings_field = 'facings' if not self.ignore_stacking else 'facings_ign_stack'
        self.completed_scene_types = []

    # main functions:
    def main_calculation(self, *args, **kwargs):
        if self.region != Const.WAREHOUSE_JUICE:
            return
        else:
            relevant_scif = self.scif
            self.calculate_set_size(relevant_scif)

    # set size functions
    def calculate_set_size(self, relevant_scif):
        threshold = 0.75
        relevant_scenes = self.get_relevant_scenes(relevant_scif)

        for scene in relevant_scenes:
            set_size = 0
            scene_type = relevant_scif[relevant_scif['scene_id'] == scene]['template_name'].iloc[0]
            if scene_type in self.completed_scene_types:
                continue

            scene_mpis = self.mpis[self.mpis['scene_fk'] == scene]
            bays_in_scene = scene_mpis['bay_number'].unique().tolist()

            for bay in bays_in_scene:
                bay_mpis = scene_mpis[scene_mpis['bay_number'] == bay]
                total_space = bay_mpis['width_mm'].sum()
                tested_group_space = \
                    bay_mpis[bay_mpis['category'].isin(Const.RELEVANT_CATEGORIES[scene_type])]['width_mm'].sum()
                if tested_group_space / total_space > threshold:
                    shelf_length = self.get_normalized_shelf_length(bay_mpis)
                    set_size += shelf_length

            kpi_fk = self.get_kpi_fk_from_kpi_name(Const.KPI_NAME)
            template_fk = self.get_template_fk(scene_type)
            self.common_db.write_to_db_result_new_tables(kpi_fk, template_fk, set_size, set_size,
                                                         denominator_id=self.store_id, denominator_result=1,
                                                         score=set_size)

    def get_relevant_scenes(self, relevant_scif):
        try:
            relevant_scenes = relevant_scif[relevant_scif['template_name'].isin(Const.RELEVANT_SCENE_TYPES)][
                'scene_id'].unique().tolist()
        except IndexError:
            relevant_scenes = []
        return relevant_scenes

    def get_normalized_shelf_length(self, bay_mpis):
        shelves = bay_mpis['shelf'].unique().tolist()
        max_shelf_length = 0
        for shelf in shelves:
            shelf_length = bay_mpis[bay_mpis['shelf_number'] == shelf]['width_mm'].sum()
            if shelf_length > max_shelf_length:
                max_shelf_length = shelf_length

        # convert shelf length from mm to ft
        max_shelf_length * 0.00328084

        # convert shelf length to base 4
        return int(round(max_shelf_length / 4) * 4)

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

    def commit_results_without_delete(self):
        self.common_db.commit_results_data_without_delete()
