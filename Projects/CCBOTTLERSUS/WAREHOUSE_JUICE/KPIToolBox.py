
from Trax.Algo.Calculations.Core.DataProvider import Data
from Projects.CCBOTTLERSUS.WAREHOUSE_JUICE.Const import Const
from KPIUtils_v2.DB.Common import Common as Common
from KPIUtils_v2.DB.CommonV2 import Common as CommonV2
from KPIUtils_v2.Calculations.SurveyCalculations import Survey
from KPIUtils_v2.Calculations.SOSCalculations import SOS

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
        self.match_product_in_scene = self.data_provider[Data.MATCHES]
        self.visit_date = self.data_provider[Data.VISIT_DATE]
        self.session_info = self.data_provider[Data.SESSION_INFO]
        self.scene_info = self.data_provider[Data.SCENES_INFO]
        self.store_id = self.data_provider[Data.STORE_FK]
        self.store_info = self.data_provider[Data.STORE_INFO]
        self.scif = self.data_provider[Data.SCENE_ITEM_FACTS]
        self.scif = self.scif[~(self.scif['product_type'] == 'Irrelevant')]
        self.survey = Survey(self.data_provider, self.output)
        self.sos = SOS(self.data_provider, self.output)
        self.templates = {}
        self.common_db = Common(self.data_provider)
        self.common_db2 = CommonV2(self.data_provider)
        self.common_scene = CommonV2(self.data_provider)
        self.region = self.store_info['region_name'].iloc[0].replace(u'\xa0', u' ')
        self.store_type = self.store_info['store_type'].iloc[0]
        self.program = self.store_info['additional_attribute_3'].iloc[0]
        self.total_score = 0
        self.ignore_stacking = False
        self.facings_field = 'facings' if not self.ignore_stacking else 'facings_ign_stack'

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
        relevant_scenes = self._get_relevant_scenes(relevant_scif)

        for scene in relevant_scenes:
            set_size = 0
            scene_scif = self.scif[self.scif['scene_id'] == scene]
            bays_in_scene = scene_scif['bay'].unique().tolist()

            for bay in bays_in_scene:
                bay_scif = scene_scif[scene_scif['bay'] == bay]
                total_space = bay_scif[LINEARSPACE]
                category_space = bay_scif[bay_scif['category'] == CATEGORY]
                if category_space / total_space > threshold:
                    shelf_length = self._get_normalized_shelf_length(bay_scif)
                    set_size += shelf_length


    def _get_relevant_scenes(self, relevant_scif):
        try:
            relevant_scenes = relevant_scif[relevant_scif['template_name'].isin(Const.RELEVANT_SCENE_TYPES)]['scene_id'].unique().tolist()
        except IndexError:
            relevant_scenes = []
        return relevant_scenes

    def _get_normalized_shelf_length(self, bay_scif):
        shelves = bay_scif['shelf'].unique().tolist()
        max_shelf_length = 0
        for shelf in shelves:
            shelf_length = bay_scif[bay_scif['shelf_number'] == shelf]['width_mm'].sum()
            if shelf_length > max_shelf_length:
                max_shelf_length = shelf_length

        # convert shelf length to base 4
        return int(round(max_shelf_length / 4) * 4)
