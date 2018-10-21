
from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Cloud.Services.Connector.Keys import DbUsers
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Trax.Data.Projects.Connector import ProjectConnector
from Trax.Utils.Logging.Logger import Log

from KPIUtils_v2.DB.Common import Common
# from KPIUtils_v2.Calculations.AssortmentCalculations import Assortment
# from KPIUtils_v2.Calculations.AvailabilityCalculations import Availability
# from KPIUtils_v2.Calculations.NumberOfScenesCalculations import NumberOfScenes
# from KPIUtils_v2.Calculations.PositionGraphsCalculations import PositionGraphs
# from KPIUtils_v2.Calculations.SOSCalculations import SOS
# from KPIUtils_v2.Calculations.SequenceCalculations import Sequence
# from KPIUtils_v2.Calculations.SurveyCalculations import Survey

from KPIUtils_v2.Calculations.CalculationsUtils.GENERALToolBoxCalculations import GENERALToolBox
__author__ = 'ilays'

KPI_RESULT = 'report.kpi_results'
KPK_RESULT = 'report.kpk_results'
KPS_RESULT = 'report.kps_results'

class MDLZUSToolBox:
    LEVEL1 = 1
    LEVEL2 = 2
    LEVEL3 = 3
    MM_TO_FEET_CONVERSION = 0.0032808399
    PG_CATEGORY = 'PG_CATEGORY'
    FABRICARE_CATEGORIES = ['TOTAL FABRIC CONDITIONERS', 'BLEACH AND LAUNDRY ADDITIVES', 'TOTAL LAUNDRY CARE']

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
        self.store_id = self.data_provider[Data.STORE_FK]
        self.scif = self.data_provider[Data.SCENE_ITEM_FACTS]
        self.rds_conn = ProjectConnector(self.project_name, DbUsers.CalculationEng)
        self.kpi_static_data = self.common.get_kpi_static_data()
        self.kpi_results_queries = []
        self.average_shelf_values = {}
        self.toolbox = GENERALToolBox(data_provider)

    def main_calculation(self, *args, **kwargs):
        """
        This function calculates the KPI results.
        """

        category_dict = {'category': ['Candy', 'Cough', 'Gum', 'Chocolate', 'Biscuit/Cookie/Crackers/Belvita']}
        result = self.calculate_category_space(threshold=0.5, **category_dict)


    def calculate_category_space(self, threshold=0.5, linear_size=3, **filters):
        """
        :param threshold: The ratio for a bay to be counted as part of a category.
        :param filters: These are the parameters which the data frame is filtered by.
        :return: The total shelf width (in mm) the relevant facings occupy.
        """
        try:
            filtered_scif = self.scif[self.toolbox.get_filter_condition(self.scif, **filters)]
            space_length = 0
            bay_values = []
            for scene in filtered_scif['scene_fk'].unique().tolist():
                scene_matches = self.match_product_in_scene[self.match_product_in_scene['scene_fk'] == scene]
                scene_filters = filters
                scene_filters['scene_fk'] = scene
                for bay in scene_matches['bay_number'].unique().tolist():
                    bay_total_linear = scene_matches.loc[(scene_matches['bay_number'] == bay) &
                                                         (scene_matches['stacking_layer'] == 1) &
                                                         (scene_matches['status'] == 1)]['width_mm_advance'].sum()
                    scene_filters['bay_number'] = bay
                    tested_group_linear = scene_matches[self.toolbox.get_filter_condition(scene_matches, **scene_filters)]
                    tested_group_linear_value = tested_group_linear['width_mm_advance'].sum()
                    # tested_group_linear = self.calculate_share_space_length(**scene_filters)
                    if tested_group_linear_value:
                        bay_ratio = bay_total_linear / float(tested_group_linear_value)
                    else:
                        bay_ratio = 0
                    if bay_ratio >= threshold:
                        bay_num_of_shelves = len(scene_matches.loc[(scene_matches['bay_number'] == bay) &
                                                                   (scene_matches['stacking_layer'] == 1)][
                                                     'shelf_number'].unique().tolist())
                        if bay_num_of_shelves:
                            bay_final_linear_value = tested_group_linear_value / float(bay_num_of_shelves)
                        else:
                            bay_final_linear_value = 0
                        bay_values.append(bay_final_linear_value)
                        space_length += bay_final_linear_value
                space_length = sum([linear_size for value in bay_values if value > 0])
        except Exception as e:
            Log.info('Linear Feet calculation failed due to {}'.format(e))
            space_length = 0

        return space_length
