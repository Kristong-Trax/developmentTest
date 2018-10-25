
from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Cloud.Services.Connector.Keys import DbUsers
from Trax.Data.Projects.Connector import ProjectConnector
# from Trax.Utils.Logging.Logger import Log
from Trax.Data.Utils.MySQLservices import get_table_insertion_query as insert

from KPIUtils.DB.Common import Common
from KPIUtils_v2.DB.CommonV2 import Common as CommonV2
from KPIUtils.DIAGEO.ToolBox import DIAGEOToolBox
from KPIUtils.GlobalProjects.DIAGEO.Utils.Fetcher import DIAGEOQueries
from KPIUtils.GlobalProjects.DIAGEO.KPIGenerator import DIAGEOGenerator
from Projects.DIAGEOCO_SAND.Utils.Const import Const

from datetime import datetime
import pandas as pd

__author__ = 'huntery'

KPI_RESULT = 'report.kpi_results'
KPK_RESULT = 'report.kpk_results'
KPS_RESULT = 'report.kps_results'

class DIAGEOCO_SANDToolBox:
    LEVEL1 = 1
    LEVEL2 = 2
    LEVEL3 = 3

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
        self.rds_conn = ProjectConnector(self.project_name, DbUsers.CalculationEng)
        self.kpi_static_data = self.common.get_kpi_static_data()
        self.kpi_results_queries = []
        self.set_templates_data = {}
        self.match_display_in_scene = self.get_match_display()
        self.tools = DIAGEOToolBox(self.data_provider, output, match_display_in_scene=self.match_display_in_scene)
        self.global_gen = DIAGEOGenerator(self.data_provider, self.output, self.common)

    def main_calculation(self, *args, **kwargs):

        """
        This function calculates the KPI results.
        """
        self.relative_positioning_template = pd.read_excel(Const.TEMPLATE_PATH, Const.RELATIVE_POSITIONING_SHEET_NAME,
                                                           header=Const.RELATIVE_POSITIONING_HEADER_ROW).to_dict(
            orient='records')
        self.brand_blocking_template = pd.read_excel(Const.TEMPLATE_PATH, Const.BRAND_BLOCKING_SHEET_NAME,
                                                     header=Const.BRAND_BLOCKING_HEADER_ROW).to_dict(orient='records')
        self.brand_pouring_status_template = pd.read_excel(Const.TEMPLATE_PATH, Const.BRAND_POURING_SHEET_NAME,
                                                           header=Const.BRAND_POURING_HEADER_ROW).to_dict(orient='records')
        self.touchpoint_template = pd.read_excel(Const.TOUCHPOINT_TEMPLATE_PATH, header=Const.TOUCHPOINT_HEADER_ROW)
        self.set_templates_data['Brand Blocking'] = self.brand_blocking_template
        self.set_templates_data['Relative Position'] = self.relative_positioning_template

        # the manufacturer name for DIAGEO is 'Diageo' by default. We need to redefine this for DiageoCO
        self.global_gen.tool_box.DIAGEO = 'DIAGEO'

        self.calculate_block_together() # working
        self.calculate_secondary_display() # working
        self.calculate_brand_pouring_status() # working
        self.calculate_touch_point() # using old tables, needs work
        self.calculate_relative_position() # working
        # self.calculate_activation_standard() # using old tables, needs work

        self.global_gen.diageo_global_assortment_function() # working
        # self.global_gen.diageo_global_share_of_shelf_function() # need template

    def calculate_secondary_display(self):
        result = self.global_gen.diageo_global_secondary_display_secondary_function()
        if result:
            self.common.write_to_db_result_new_tables(**result)
        set_name = Const.SECONDARY_DISPLAYS
        set_score = self.tools.calculate_assortment(assortment_entity='scene_id', location_type='Secondary Shelf')
        self.save_level2_and_level3(set_name, set_name, set_score)

    def calculate_brand_pouring_status(self):
        results_list = self.global_gen.diageo_global_brand_pouring_status_function(self.brand_pouring_status_template)
        if results_list:
            for result in results_list:
                self.common.write_to_db_result_new_tables(**result)

    def calculate_touch_point(self):
        sub_brands = self.touchpoint_template['Sub Brand'].dropna().unique().tolist()
        results_list = self.global_gen.diageo_global_touch_point_function(sub_brand_list=sub_brands, old_tables=False,
                                                                          new_tables=False)
        if results_list:
            for result in results_list:
                self.common_v2.write_to_db_result(**result)

    def calculate_block_together(self):
        results_list = self.global_gen.diageo_global_block_together(Const.BRAND_BLOCKING_BRAND_FROM_CATEGORY, self.brand_blocking_template)
        if results_list:
            for result in results_list:
                self.common.write_to_db_result_new_tables(**result)

    def calculate_activation_standard(self):
        result = self.global_gen.diageo_global_activation_standard_function(kpi_scores, set_scores, local_templates_path)
        # needs work

    def calculate_relative_position(self):
        # returns list of dict
        results_list = self.global_gen.diageo_global_relative_position_function(self.relative_positioning_template)
        if results_list:
            for result in results_list:
                self.common.write_to_db_result_new_tables(**result)

    def get_match_display(self):
        """
        This function extracts the display matches data and saves it into one global data frame.
        The data is taken from probedata.match_display_in_scene.
        """
        query = DIAGEOQueries.get_match_display(self.session_uid)
        match_display = pd.read_sql_query(query, self.rds_conn.db)
        return match_display

    def save_level2_and_level3(self, set_name, kpi_name, score):
        """
        Given KPI data and a score, this functions writes the score for both KPI level 2 and 3 in the DB.
        """
        kpi_data = self.kpi_static_data[(self.kpi_static_data['kpi_set_name'] == set_name) &
                                        (self.kpi_static_data['kpi_name'] == kpi_name)]
        kpi_fk = kpi_data['kpi_fk'].values[0]
        atomic_kpi_fk = kpi_data['atomic_kpi_fk'].values[0]
        self.write_to_db_result(kpi_fk, score, self.LEVEL2)
        self.write_to_db_result(atomic_kpi_fk, score, self.LEVEL3)

    def write_to_db_result(self, fk, score, level):
        """
        This function the result data frame of every KPI (atomic KPI/KPI/KPI set),
        and appends the insert SQL query into the queries' list, later to be written to the DB.
        """
        attributes = self.create_attributes_dict(fk, score, level)
        if level == self.LEVEL1:
            table = KPS_RESULT
        elif level == self.LEVEL2:
            table = KPK_RESULT
        elif level == self.LEVEL3:
            table = KPI_RESULT
        else:
            return
        query = insert(attributes, table)
        self.kpi_results_queries.append(query)

    def create_attributes_dict(self, fk, score, level):
        """
        This function creates a data frame with all attributes needed for saving in KPI results tables.

        """
        score = round(score, 2)
        if level == self.LEVEL1:
            kpi_set_name = self.kpi_static_data[self.kpi_static_data['kpi_set_fk'] == fk]['kpi_set_name'].values[0]
            score_type = '%' if kpi_set_name in self.tools.KPI_SETS_WITH_PERCENT_AS_SCORE else ''
            attributes = pd.DataFrame([(kpi_set_name, self.session_uid, self.store_id, self.visit_date.isoformat(),
                                        format(score, '.2f'), score_type, fk)],
                                      columns=['kps_name', 'session_uid', 'store_fk', 'visit_date', 'score_1',
                                               'score_2', 'kpi_set_fk'])

        elif level == self.LEVEL2:
            kpi_name = self.kpi_static_data[self.kpi_static_data['kpi_fk'] == fk]['kpi_name'].values[0].replace("'", "\\'")
            attributes = pd.DataFrame([(self.session_uid, self.store_id, self.visit_date.isoformat(),
                                        fk, kpi_name, score)],
                                      columns=['session_uid', 'store_fk', 'visit_date', 'kpi_fk', 'kpk_name', 'score'])
        elif level == self.LEVEL3:
            data = self.kpi_static_data[self.kpi_static_data['atomic_kpi_fk'] == fk]
            atomic_kpi_name = data['atomic_kpi_name'].values[0].replace("'", "\\'")
            kpi_fk = data['kpi_fk'].values[0]
            kpi_set_name = self.kpi_static_data[self.kpi_static_data['atomic_kpi_fk'] == fk]['kpi_set_name'].values[0]
            attributes = pd.DataFrame([(atomic_kpi_name, self.session_uid, kpi_set_name, self.store_id,
                                        self.visit_date.isoformat(), datetime.utcnow().isoformat(),
                                        score, kpi_fk, fk, None, None)],
                                      columns=['display_text', 'session_uid', 'kps_name', 'store_fk', 'visit_date',
                                               'calculation_time', 'score', 'kpi_fk', 'atomic_kpi_fk', 'threshold',
                                               'result'])
        else:
            attributes = pd.DataFrame()
        return attributes.to_dict()

    def commit_results_data(self):
        # print('success')
        self.common.commit_results_data_to_new_tables()
        self.common_v2.commit_results_data()
