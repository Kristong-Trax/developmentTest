
from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Cloud.Services.Connector.Keys import DbUsers
from KPIUtils_v2.DB.PsProjectConnector import PSProjectConnector
# from Trax.Utils.Logging.Logger import Log
from Trax.Data.Utils.MySQLservices import get_table_insertion_query as insert

from KPIUtils.DB.Common import Common
from KPIUtils_v2.DB.CommonV2 import Common as CommonV2
from KPIUtils.DIAGEO.ToolBox import DIAGEOToolBox
from KPIUtils.GlobalProjects.DIAGEO.Utils.Fetcher import DIAGEOQueries
from KPIUtils.GlobalProjects.DIAGEO.KPIGenerator import DIAGEOGenerator
from Projects.DIAGEOCO.Data.Const import Const

from datetime import datetime
import pandas as pd

__author__ = 'huntery'

KPI_RESULT = 'report.kpi_results'
KPK_RESULT = 'report.kpk_results'
KPS_RESULT = 'report.kps_results'

class DIAGEOCOToolBox:
    LEVEL1 = 1
    LEVEL2 = 2
    LEVEL3 = 3

    def __init__(self, data_provider, output):
        self.output = output
        self.data_provider = data_provider
        # ----------- fix for nan types in dataprovider -----------
        all_products = self.data_provider._static_data_provider.all_products.where(
            (pd.notnull(self.data_provider._static_data_provider.all_products)), None)
        self.data_provider._set_all_products(all_products)
        self.data_provider._init_session_data(None, True)
        self.data_provider._init_report_data(self.data_provider.session_uid)
        self.data_provider._init_reporting_data(self.data_provider.session_id)
        # ----------- fix for nan types in dataprovider -----------
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
        self.store_info = self.data_provider[Data.STORE_INFO]
        self.store_channel = self.store_info['store_type'].values[0]
        if self.store_channel:
            self.store_channel = self.store_channel.upper()
        self.scif = self.data_provider[Data.SCENE_ITEM_FACTS]
        self.rds_conn = PSProjectConnector(self.project_name, DbUsers.CalculationEng)
        self.kpi_static_data = self.common.get_kpi_static_data()
        self.kpi_results_queries = []
        self.set_templates_data = {}
        self.match_display_in_scene = self.get_match_display()
        self.tools = DIAGEOToolBox(self.data_provider, output, match_display_in_scene=self.match_display_in_scene)
        self.diageo_generator = DIAGEOGenerator(self.data_provider, self.output, self.common, menu=True)

    def main_calculation(self):
        """
        This function calculates the KPI results.
        """
        set_names = ['Brand Blocking', 'Secondary Displays', 'Brand Pouring', 'TOUCH POINT',
                     'Relative Position']

        self.tools.update_templates()
        self.set_templates_data['TOUCH POINT'] = pd.read_excel(Const.TEMPLATE_PATH, Const.TOUCH_POINT_SHEET_NAME,
                                                               header=Const.TOUCH_POINT_HEADER_ROW)

        # the manufacturer name for DIAGEO is 'Diageo' by default. We need to redefine this for DiageoCO
        self.diageo_generator.tool_box.DIAGEO = 'DIAGEO'

        # Global assortment kpis
        assortment_res_dict = self.diageo_generator.diageo_global_assortment_function_v2()
        self.common_v2.save_json_to_new_tables(assortment_res_dict)

        # Menu kpis
        menus_res_dict = self.diageo_generator.diageo_global_share_of_menu_cocktail_function()
        self.common_v2.save_json_to_new_tables(menus_res_dict)

        for set_name in set_names:
            set_score = 0

            if set_name not in self.tools.KPI_SETS_WITHOUT_A_TEMPLATE and set_name not in self.set_templates_data.keys():
                self.set_templates_data[set_name] = self.tools.download_template(set_name)

            if set_name == 'Secondary Displays':
                result = self.diageo_generator.diageo_global_secondary_display_secondary_function()
                if result:
                    self.common_v2.write_to_db_result(**result)
                set_score = self.tools.calculate_assortment(assortment_entity='scene_id', location_type='Secondary Shelf')
                self.save_level2_and_level3(set_name, set_name, set_score)

            elif set_name == 'Brand Pouring':
                results_list = self.diageo_generator.diageo_global_brand_pouring_status_function(
                    self.set_templates_data[set_name])
                self.save_results_to_db(results_list)
                set_score = self.calculate_brand_pouring_sets(set_name)

            elif set_name == 'Brand Blocking':
                results_list = self.diageo_generator.diageo_global_block_together(set_name,
                                                                            self.set_templates_data[set_name])
                self.save_results_to_db(results_list)
                set_score = self.calculate_block_together_sets(set_name)

            elif set_name == 'Relative Position':
                results_list = self.diageo_generator.diageo_global_relative_position_function(self.set_templates_data[set_name])
                self.save_results_to_db(results_list)
                set_score = self.calculate_relative_position_sets(set_name)

            elif set_name == 'Activation Standard':
                pass
                # result = self.global_gen.diageo_global_activation_standard_function(kpi_scores, set_scores,
                #                                                                     local_templates_path)

            elif set_name == 'TOUCH POINT':
                store_attribute = 'additional_attribute_2'
                template = self.set_templates_data[set_name].fillna(method='ffill').set_index(
                    self.set_templates_data[set_name].keys()[0])
                results_list = self.diageo_generator.diageo_global_touch_point_function(template=template,
                                                                                  old_tables=True, new_tables=False,
                                                                                  store_attribute=store_attribute)
                self.save_results_to_db(results_list)
            else:
                return

            if set_score == 0:
                pass
            elif set_score is False:
                return

            if set_name != 'TOUCH POINT': # we need to do this to prevent duplicate entries in report.kps_results
                set_fk = self.kpi_static_data[self.kpi_static_data['kpi_set_name'] == set_name]['kpi_set_fk'].values[0]
                self.write_to_db_result(set_fk, set_score, self.LEVEL1)
        return

    def save_results_to_db(self, results_list):
        if results_list:
            for result in results_list:
                if result is not None:
                    self.common_v2.write_to_db_result(**result)

    def calculate_brand_pouring_sets(self, set_name):
        """
        This function calculates every Brand-Pouring-typed KPI from the relevant sets, and returns the set final score.
        """
        scores = []
        for params in self.set_templates_data[set_name]:
            if self.tools.calculate_number_of_scenes(**{self.tools.BRAND_POURING_FIELD: 'Y'}) > 0:
                # 'Pouring' scenes
                result = self.tools.calculate_brand_pouring_status(params.get(self.tools.BRAND_NAME),
                                                                   **{self.tools.BRAND_POURING_FIELD: 'Y'})
            elif self.tools.calculate_number_of_scenes(**{self.tools.BRAND_POURING_FIELD: 'back_bar'}) > 0:
                # 'Back Bar' scenes
                result = self.tools.calculate_brand_pouring_status(params.get(self.tools.BRAND_NAME),
                                                                   **{self.tools.BRAND_POURING_FIELD: 'back_bar'})
            else:
                result = 0
            score = 1 if result else 0
            scores.append(score)

            self.save_level2_and_level3(set_name, params.get(self.tools.KPI_NAME), score)

        if not scores:
            return False
        set_score = (sum(scores) / float(len(scores))) * 100
        return set_score

    def calculate_relative_position_sets(self, set_name):
        """
        This function calculates every relative-position-typed KPI from the relevant sets, and returns the set final score.
        """
        scores = []
        for params in self.set_templates_data[set_name]:
            if self.store_channel == params.get(self.tools.CHANNEL, '').upper():
                tested_filters = {'product_ean_code': params.get(self.tools.TESTED)}
                anchor_filters = {'product_ean_code': params.get(self.tools.ANCHOR)}
                direction_data = {'top': self._get_direction_for_relative_position(params.get(self.tools.TOP_DISTANCE)),
                                  'bottom': self._get_direction_for_relative_position(params.get(self.tools.BOTTOM_DISTANCE)),
                                  'left': self._get_direction_for_relative_position(params.get(self.tools.LEFT_DISTANCE)),
                                  'right': self._get_direction_for_relative_position(params.get(self.tools.RIGHT_DISTANCE))}
                general_filters = {'template_name': params.get(self.tools.LOCATION)}
                result = self.tools.calculate_relative_position(tested_filters, anchor_filters, direction_data, **general_filters)
                score = 1 if result else 0
                scores.append(score)

                self.save_level2_and_level3(set_name, params.get(self.tools.KPI_NAME), score)

        if not scores:
            return False
        set_score = (sum(scores) / float(len(scores))) * 100
        return set_score

    def _get_direction_for_relative_position(self, value):
        """
        This function converts direction data from the template (as string) to a number.
        """
        if value == self.tools.UNLIMITED_DISTANCE:
            value = 1000
        elif not value or not str(value).isdigit():
            value = 0
        else:
            value = int(value)
        return value

    def calculate_block_together_sets(self, set_name):
        """
        This function calculates every block-together-typed KPI from the relevant sets, and returns the set final score.
        """
        scores = []
        for params in self.set_templates_data[set_name]:
            if self.store_channel == params.get(self.tools.CHANNEL, '').upper():
                filters = {'template_name': params.get(self.tools.LOCATION)}
                if params.get(self.tools.SUB_BRAND_NAME):
                    filters['sub_brand_name'] = params.get(self.tools.SUB_BRAND_NAME)
                else:
                    filters['brand_name'] = params.get(self.tools.BRAND_NAME)
                result = self.tools.calculate_block_together(**filters)
                score = 1 if result else 0
                scores.append(score)

                self.save_level2_and_level3(set_name, params.get(self.tools.KPI_NAME), score)

        if not scores:
            return False
        set_score = (sum(scores) / float(len(scores))) * 100
        return set_score

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
        # self.common.commit_results_data_to_new_tables()
        self.common_v2.commit_results_data()  # new tables

        # old tables
        cur = self.rds_conn.db.cursor()
        delete_queries = DIAGEOQueries.get_delete_session_results_query_old_tables(self.session_uid)
        for query in delete_queries:
            cur.execute(query)
        for query in self.kpi_results_queries:
            cur.execute(query)
        # needed to save Touch Point values
        for query in self.common.kpi_results_queries:
            cur.execute(query)

        # this is only needed temporarily until the global assortment function is updated to use the new commonv2 object
        insert_queries = self.common.merge_insert_queries(self.common.kpi_results_new_tables_queries)
        for query in insert_queries:
            cur.execute(query)

        self.rds_conn.db.commit()
