import pandas as pd
from datetime import datetime

from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Utils.Conf.Keys import DbUsers
from KPIUtils_v2.DB.PsProjectConnector import PSProjectConnector
from Trax.Utils.Logging.Logger import Log
from Trax.Data.Utils.MySQLservices import get_table_insertion_query as insert
from KPIUtils.DIAGEO.ToolBox import DIAGEOToolBox
from KPIUtils.GlobalProjects.DIAGEO.Utils.Fetcher import DIAGEOQueries
from KPIUtils.GlobalProjects.DIAGEO.KPIGenerator import DIAGEOGenerator
from KPIUtils.DB.Common import Common
from KPIUtils_v2.DB.CommonV2 import Common as CommonV2
from OutOfTheBox.Calculations.ManufacturerSOS import ManufacturerFacingsSOSInWholeStore, \
    ManufacturerFacingsSOSPerSubCategoryInStore
from OutOfTheBox.Calculations.SubCategorySOS import SubCategoryFacingsSOSPerCategory

__author__ = 'Yasmin'

KPI_RESULT = 'report.kpi_results'
KPK_RESULT = 'report.kpk_results'
KPS_RESULT = 'report.kps_results'

# Relative Position params
TESTED_VALUE = 'Tested Product Name'
ANCHOR_VALUE = 'Anchor Product Name'
TESTED_TYPE = 'Tested Type'
ANCHOR_TYPE = 'Anchor Type'


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


class PENAFLORARToolBox:
    LEVEL1 = 1
    LEVEL2 = 2
    LEVEL3 = 3
    ACTIVATION_STANDARD = 'Activation Standard'

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
        self.store_info = self.data_provider[Data.STORE_INFO]
        self.store_type = self.store_info['additional_attribute_1'].values[0]
        self.kpi_static_data = self.get_kpi_static_data()
        self.set_templates_data = {}
        self.match_display_in_scene = self.get_match_display()
        self.kpi_results_queries = []
        self.scores = {self.LEVEL1: {},
                       self.LEVEL2: {},
                       self.LEVEL3: {}}
        self.output = output
        self.common = Common(self.data_provider)
        self.commonV2 = CommonV2(self.data_provider)
        self.tools = DIAGEOToolBox(self.data_provider, output,
                                   match_display_in_scene=self.match_display_in_scene)  # replace the old one
        self.diageo_generator = DIAGEOGenerator(self.data_provider, self.output, self.common)

    def get_kpi_static_data(self):
        """
        This function extracts the static KPI data and saves it into one global data frame.
        The data is taken from static.kpi / static.atomic_kpi / static.kpi_set.
        """
        query = DIAGEOQueries.get_all_kpi_data()
        kpi_static_data = pd.read_sql_query(query, self.rds_conn.db)
        return kpi_static_data

    def get_match_display(self):
        """
        This function extracts the display matches data and saves it into one global data frame.
        The data is taken from probedata.match_display_in_scene.
        """
        query = DIAGEOQueries.get_match_display(self.session_uid)
        match_display = pd.read_sql_query(query, self.rds_conn.db)
        return match_display

    def main_calculation(self, set_names):
        """
        This function calculates the KPI results.
        """
        # SOS Out Of The Box kpis
        self.activate_ootb_kpis()

        # sos by scene type
        self.diageo_generator.sos_by_scene_type(self.commonV2)

        # Global assortment kpis
        assortment_res_dict = self.diageo_generator.diageo_global_assortment_function_v2()
        self.commonV2.save_json_to_new_tables(assortment_res_dict)

        # Global assortment kpis - v3 for NEW MOBILE REPORTS use
        assortment_res_dict_v3 = self.diageo_generator.diageo_global_assortment_function_v3()
        self.commonV2.save_json_to_new_tables(assortment_res_dict_v3)

        for set_name in set_names:
            if set_name not in self.tools.KPI_SETS_WITHOUT_A_TEMPLATE and set_name not in self.set_templates_data.keys():
                try:
                    self.set_templates_data[set_name] = self.tools.download_template(set_name)
                except:
                    Log.warning("Couldn't find a template for set name: " + str(set_name))
                    continue

            # Global Visible to Customer / Visible to Consumer
            if set_name in ('Visible to Customer', 'Visible to Consumer %', 'Visible to Consumer'):
                # Global function
                sku_list = filter(None, self.scif[self.scif['product_type'] == 'SKU'].product_ean_code.tolist())
                res_dict = self.diageo_generator.diageo_global_visible_percentage(sku_list)

                if res_dict:
                    # Saving to new tables
                    self.commonV2.save_json_to_new_tables(res_dict)

                # Saving to old tables
                filters = {self.tools.VISIBILITY_PRODUCTS_FIELD: 'Y'}
                set_score = self.tools.calculate_visible_percentage(visible_filters=filters)
                self.save_level2_and_level3(set_name, set_name, set_score)

            elif set_name in ('Relative Position'):
                # Global function
                res_dict = self.diageo_generator.diageo_global_relative_position_function(
                    self.set_templates_data[set_name], location_type='template_display_name')

                if res_dict:
                    # Saving to new tables
                    self.commonV2.save_json_to_new_tables(res_dict)

                set_score = self.calculate_relative_position_sets(set_name)

            else:
                continue

            if set_score == 0:
                pass
            elif set_score is False:
                continue

            set_fk = self.kpi_static_data[self.kpi_static_data['kpi_set_name'] == set_name]['kpi_set_fk'].values[0]
            self.write_to_db_result(set_fk, set_score, self.LEVEL1)

        # commiting to new tables
        self.commonV2.commit_results_data()

    def calculate_assortment_sets(self, set_name):
        """
        This function calculates every Assortment-typed KPI from the relevant sets, and returns the set final score.
        """
        scores = []
        for params in self.set_templates_data[set_name]:
            target = str(params.get(self.store_type, ''))
            if target.isdigit() or target.capitalize() in (self.tools.RELEVANT_FOR_STORE, self.tools.OR_OTHER_PRODUCTS):
                products = str(params.get(self.tools.PRODUCT_EAN_CODE,
                                          params.get(self.tools.PRODUCT_EAN_CODE2, ''))).replace(',', ' ').split()
                target = 1 if not target.isdigit() else int(target)
                kpi_name = params.get(self.tools.GROUP_NAME, params.get(self.tools.PRODUCT_NAME))
                kpi_static_data = self.kpi_static_data[(self.kpi_static_data['kpi_set_name'] == set_name) &
                                                       (self.kpi_static_data['kpi_name'] == kpi_name)]
                if len(products) > 1:
                    result = 0
                    for product in products:
                        product_score = self.tools.calculate_assortment(product_ean_code=product)
                        result += product_score
                        atomic_fk = kpi_static_data[kpi_static_data['description'] == product]['atomic_kpi_fk'].values[
                            0]
                        self.write_to_db_result(atomic_fk, product_score, level=self.LEVEL3)
                    score = 1 if result >= target else 0
                else:
                    result = self.tools.calculate_assortment(product_ean_code=products)
                    atomic_fk = kpi_static_data['atomic_kpi_fk'].values[0]
                    score = 1 if result >= target else 0
                    self.write_to_db_result(atomic_fk, score, level=self.LEVEL3)

                scores.append(score)
                kpi_fk = kpi_static_data['kpi_fk'].values[0]
                self.write_to_db_result(kpi_fk, score, level=self.LEVEL2)

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
            if self.store_info.at[0, 'additional_attribute_2'] == params.get('additional_attribute_2', 'Empty'):
                tested_filters = {params.get(TESTED_TYPE): params.get(TESTED_VALUE)}
                anchor_filters = {params.get(ANCHOR_TYPE): params.get(ANCHOR_VALUE)}
                direction_data = {'top': self._get_direction_for_relative_position(params.get(self.tools.TOP_DISTANCE)),
                                  'bottom': self._get_direction_for_relative_position(
                                      params.get(self.tools.BOTTOM_DISTANCE)),
                                  'left': self._get_direction_for_relative_position(
                                      params.get(self.tools.LEFT_DISTANCE)),
                                  'right': self._get_direction_for_relative_position(
                                      params.get(self.tools.RIGHT_DISTANCE))}
                general_filters = {'template_display_name': params.get(self.tools.LOCATION)}
                result = self.tools.calculate_relative_position(tested_filters, anchor_filters, direction_data,
                                                                **general_filters)
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

    def calculate_activation_standard(self):
        """
        This function calculates the Activation Standard KPI, and saves the result to the DB (for all 3 levels).
        """
        final_score = 0
        for params in self.tools.download_template(self.ACTIVATION_STANDARD):
            set_name = params.get(self.tools.ACTIVATION_SET_NAME)
            kpi_name = params.get(self.tools.ACTIVATION_KPI_NAME)
            target = float(params.get(self.tools.ACTIVATION_TARGET))
            target = target * 100 if target < 1 else target
            score_type = params.get(self.tools.ACTIVATION_SCORE)
            weight = float(params.get(self.tools.ACTIVATION_WEIGHT))
            if kpi_name:
                kpi_fk = self.kpi_static_data[(self.kpi_static_data['kpi_set_name'] == set_name) &
                                              (self.kpi_static_data['kpi_name'] == kpi_name)]['kpi_fk'].values[0]
                score = self.scores[self.LEVEL2].get(kpi_fk, 0)
            else:
                set_fk = self.kpi_static_data[self.kpi_static_data['kpi_set_name'] == set_name]['kpi_set_fk'].values[0]
                score = self.scores[self.LEVEL1].get(set_fk, 0)
            if score >= target:
                score = 100
            else:
                if score_type == 'PROPORTIONAL':
                    score = (score / float(target)) * 100
                else:
                    score = 0
            final_score += score * weight
            self.save_level2_and_level3(self.ACTIVATION_STANDARD, set_name, score)
        set_fk = self.kpi_static_data[self.kpi_static_data['kpi_set_name'] ==
                                      self.ACTIVATION_STANDARD]['kpi_set_fk'].values[0]
        self.write_to_db_result(set_fk, final_score, self.LEVEL1)

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

    def activate_ootb_kpis(self):
        # FACINGS_SOS_MANUFACTURER_IN_WHOLE_STORE - level 1
        sos_store_fk = self.commonV2.get_kpi_fk_by_kpi_name('SOS OUT OF STORE')
        sos_store = ManufacturerFacingsSOSInWholeStore(data_provider=self.data_provider,
                                                       kpi_definition_fk=sos_store_fk).calculate()
        # FACINGS_SOS_CATEGORY_IN_WHOLE_STORE - level 2
        sos_cat_out_of_store_fk = self.commonV2.get_kpi_fk_by_kpi_name('SOS CATEGORY OUT OF STORE')
        sos_cat_out_of_store = self.calculate_sos_of_cat_of_out_of_store_new(sos_cat_out_of_store_fk)

        # FACINGS_SOS_SUB_CATEGORY_OUT_OF_CATEGORY - level 3
        sos_sub_cat_out_of_cat_fk = self.commonV2.get_kpi_fk_by_kpi_name('SOS SUB CATEGORY OUT OF CATEGORY')
        sos_sub_cat_out_of_cat = SubCategoryFacingsSOSPerCategory(data_provider=self.data_provider,
                                                                  kpi_definition_fk=sos_sub_cat_out_of_cat_fk).calculate()

        # FACINGS_SOS_MANUFACTURER_OUT_OF_SUB_CATEGORY - level 4
        sos_man_out_of_sub_cat_fk = self.commonV2.get_kpi_fk_by_kpi_name('SOS MANUFACTURER OUT OF SUB CATEGORY')
        sos_man_out_of_sub_cat = ManufacturerFacingsSOSPerSubCategoryInStore(
            data_provider=self.data_provider, kpi_definition_fk=sos_man_out_of_sub_cat_fk).calculate()

        # FACINGS_SOS_BRAND_OUT_OF_SUB_CATEGORY_IN_WHOLE_STORE - level 5
        sos_brand_out_of_sub_cat_fk = self.commonV2.get_kpi_fk_by_kpi_name('SOS BRAND OUT OF MANUFACTURER')
        sos_brand_out_of_sub_cat = self.calculate_sos_of_brand_out_of_manufacturer_in_sub_cat(
            sos_brand_out_of_sub_cat_fk)

        # Savings results in Hierarchy
        self.save_hierarchy(sos_store, sos_cat_out_of_store, sos_sub_cat_out_of_cat, sos_man_out_of_sub_cat,
                            sos_brand_out_of_sub_cat)

    def calculate_sos_of_brand_out_of_manufacturer_in_sub_cat(self, kpi_fk):
        pass
        res_list = []
        res_dict = dict()
        # Get rid of Irrelevant and Empty types and keep only facings > 1
        filtered_scif = self.scif[
            ~self.scif['product_type'].isin(['Irrelevant', 'Empty']) & self.scif['facings_ign_stack'] > 0]

        # Filter by each Sub Category and Manufacturer
        sub_cat_fk_list = filtered_scif['sub_category_fk'].unique().tolist()
        for sub_cat in sub_cat_fk_list:
            filtered_scif_by_sub_cat = filtered_scif[filtered_scif['sub_category_fk'] == sub_cat]
            list_of_relevant_manufacturers = filtered_scif_by_sub_cat['manufacturer_fk'].unique().tolist()
            for manu_fk in list_of_relevant_manufacturers:
                filtered_scif_by_sub_cat_and_manufacturer = filtered_scif_by_sub_cat[
                    filtered_scif_by_sub_cat['manufacturer_fk'] == manu_fk]
                denominator_result = filtered_scif_by_sub_cat_and_manufacturer['facings_ign_stack'].sum()

                # Calculate results per Brand
                list_of_relevant_brands = filtered_scif_by_sub_cat_and_manufacturer['brand_fk'].unique().tolist()
                for brand_fk in list_of_relevant_brands:
                    filtered_scif_by_brand = filtered_scif_by_sub_cat_and_manufacturer[
                        filtered_scif_by_sub_cat_and_manufacturer['brand_fk'] == brand_fk]
                    facings_brand_results = filtered_scif_by_brand['facings_ign_stack'].sum()
                    result_for_brand = facings_brand_results / denominator_result

                    # Preparing the results' dictionary
                    res_dict['kpi_definition_fk'] = kpi_fk
                    res_dict['numerator_id'] = brand_fk
                    res_dict['numerator_result'] = facings_brand_results
                    res_dict['denominator_id'] = int(sub_cat)
                    res_dict['denominator_result'] = denominator_result
                    res_dict['identifier_result'] = (int(brand_fk), int(sub_cat), int(manu_fk))
                    res_dict['identifier_parent'] = int(manu_fk), (int(sub_cat))
                    res_dict['result'] = result_for_brand
                    res_dict['score'] = result_for_brand
                    res_list.append(res_dict.copy())
        return res_list

    def calculate_sos_of_cat_of_out_of_store_new(self, kpi_fk):
        res_list = []
        res_dict = dict()
        # Get rid of Irrelevant and Empty types and keep only facings ignore stacking > 1
        filtered_scif = self.scif[
            ~self.scif['product_type'].isin(['Irrelevant', 'Empty']) & self.scif['facings_ign_stack'] > 0]
        denominator_result = filtered_scif['facings_ign_stack'].sum()
        categories_fk_list = filtered_scif['category_fk'].unique().tolist()

        # Calculate result per category (using facings_ign_stack!)
        for category_fk in categories_fk_list:
            filtered_scif_by_category = filtered_scif[filtered_scif['category_fk'] == category_fk]
            facings_category_result = filtered_scif_by_category['facings_ign_stack'].sum()
            result_for_category = facings_category_result / denominator_result

            # Preparing the results' dictionary
            res_dict['kpi_definition_fk'] = kpi_fk
            res_dict['numerator_id'] = category_fk
            res_dict['numerator_result'] = facings_category_result
            res_dict['denominator_id'] = self.store_id
            res_dict['denominator_result'] = denominator_result
            res_dict['result'] = result_for_category
            res_dict['score'] = result_for_category
            res_list.append(res_dict.copy())
        return res_list

    def save_hierarchy(self, level_1, level_2, level_3, level_4, level_5):
        for i in level_1:
            res = i.to_dict
            kpi_identifier = "level_1"
            self.commonV2.write_to_db_result(fk=res['kpi_definition_fk'], numerator_id=res['numerator_id'],
                                             denominator_id=res['denominator_id'],
                                             numerator_result=res['numerator_result'],
                                             denominator_result=res['denominator_result'], result=res['result'],
                                             score=res['result'],
                                             identifier_result=kpi_identifier, should_enter=False)

        for res in level_2:
            kpi_identifier = "level_2_" + str(int(res['numerator_id']))
            parent_identifier = "level_1"
            self.commonV2.write_to_db_result(fk=res['kpi_definition_fk'], numerator_id=res['numerator_id'],
                                             denominator_id=res['denominator_id'],
                                             numerator_result=res['numerator_result'],
                                             denominator_result=res['denominator_result'], result=res['result'],
                                             score=res['result'],
                                             identifier_result=kpi_identifier,
                                             identifier_parent=parent_identifier, should_enter=True)

        for i in level_3:
            res = i.to_dict
            kpi_identifier = str(int(res['numerator_id']))
            parent_identifier = "level_2_" + str(int(res['denominator_id']))
            self.commonV2.write_to_db_result(fk=res['kpi_definition_fk'], numerator_id=res['numerator_id'],
                                             denominator_id=res['denominator_id'],
                                             numerator_result=res['numerator_result'],
                                             denominator_result=res['denominator_result'], result=res['result'],
                                             score=res['result'],
                                             identifier_result=kpi_identifier,
                                             identifier_parent=parent_identifier, should_enter=True)

        for i in level_4:
            res = i.to_dict
            kpi_identifier = "level_4_" + str((int(res['numerator_id']), int(res['denominator_id'])))
            parent_identifier = str(int(res['denominator_id']))
            self.commonV2.write_to_db_result(fk=res['kpi_definition_fk'], numerator_id=res['numerator_id'],
                                             denominator_id=res['denominator_id'],
                                             numerator_result=res['numerator_result'],
                                             denominator_result=res['denominator_result'], result=res['result'],
                                             score=res['result'],
                                             identifier_result=kpi_identifier,
                                             identifier_parent=parent_identifier, should_enter=True)

        for res in level_5:
            kpi_identifier = "level_5_" + str(res['identifier_result'])
            parent_identifier = "level_4_" + str(res['identifier_parent'])
            self.commonV2.write_to_db_result(fk=res['kpi_definition_fk'], numerator_id=res['numerator_id'],
                                             denominator_id=res['denominator_id'],
                                             numerator_result=res['numerator_result'],
                                             denominator_result=res['denominator_result'], result=res['result'],
                                             score=res['result'],
                                             identifier_result=kpi_identifier, identifier_parent=parent_identifier,
                                             should_enter=True)

    def write_to_db_result(self, fk, score, level):
        """
        This function creates the result data frame of every KPI (atomic KPI/KPI/KPI set),
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
        if level == self.LEVEL1:
            kpi_set_name = self.kpi_static_data[self.kpi_static_data['kpi_set_fk'] == fk]['kpi_set_name'].values[0]
            attributes = pd.DataFrame([(kpi_set_name, self.session_uid, self.store_id, self.visit_date.isoformat(),
                                        format(score, '.2f'), fk)],
                                      columns=['kps_name', 'session_uid', 'store_fk', 'visit_date', 'score_1',
                                               'kpi_set_fk'])
        elif level == self.LEVEL2:
            kpi_name = self.kpi_static_data[self.kpi_static_data['kpi_fk'] == fk]['kpi_name'].values[0].replace("'",
                                                                                                                "''")
            attributes = pd.DataFrame([(self.session_uid, self.store_id, self.visit_date.isoformat(),
                                        fk, kpi_name, score)],
                                      columns=['session_uid', 'store_fk', 'visit_date', 'kpi_fk', 'kpk_name', 'score'])
        elif level == self.LEVEL3:
            data = self.kpi_static_data[self.kpi_static_data['atomic_kpi_fk'] == fk]
            atomic_kpi_name = data['atomic_kpi_name'].values[0].replace("'", "''")
            kpi_fk = data['kpi_fk'].values[0]
            kpi_set_name = self.kpi_static_data[self.kpi_static_data['atomic_kpi_fk'] == fk]['kpi_set_name'].values[0]
            attributes = pd.DataFrame([(atomic_kpi_name, self.session_uid, kpi_set_name, self.store_id,
                                        self.visit_date.isoformat(), datetime.utcnow().isoformat(),
                                        score, kpi_fk, fk)],
                                      columns=['display_text', 'session_uid', 'kps_name', 'store_fk', 'visit_date',
                                               'calculation_time', 'score', 'kpi_fk', 'atomic_kpi_fk'])
        else:
            attributes = pd.DataFrame()
        return attributes.to_dict()

    @log_runtime('Saving to DB')
    def commit_results_data(self):
        """
        This function writes all KPI results to the DB, and commits the changes.
        """
        insert_queries = self.merge_insert_queries(self.kpi_results_queries)
        rds_conn = PSProjectConnector(self.project_name, DbUsers.CalculationEng)
        cur = rds_conn.db.cursor()
        delete_queries = DIAGEOQueries.get_delete_session_results_query_old_tables(self.session_uid)
        for query in delete_queries:
            cur.execute(query)
        for query in insert_queries:
            cur.execute(query)
        rds_conn.db.commit()

    @staticmethod
    def merge_insert_queries(insert_queries):
        query_groups = {}
        for query in insert_queries:
            static_data, inserted_data = query.split('VALUES ')
            if static_data not in query_groups:
                query_groups[static_data] = []
            query_groups[static_data].append(inserted_data)
        merged_queries = []
        for group in query_groups:
            merged_queries.append('{0} VALUES {1}'.format(group, ',\n'.join(query_groups[group])))
        return merged_queries
