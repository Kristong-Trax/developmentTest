
import pandas as pd
from datetime import datetime
from OutOfTheBox.Calculations.ManufacturerSOS import ManufacturerFacingsSOSInWholeStore, \
    ManufacturerFacingsSOSPerSubCategoryInStore
from OutOfTheBox.Calculations.SubCategorySOS import SubCategoryFacingsSOSPerCategory
from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
from Trax.Cloud.Services.Connector.Keys import DbUsers
from KPIUtils_v2.DB.PsProjectConnector import PSProjectConnector
from Trax.Utils.Logging.Logger import Log
from Trax.Data.Utils.MySQLservices import get_table_insertion_query as insert
from KPIUtils.DIAGEO.ToolBox import DIAGEOToolBox
from KPIUtils.GlobalProjects.DIAGEO.Utils.Fetcher import DIAGEOQueries
from KPIUtils.GlobalProjects.DIAGEO.KPIGenerator import DIAGEOGenerator
from KPIUtils.DB.Common import Common
from KPIUtils_v2.DB.CommonV2 import Common as CommonV2

__author__ = 'Nidhin'

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


class DIAGEOIN_SANDToolBox:
    LEVEL1 = 1
    LEVEL2 = 2
    LEVEL3 = 3

    def __init__(self, data_provider, output):
        self.k_engine = BaseCalculationsScript(data_provider, output)
        self.data_provider = data_provider
        self.project_name = self.data_provider.project_name
        self.session_uid = self.data_provider.session_uid
        self.rds_conn = PSProjectConnector(self.project_name, DbUsers.CalculationEng)
        self.match_display_in_scene = self.get_match_display()
        self.output = output
        self.store_info = self.data_provider[Data.STORE_INFO]
        self.store_id = self.data_provider[Data.STORE_FK]
        self.visit_date = self.data_provider[Data.VISIT_DATE]
        self.all_products = self.data_provider[Data.ALL_PRODUCTS]
        self.match_product_in_scene = self.data_provider[Data.MATCHES]
        self.set_templates_data = {}
        self.kpi_results_queries = []
        self.store_channel = self.store_info['store_type'].values[0]
        if self.store_channel:
            self.store_channel = self.store_channel.upper()
        self.segment = self.get_business_unit_name()
        self.scif = self.data_provider[Data.SCENE_ITEM_FACTS]
        self.store_type = self.store_info['additional_attribute_1'].values[0]
        self.kpi_static_data = self.get_kpi_static_data()
        self.common = Common(self.data_provider)
        self.commonV2 = CommonV2(self.data_provider)
        self.tools = DIAGEOToolBox(self.data_provider, output, match_display_in_scene=self.match_display_in_scene)
        self.diageo_generator = DIAGEOGenerator(self.data_provider, self.output, self.common)

    def get_match_display(self):
        """
        This function extracts the display matches data and saves it into one global data frame.
        The data is taken from probedata.match_display_in_scene.
        """
        query = DIAGEOQueries.get_match_display(self.session_uid)
        match_display = pd.read_sql_query(query, self.rds_conn.db)
        return match_display

    def get_business_unit_name(self):
        """
        This function extracts the static KPI data and saves it into one global data frame.
        The data is taken from static.kpi / static.atomic_kpi / static.kpi_set.
        """
        query = DIAGEOQueries.get_business_unit_name(self.store_id)
        business_unit_name = pd.read_sql_query(query, self.rds_conn.db)
        if business_unit_name['business_unit_name'].empty:
            return ""
        else:
            return business_unit_name['business_unit_name'].values[0]

    def get_kpi_static_data(self):
        """
        This function extracts the static KPI data and saves it into one global data frame.
        The data is taken from static.kpi / static.atomic_kpi / static.kpi_set.
        """
        query = DIAGEOQueries.get_all_kpi_data()
        kpi_static_data = pd.read_sql_query(query, self.rds_conn.db)
        return kpi_static_data

    def main_calculation(self, set_names):
        """
        This function calculates the KPI results.
        """
        # # SOS Out Of The Box kpis
        self.activate_ootb_kpis()

        # Global assortment kpis
        assortment_res_dict = self.diageo_generator.diageo_global_assortment_function_v3()
        self.commonV2.save_json_to_new_tables(assortment_res_dict)

        for set_name in set_names:
            if set_name not in self.tools.KPI_SETS_WITHOUT_A_TEMPLATE and set_name not in self.set_templates_data.keys():
                try:
                    self.set_templates_data[set_name] = self.tools.download_template(set_name)
                except:
                    Log.warning("Couldn't find a template for set name: " + str(set_name))
                    continue

            if set_name in ('Secondary Displays', 'Secondary'):
                # Global function
                res_dict = self.diageo_generator.diageo_global_secondary_display_secondary_function()

                # Saving to new tables
                if res_dict:
                    self.commonV2.write_to_db_result(fk=res_dict['fk'], numerator_id=1, denominator_id=self.store_id,
                                                     result=res_dict['result'])

                # Saving to old tables
                set_score = self.tools.calculate_assortment(assortment_entity='scene_id',
                                                            location_type='Secondary Shelf')
                self.save_level2_and_level3(set_name, set_name, set_score)

            # elif set_name == 'Visible to Customer':
            #
            #     # Global function
            #     sku_list = filter(None, self.scif[self.scif['product_type'] == 'SKU'].product_ean_code.tolist())
            #     res_dict = self.diageo_generator.diageo_global_visible_percentage(sku_list)
            #
            #     if res_dict:
            #         # Saving to new tables
            #         parent_res = res_dict[-1]
            #         for r in res_dict:
            #             self.commonV2.write_to_db_result(**r)
            #
            #         # Saving to old tables
            #         set_score = result = parent_res['result']
            #         self.save_level2_and_level3(set_name=set_name, kpi_name=set_name, score=result)
            #
            #     # filters = {self.tools.VISIBILITY_PRODUCTS_FIELD: 'Y'}
            #     # set_score = self.tools.calculate_visible_percentage(visible_filters=filters)
            #     # self.save_level2_and_level3(set_name, set_name, set_score)

            # elif set_name in ('MPA', 'New Products'):
            #     set_score = self.calculate_assortment_sets(set_name)
            # elif set_name in ('POSM',):
            #     set_score = self.calculate_posm_sets(set_name)
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
        sos_brand_out_of_sub_cat_fk = self.commonV2.get_kpi_fk_by_kpi_name('SOS BRAND OUT OF SUB CATEGORY')
        sos_brand_out_of_sub_cat = self.calculate_sos_of_brand_out_of_manufacturer_in_sub_cat(
            sos_brand_out_of_sub_cat_fk)

        # Savings results in Hierarchy
        self.save_hierarchy(sos_store, sos_cat_out_of_store, sos_sub_cat_out_of_cat, sos_man_out_of_sub_cat,
                            sos_brand_out_of_sub_cat)

    def calculate_sos_of_brand_out_of_manufacturer_in_sub_cat(self, kpi_fk):
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
            kpi_identifier = "level_2_"+str(int(res['numerator_id']))
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
            parent_identifier = "level_2_"+str(int(res['denominator_id']))
            self.commonV2.write_to_db_result(fk=res['kpi_definition_fk'], numerator_id=res['numerator_id'],
                                             denominator_id=res['denominator_id'],
                                             numerator_result=res['numerator_result'],
                                             denominator_result=res['denominator_result'], result=res['result'],
                                             score=res['result'],
                                             identifier_result=kpi_identifier,
                                             identifier_parent=parent_identifier, should_enter=True)

        for i in level_4:
            res = i.to_dict
            kpi_identifier = "level_4_"+str((int(res['numerator_id']), int(res['denominator_id'])))
            parent_identifier = str(int(res['denominator_id']))
            self.commonV2.write_to_db_result(fk=res['kpi_definition_fk'], numerator_id=res['numerator_id'],
                                             denominator_id=res['denominator_id'],
                                             numerator_result=res['numerator_result'],
                                             denominator_result=res['denominator_result'], result=res['result'],
                                             score=res['result'],
                                             identifier_result=kpi_identifier,
                                             identifier_parent=parent_identifier, should_enter=True)

        for res in level_5:
            kpi_identifier = "level_5_"+str(res['identifier_result'])
            parent_identifier = "level_4_"+str(res['identifier_parent'])
            self.commonV2.write_to_db_result(fk=res['kpi_definition_fk'], numerator_id=res['numerator_id'],
                                             denominator_id=res['denominator_id'],
                                             numerator_result=res['numerator_result'],
                                             denominator_result=res['denominator_result'], result=res['result'],
                                             score=res['result'],
                                             identifier_result=kpi_identifier, identifier_parent=parent_identifier,
                                             should_enter=True)

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

    def calculate_posm_sets(self, set_name):
        """
        This function calculates every POSM-typed KPI from the relevant sets, and returns the set final score.
        """
        scores = []
        for params in self.set_templates_data[set_name]:
            if self.store_channel is None:
                break

            kpi_res = self.tools.calculate_posm(display_name=params.get(self.tools.DISPLAY_NAME))
            score = 1 if kpi_res > 0 else 0
            if params.get(self.store_type) == self.tools.RELEVANT_FOR_STORE:
                scores.append(score)

            if score == 1 or params.get(self.store_type) == self.tools.RELEVANT_FOR_STORE:
                self.save_level2_and_level3(set_name, params.get(self.tools.DISPLAY_NAME), score)

        if not scores:
            return False
        set_score = (sum(scores) / float(len(scores))) * 100
        return set_score

    def calculate_assortment_sets(self, set_name):
        """
        This function calculates every Assortment-typed KPI from the relevant sets, and returns the set final score.
        """
        scores = []
        segment = '{};{}'.format(self.store_type, self.segment)
        for params in self.set_templates_data[set_name]:
            if params.get(segment, '').capitalize() in (self.tools.RELEVANT_FOR_STORE,
                                                        self.tools.OR_OTHER_PRODUCTS):

                object_type = self.tools.ENTITY_TYPE_CONVERTER.get(params.get(self.tools.ENTITY_TYPE),
                                                                   'product_ean_code')
                objects = [str(params.get(self.tools.PRODUCT_EAN_CODE, params.get(self.tools.PRODUCT_EAN_CODE2, '')))]
                if params.get(self.store_type) == self.tools.OR_OTHER_PRODUCTS:
                    additional_objects = str(params.get(self.tools.ADDITIONAL_SKUS)).split(',')
                    objects.extend(additional_objects)
                filters = {object_type: objects}
                result = self.tools.calculate_assortment(**filters)
                score = 1 if result > 0 else 0
                scores.append(score)

                self.save_level2_and_level3(set_name, params.get(self.tools.PRODUCT_NAME), score)

        if not scores:
            return False
        set_score = (sum(scores) / float(len(scores))) * 100
        return set_score

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

    @log_runtime('Saving to DB')
    def commit_results_data(self):
        """
        This function writes all KPI results to the DB, and commits the changes.
        """
        cur = self.rds_conn.db.cursor()
        delete_queries = DIAGEOQueries.get_delete_session_results_query_old_tables(self.session_uid)
        for query in delete_queries:
            cur.execute(query)
        for query in self.kpi_results_queries:
            cur.execute(query)
        self.rds_conn.db.commit()
