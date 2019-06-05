
import pandas as pd
from datetime import datetime
import os
from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
from Trax.Cloud.Services.Connector.Keys import DbUsers
from KPIUtils_v2.DB.PsProjectConnector import PSProjectConnector
from Trax.Utils.Logging.Logger import Log
from Trax.Data.Utils.MySQLservices import get_table_insertion_query as insert

from KPIUtils.DIAGEO.ToolBox import DIAGEOToolBox
from KPIUtils.GlobalProjects.DIAGEO.Utils.Fetcher import DIAGEOQueries
from KPIUtils.GlobalProjects.DIAGEO.KPIGenerator import DIAGEOGenerator
from KPIUtils.GlobalProjects.DIAGEO.Utils.ParseTemplates import parse_template
from KPIUtils.DB.Common import Common
from KPIUtils_v2.DB.CommonV2 import Common as CommonV2
from OutOfTheBox.Calculations.ManufacturerSOS import ManufacturerFacingsSOSInWholeStore, \
    ManufacturerFacingsSOSPerSubCategoryInStore
from OutOfTheBox.Calculations.SubCategorySOS import SubCategoryFacingsSOSPerCategory


__author__ = 'Nimrod'

KPI_RESULT = 'report.kpi_results'
KPK_RESULT = 'report.kpk_results'
KPS_RESULT = 'report.kps_results'
RELATIVE_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', 'Data', 'Relative Position.xlsx')


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


class DIAGEOUK_SANDToolBox:

    LEVEL1 = 1
    LEVEL2 = 2
    LEVEL3 = 3

    def __init__(self, data_provider, output):
        self.k_engine = BaseCalculationsScript(data_provider, output)
        self.data_provider = data_provider
        self.project_name = self.data_provider.project_name
        self.session_uid = self.data_provider.session_uid
        self.products = self.data_provider[Data.PRODUCTS]
        self.all_products = self.data_provider[Data.ALL_PRODUCTS]
        self.match_product_in_scene = self.data_provider[Data.MATCHES]
        self.visit_date = self.data_provider[Data.VISIT_DATE]
        self.session_info = self.data_provider[Data.SESSION_INFO]
        self.rds_conn = PSProjectConnector(self.project_name, DbUsers.CalculationEng)
        self.store_info = self.data_provider[Data.STORE_INFO]
        self.store_channel = self.store_info['store_type'].values[0]
        if self.store_channel:
            self.store_channel = self.store_channel.upper()
        self.store_type = self.store_info['additional_attribute_1'].values[0]
        self.scene_info = self.data_provider[Data.SCENES_INFO]
        self.store_id = self.data_provider[Data.STORE_FK]
        self.scif = self.data_provider[Data.SCENE_ITEM_FACTS]
        self.match_display_in_scene = self.get_match_display()
        self.set_templates_data = {}
        self.kpi_static_data = self.get_kpi_static_data()
        self.kpi_results_queries = []
        self.output = output
        self.common = Common(self.data_provider)
        self.commonV2 = CommonV2(self.data_provider)
        self.global_gen = DIAGEOGenerator(self.data_provider, self.output, self.common)
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
        log_runtime('Updating templates')(self.tools.update_templates)()
        # SOS Out Of The Box kpis
        self.activate_ootb_kpis()

        # Global assortment kpis
        assortment_res_dict = self.diageo_generator.diageo_global_assortment_function_v2()
        self.commonV2.save_json_to_new_tables(assortment_res_dict)

        # Global assortment kpis - v3 for NEW MOBILE REPORTS use
        assortment_res_dict_v3 = self.diageo_generator.diageo_global_assortment_function_v3()
        self.commonV2.save_json_to_new_tables(assortment_res_dict_v3)

        for set_name in set_names:
            set_score = 0
            if set_name not in self.tools.KPI_SETS_WITHOUT_A_TEMPLATE and set_name not in self.set_templates_data.keys():
                try:
                    self.set_templates_data[set_name] = self.tools.download_template(set_name)
                except:
                    Log.warning("Couldn't find a template for set name: " + str(set_name))
                    continue

            # Global relative position
            if set_name in ('Relative Position'):
                # Global function
                res_dict = self.diageo_generator.diageo_global_relative_position_function(
                                    self.set_templates_data[set_name], location_type='template_group')
                self.commonV2.save_json_to_new_tables(res_dict)

                # Saving to old tables
                self.set_templates_data[set_name] = parse_template(RELATIVE_PATH, lower_headers_row_index=2)
                set_score = self.calculate_relative_position_sets(set_name)
            # elif set_name in ('MPA', 'New Products', 'Local MPA'):
            elif set_name in ('Local MPA'):
                set_score = self.calculate_assortment_sets(set_name)

            # Global Secondary Displays
            elif set_name in ('Secondary Displays', 'Secondary'):
                # Global function
                res_json = self.diageo_generator.diageo_global_secondary_display_secondary_function()
                if res_json:
                    # Saving to new tables
                    self.commonV2.write_to_db_result(fk=res_json['fk'], numerator_id=1, denominator_id=self.store_id,
                                                     result=res_json['result'])

                # Saving to old tables
                set_score = self.tools.calculate_number_of_scenes(location_type='Secondary')
                if not set_score:
                    set_score = self.tools.calculate_number_of_scenes(location_type='Secondary Shelf')
                self.save_level2_and_level3(set_name, set_name, set_score)
            elif set_name == 'POSM':
                set_score = self.calculate_posm_sets(set_name)
            elif set_name in ('Visible to Customer', 'Visible to Consumer %'):
                # Global function
                sku_list = filter(None, self.scif[self.scif['product_type'] == 'SKU'].product_ean_code.tolist())
                res_dict = self.diageo_generator.diageo_global_visible_percentage(sku_list)

                if res_dict:
                    # Saving to new tables
                    parent_res = res_dict[-1]
                    self.commonV2.save_json_to_new_tables(res_dict)

                    # Saving to old tables
                    # result = parent_res['result']
                    # self.save_level2_and_level3(set_name=set_name, kpi_name=set_name, score=result)

                # Saving to old tables
                filters = {self.tools.VISIBILITY_PRODUCTS_FIELD: 'Y'}
                set_score = self.tools.calculate_visible_percentage(visible_filters=filters)
                self.save_level2_and_level3(set_name, set_name, set_score)
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

    def save_level2_and_level3(self, set_name, kpi_name, score):
        """
        Given KPI data and a score, this functions writes the score for both KPI level 2 and 3 in the DB.
        """
        kpi_data = self.kpi_static_data[(self.kpi_static_data['kpi_set_name'].str.encode('utf-8') ==
                                         set_name.encode('utf-8')) & (self.kpi_static_data['kpi_name'].str.
                                                                      encode('utf-8') == kpi_name.encode('utf-8'))]
        try:
            kpi_fk = kpi_data['kpi_fk'].values[0]
        except:
            Log.warning("kpi name or set name don't exist")
            return
        atomic_kpi_fk = kpi_data['atomic_kpi_fk'].values[0]
        self.write_to_db_result(kpi_fk, score, self.LEVEL2)
        self.write_to_db_result(atomic_kpi_fk, score, self.LEVEL3)

    def calculate_relative_position_sets(self, set_name):
        """
        This function calculates every relative-position-typed KPI from the relevant sets, and returns the set final score.
        """

        scores = []
        for i in xrange(len(self.set_templates_data[set_name])):
            params = self.set_templates_data[set_name].iloc[i]
            if self.store_channel == params.get(self.tools.CHANNEL, '').upper():
                scif_tested_param = 'brand_name' if params.get(self.tools.TESTED_TYPE, '') == self.tools.BRAND \
                    else 'product_ean_code'
                scif_anchor_param = 'brand_name' if params.get(self.tools.ANCHOR_TYPE, '') == self.tools.BRAND \
                    else 'product_ean_code'
                tested_filters = {scif_tested_param: params.get(self.tools.TESTED_NEW)}
                anchor_filters = {scif_anchor_param: params.get(self.tools.ANCHOR_NEW)}

                direction_data = {'top': self._get_direction_for_relative_position(params.get(self.tools.TOP_DISTANCE)),
                                  'bottom': self._get_direction_for_relative_position(
                                      params.get(self.tools.BOTTOM_DISTANCE)),
                                  'left': self._get_direction_for_relative_position(
                                      params.get(self.tools.LEFT_DISTANCE)),
                                  'right': self._get_direction_for_relative_position(
                                      params.get(self.tools.RIGHT_DISTANCE))}
                if params.get(self.tools.LOCATION_OLD, ''):
                    general_filters = {'template_group': params.get(self.tools.LOCATION_OLD)}
                else:
                    general_filters = {}

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
                        try:
                            product_name = self.all_products[self.all_products['product_ean_code'] == product][
                                    'product_name'].values[0]
                        except Exception as e:
                            Log.warning('Product {} is not defined in the DB'.format(product))
                            continue
                        try:
                            atomic_fk = \
                            kpi_static_data[kpi_static_data['atomic_kpi_name'] == product_name]['atomic_kpi_fk'].values[
                                0]
                        except Exception as e:
                            Log.warning('Product {} is not defined in the DB'.format(product_name))
                            continue
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

    # def calculate_assortment_sets(self, set_name): # the old version. I changed it to the function of KE for local MPA.
    #     """
    #     This function calculates every Assortment-typed KPI from the relevant sets, and returns the set final score.
    #     """
    #     scores = []
    #     for params in self.set_templates_data[set_name]:
    #         if params.get(self.store_type, '').capitalize() in (self.tools.RELEVANT_FOR_STORE,
    #                                                             self.tools.OR_OTHER_PRODUCTS):
    #             object_type = self.tools.ENTITY_TYPE_CONVERTER.get(params.get(self.tools.ENTITY_TYPE),
    #                                                                'product_ean_code')
    #             objects = [str(params.get(self.tools.PRODUCT_EAN_CODE, params.get(self.tools.PRODUCT_EAN_CODE2, '')))]
    #             if params.get(self.store_type) == self.tools.OR_OTHER_PRODUCTS:
    #                 additional_objects = str(params.get(self.tools.ADDITIONAL_SKUS)).split(',')
    #                 objects.extend(additional_objects)
    #             filters = {object_type: objects}
    #             result = self.tools.calculate_assortment(**filters)
    #             score = 1 if result > 0 else 0
    #             scores.append(score)
    #
    #             self.save_level2_and_level3(set_name, params.get(self.tools.PRODUCT_NAME), score)
    #
    #     if not scores:
    #         return False
    #     set_score = (sum(scores) / float(len(scores))) * 100
    #     return set_score

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
        self.rds_conn.disconnect_rds()
        self.rds_conn.connect_rds()
        cur = self.rds_conn.db.cursor()
        delete_queries = DIAGEOQueries.get_delete_session_results_query_old_tables(self.session_uid)
        for query in delete_queries:
            cur.execute(query)
        for query in self.kpi_results_queries:
            cur.execute(query)
        self.rds_conn.db.commit()
