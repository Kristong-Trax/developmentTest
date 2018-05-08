
import os
import datetime
import pandas as pd

from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
from Trax.Cloud.Services.Connector.Keys import DbUsers
from Trax.Data.Projects.ProjectConnector import AwsProjectConnector
from Trax.Utils.Logging.Logger import Log
from Trax.Data.Utils.MySQLservices import get_table_insertion_query as insert

from Projects.PNGMCCN_SAND.GeneralToolBox import PNGMCCN_SANDGENERALToolBox
from Projects.PNGMCCN_SAND.Utils.Fetcher import PNGMCCN_SANDQueries


__author__ = 'Nimrod'

KPI_RESULT = 'report.kpi_results'
KPK_RESULT = 'report.kpk_results'
KPS_RESULT = 'report.kps_results'

TEMPLATES_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', 'Data')
SEPARATOR = ','
PNG_MANUFACTURER_FK = 3
GENERAL_SETS = ['A Counter', 'B Counter']
PNG_STORE_TYPES = ['SMM', 'LMM', 'SBT', 'LBT']
PSKU = 'PSKU'

# Names of fields in the template
SET_NAME = 'KPI Level 1 Name'
KPI_NAME = 'KPI Level 2 Name'
STORE_TYPE = 'Store_Type'
LOCATION_TYPE = 'Location_Type'
KPI_TYPE = 'KPI_Type'
ENTITY = 'Entity'
MAX_PARAMS = 2
PARAMS = 'Param'
VALUES = 'Value'
LOCATION_FIELD = 'location_type'

# KPI types
NUMBER_OF_SKUS = "# of SKU's"
SHARE_OF_SHELF = 'Custom Facing SOS'
NUMBER_OF_SCENES = '# of Counters'
NUMBER_OF_VALID_SCENES = '# of Valid Counters'
SHARE_OF_BAY = 'Custom Share of Bay'

COUNTER_DISTRIBUTION = 'Counter Distribution %'

# Name of fields in the PSKU template
PRODUCT_EAN_CODE = 'Product EAN'
RELEVANT_FOR_STORE = 'Y'
IRRELEVANT_FOR_STORE = 'N'


class PNGMCCN_SANDToolBox:
    LEVEL1 = 1
    LEVEL2 = 2
    LEVEL3 = 3

    def __init__(self, data_provider, output):
        self.k_engine = BaseCalculationsScript(data_provider, output)
        self.data_provider = data_provider
        self.project_name = self.data_provider.project_name
        self.session_uid = self.data_provider.session_uid
        self.rds_conn = AwsProjectConnector(self.project_name, DbUsers.CalculationEng)

        self.product_attributes = self.get_missing_attributes_data()
        self.products = self.data_provider[Data.PRODUCTS]
        self.products = self.products.merge(self.product_attributes, how='left', on='product_fk')
        self.all_products = self.data_provider[Data.ALL_PRODUCTS]
        self.all_products = self.all_products.merge(self.product_attributes, how='left', on='product_fk')
        self.scif = self.data_provider[Data.SCENE_ITEM_FACTS]
        self.scif = self.scif.merge(self.product_attributes, how='left', on='product_fk')

        self.matches = self.data_provider[Data.MATCHES]
        self.matches = self.matches.merge(self.products, how='left', on='product_fk')
        self.matches = self.matches.merge(self.scif[['scene_fk', LOCATION_FIELD]].drop_duplicates(),
                                          how='left', on='scene_fk')

        self.visit_date = self.data_provider[Data.VISIT_DATE]
        self.session_info = self.data_provider[Data.SESSION_INFO]
        self.scene_info = self.data_provider[Data.SCENES_INFO]
        self.store_type = self.data_provider[Data.STORE_INFO].store_type.values[0]
        self.store_id = self.data_provider[Data.STORE_FK]
        self.kpi_static_data = self.get_kpi_static_data()
        self.tools = PNGMCCN_SANDGENERALToolBox(self.data_provider, output, self.kpi_static_data, geometric_kpi_flag=False)
        self.template_data = self.tools.get_json_data('{}/Template.xlsx'.format(TEMPLATES_PATH))
        self.power_skus = self.get_power_skus_for_store()
        self.kpi_results_queries = []

    def get_kpi_static_data(self):
        """
        This function extracts the static KPI data and saves it into one global data frame.
        The data is taken from static.kpi / static.atomic_kpi / static.kpi_set.
        """
        query = PNGMCCN_SANDQueries.get_all_kpi_data()
        kpi_static_data = pd.read_sql_query(query, self.rds_conn.db)
        return kpi_static_data

    def get_missing_attributes_data(self):
        """
        This function extract 'att3' from static.product. (it's currently missing from the Data Provider.)
        """
        query = PNGMCCN_SANDQueries.get_attributes_data()
        product_attributes = pd.read_sql_query(query, self.rds_conn.db)
        return product_attributes

    def get_power_skus_for_store(self):
        """
        This function extracts the different lists of Power SKUs (for each store type) from the PSKU template.
        """
        pskus = self.tools.get_json_data('{}/PSKUs.xlsx'.format(TEMPLATES_PATH), skiprows=2)
        pskus_for_store = {key: [] for key in PNG_STORE_TYPES}
        for store_type in pskus_for_store.keys():
            for product in pskus:
                if product.get(store_type) == RELEVANT_FOR_STORE:
                    pskus_for_store[store_type].append(str(product.get(PRODUCT_EAN_CODE)))
        return pskus_for_store

    def main_calculation(self):
        """
        This function calculates the KPI results.
        """
        for params in self.template_data:
            if params.get(KPI_NAME) == COUNTER_DISTRIBUTION:    # This particular KPI is calculated later
                continue

            if self.store_type in params.get(STORE_TYPE).split(SEPARATOR):
                if self.validate_category_and_location(params):
                    kpi_type = params.get(KPI_TYPE)
                    kpi_data = self.kpi_static_data[(self.kpi_static_data['kpi_set_name'] == params.get(SET_NAME)) &
                                                    (self.kpi_static_data['kpi_name'] == params.get(KPI_NAME))]
                    kpi_fk = kpi_data['kpi_fk'].values[0]
                    atomic_kpi_fk = kpi_data['atomic_kpi_fk'].values[0]
                    filters = self.get_filters(params)

                    if kpi_type == NUMBER_OF_SKUS:
                        assortment = self.tools.calculate_assortment(**filters)
                        number_of_products = self.get_total_number_of_products(filters)
                        if number_of_products > 0:
                            score = (assortment / float(number_of_products)) * 100
                        else:
                            score = 0

                    elif kpi_type == SHARE_OF_SHELF:
                        sos_filters = {'sub_brand_name': filters.pop('sub_brand_name')}
                        filters['manufacturer_fk'] = PNG_MANUFACTURER_FK
                        sos = self.tools.calculate_share_of_shelf(sos_filters, self.tools.EXCLUDE_EMPTY, **filters)
                        score = sos * 100

                    elif kpi_type == NUMBER_OF_SCENES:
                        score = self.tools.calculate_number_of_scenes(**filters)

                    elif kpi_type == NUMBER_OF_VALID_SCENES:
                        score = self.calculate_number_of_valid_scenes(params, filters)

                        # saving counter-distribution KPI results to the DB (this KPI is directly affected by the above)
                        distribution_score = 1 if score > 0 else 0
                        distribution = self.kpi_static_data[(self.kpi_static_data['kpi_set_name'] == params.get(SET_NAME)) &
                                                            (self.kpi_static_data['kpi_name'] == COUNTER_DISTRIBUTION)]
                        self.write_to_db_result(fk=distribution['kpi_fk'].values[0], score=distribution_score,
                                                level=self.LEVEL2)
                        self.write_to_db_result(fk=distribution['atomic_kpi_fk'].values[0], score=distribution_score,
                                                level=self.LEVEL3)

                    elif kpi_type == SHARE_OF_BAY:
                        score = self.calculate_share_of_bay(**filters)

                    else:
                        Log.warning('KPI type {} is not valid'.format(kpi_type))
                        continue

                    score = round(score, 2)
                    self.write_to_db_result(fk=kpi_fk, score=score, level=self.LEVEL2)
                    self.write_to_db_result(fk=atomic_kpi_fk, score=score, level=self.LEVEL3)

    def validate_category(self, category):
        """
        This function checks whether the session contains at least one facing of the relevant category.
        """
        if category in GENERAL_SETS:
            return True
        facings = self.scif[self.scif['category'] == category]['facings'].sum()
        if facings > 0:
            return True
        else:
            return False

    def validate_category_and_location(self, params):
        """
        This function checks whether the session contains at least one facing of the relevant category and location type.
        """
        category = params.get(SET_NAME)
        if category in GENERAL_SETS:
            return True
        facings = self.scif[(self.scif['category'] == category) &
                            (self.scif[LOCATION_FIELD].isin(params.get(LOCATION_TYPE).split(SEPARATOR)))]['facings'].sum()
        if facings > 0:
            return True
        else:
            return False

    def get_total_number_of_products(self, filters):
        """
        This function returns the number of products fitting the given filters, out of static.products
        (Later to be used as the maximum assortment for the KPI.)
        """
        filters.pop(LOCATION_FIELD, None)
        products = self.all_products[self.tools.get_filter_condition(self.all_products, **filters)]
        number_of_products = len(products['product_ean_code'].unique())
        if number_of_products == 0:
            Log.warning('filters {} don\'t match any static product.'.format(filters))
        return number_of_products

    def get_filters(self, params):
        """
        This function extracts the SKU's filters for the template excel.
        """
        filters = {LOCATION_FIELD: params.get(LOCATION_TYPE).split(SEPARATOR)}
        for p in xrange(1, MAX_PARAMS+1):
            param = params.get("{}{}".format(PARAMS, p))
            if param:
                if param == PSKU:
                    param = 'product_ean_code'
                    value = self.power_skus[self.store_type]
                else:
                    value = unicode(params.get("{}{}".format(VALUES, p))).split(SEPARATOR)
                filters[param] = value
        return filters

    def calculate_number_of_valid_scenes(self, params, filters):
        """
        This function checks the number of scenes in a session which contains at least one facing
        of each of the KPI's relevant products.
        """
        scenes = self.scif[self.scif[LOCATION_FIELD].isin(params.get(LOCATION_TYPE).split(SEPARATOR))]['scene_id'].unique()
        valid_scenes = []
        for scene in scenes:
            number_of_unique_products_in_scene = self.tools.calculate_assortment(**filters)
            total_number_of_products = self.get_total_number_of_products(filters)
            if number_of_unique_products_in_scene == total_number_of_products:
                valid_scenes.append(scene)
        return len(valid_scenes)

    def calculate_share_of_bay(self, **filters):
        """
        This function calculates the Share of Bay score for given KPI data.
        """
        total_facings = 0.0
        png_facings = 0.0
        bays_for_category = 0.0
        matches = self.matches[self.tools.get_filter_condition(self.matches, **filters)]
        for scene in matches['scene_fk'].unique():
            matches_for_scene = matches[matches['scene_fk'] == scene]
            for bay in matches_for_scene['bay_number'].unique():
                matches_for_bay = matches_for_scene[matches_for_scene['bay_number'] == bay]
                bay_total_facings = len(matches_for_bay)
                total_facings += bay_total_facings
                bay_png_facings = len(matches_for_bay[matches_for_bay['manufacturer_fk'] == PNG_MANUFACTURER_FK])
                png_facings += bay_png_facings
                total_facings_for_all_categories = len(self.matches[(self.matches['scene_fk'] == scene) &
                                                                    (self.matches['bay_number'] == bay)])
                bays_for_category += bay_total_facings / float(total_facings_for_all_categories)

        result = (png_facings / total_facings) * bays_for_category
        return result

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
        if isinstance(score, tuple):
            score, result, threshold = score
        else:
            result = score
            threshold = 0
        if level == self.LEVEL1:
            kpi_set_name = self.kpi_static_data[self.kpi_static_data['kpi_set_fk'] == fk]['kpi_set_name'].values[0]
            attributes = pd.DataFrame([(kpi_set_name, self.session_uid, self.store_id, self.visit_date.isoformat(),
                                        format(score, '.2f'), fk)],
                                      columns=['kps_name', 'session_uid', 'store_fk', 'visit_date', 'score_1',
                                               'kpi_set_fk'])
        elif level == self.LEVEL2:
            kpi_name = self.kpi_static_data[self.kpi_static_data['kpi_fk'] == fk]['kpi_name'].values[0]
            attributes = pd.DataFrame([(self.session_uid, self.store_id, self.visit_date.isoformat(),
                                        fk, kpi_name, score)],
                                      columns=['session_uid', 'store_fk', 'visit_date', 'kpi_fk', 'kpk_name', 'score'])
        elif level == self.LEVEL3:
            data = self.kpi_static_data[self.kpi_static_data['atomic_kpi_fk'] == fk]
            atomic_kpi_name = data['atomic_kpi_name'].values[0]
            kpi_fk = data['kpi_fk'].values[0]
            kpi_set_name = self.kpi_static_data[self.kpi_static_data['atomic_kpi_fk'] == fk]['kpi_set_name'].values[0]
            attributes = pd.DataFrame([(atomic_kpi_name, self.session_uid, kpi_set_name, self.store_id,
                                        self.visit_date.isoformat(), datetime.datetime.utcnow().isoformat(),
                                        score, kpi_fk, fk, threshold, result)],
                                      columns=['display_text', 'session_uid', 'kps_name', 'store_fk', 'visit_date',
                                               'calculation_time', 'score', 'kpi_fk', 'atomic_kpi_fk', 'threshold',
                                               'result'])
        else:
            attributes = pd.DataFrame()
        return attributes.to_dict()

    def commit_results_data(self):
        """
        This function writes all KPI results to the DB, and commits the changes.
        """
        cur = self.rds_conn.db.cursor()
        delete_queries = PNGMCCN_SANDQueries.get_delete_session_results_query(self.session_uid)
        for query in delete_queries:
            cur.execute(query)
        for query in self.kpi_results_queries:
            cur.execute(query)
        self.rds_conn.db.commit()
