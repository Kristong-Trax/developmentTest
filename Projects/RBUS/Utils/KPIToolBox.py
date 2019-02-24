import os
import pandas as pd
from datetime import datetime
import math

from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Utils.Conf.Keys import DbUsers
from KPIUtils_v2.DB.PsProjectConnector import PSProjectConnector
from Trax.Utils.Logging.Logger import Log
from Trax.Data.Utils.MySQLservices import get_table_insertion_query as insert
from Projects.RBUS.Utils.Fetcher import RBUSRBUSQueries
from Projects.RBUS.Utils.GeneralToolBox import RBUSRBUSGENERALToolBox

# constant names
ATOMIC_KPI_FK = 'atomic_kpi_fk'
KPI_SET_NAME = 'kpi_set_name'
ATOMIC_KPI_NAME = 'atomic_kpi_name'
SHELF_OCCUPATIONS_KPIS = 'shelf_occupations_kpis'
KPI_LEVEL_3_NAME = 'KPI Level 3 Name'
PLACEMENT_COUNT = 'Placement count'
MAIN_PLACEMENT = 'MAIN PLACEMENT'
SHELF_OCCUPATION = "Shelf occupation"
DF = 'df'
SKU = "sku"
BRAND = "brand"
EXCEL_DF = 'excel_df'
MAIN_PLACEMENT_SCENES = 'main_placement_scenes'
NUM_OF_BAYS = 'num_of_bays'
NUM_OF_SHELVES = 'num_of_shelves'
BRANDS_DICT = 'brands_dict'
SKUS_DICT = 'skus_dict'
KPI_RESULT = 'report.kpi_results'
KPK_RESULT = 'report.kpk_results'
KPS_RESULT = 'report.kps_results'
KPI_SET = 'kpi_set'
SHELF_NUMBER = 'shelf_number'
BAY_NUMBER = 'bay_number'

__author__ = 'yoava'


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


def get_face_count(face_count):
    """
    get number of face count
    :param face_count: NaN or integer
    :return: number of face count
    """
    if math.isnan(float(face_count)):
        return 1
    else:
        return face_count


def get_curr_probe(df, shelf_number, bay_number, main_placement_scenes):
    """
    get the current probe from combination of shelf,bay,template,and data frame
    :param df: match product in scene
    :param shelf_number: shelf number
    :param bay_number: bay number
    :param main_placement_scenes: main placement scenes list
    :return: Data frame combined of all above params
    """
    curr_probe = df[(df[SHELF_NUMBER] == shelf_number) & (df[BAY_NUMBER] == bay_number) &
                    (df['scene_fk'].isin(main_placement_scenes))]
    return curr_probe


def get_placement_count_atomic_kpi_from_template(template_name):
    """
    convert template name in scif to the appropriate atomic kpi name
    :param template_name: template name from scif
    :return: atomic kpi value if template is appropriate
    """


    # if template_name == 'ADDITIONAL AMBIENT PLACEMENT':
    #     return 'K006'
    # elif template_name == 'ADDITIONAL CHILLED PLACEMENT':
    #     return 'K007'
    # elif template_name == 'CHILLED CASHIER PLACEMENT':
    #     return 'K008'
    # elif template_name == 'CHILLED CAN COOLERS':
    #     return 'K009'
    # else:
    #     return 'no'


class RBUSRBUSToolBox:
    LEVEL1 = 1
    LEVEL2 = 2
    LEVEL3 = 3

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
        self.tools = RBUSRBUSGENERALToolBox(self.data_provider, self.output, rds_conn=self.rds_conn)
        self.kpi_static_data = self.get_kpi_static_data()
        self.kpi_results_queries = []
        self.templates_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', 'Data')
        self.template_dict = self.get_df_from_excel_path()
        self.template_dict = self.template_dict[self.template_dict['Template for KPI'] != 'none']\
                                                .set_index('Template for KPI')['KPI Level 3 Name'].to_dict()
        self.excluded_sub_categories = self.get_excluded_sub_categories(self.get_df_from_excel_path())

    def get_kpi_static_data(self):
        """
        This function extracts the static KPI data and saves it into one global data frame.
        The data is taken from static.kpi / static.atomic_kpi / static.kpi_set.
        """
        query = RBUSRBUSQueries.get_all_kpi_data()
        kpi_static_data = pd.read_sql_query(query, self.rds_conn.db)
        return kpi_static_data

    @staticmethod
    def get_scene_count(df, template_name):
        return len(df['scene_fk'][df.template_name == template_name].unique())

    def calculate_placement_count(self):
        """
        this function calculates number of occurrences for each scene type and adds the correct score to insert query
        which will be later committed to report.kpi_result db
        :return: None
        """
        df = self.scif[['template_name', 'scene_fk']]
        # iterate scene types names
        for template_name in df.template_name.unique():
            # get number of occurrences of this scene type
            value = self.get_scene_count(df, template_name)
            # get the atomic kpi name
            atomic_kpi_name = 'no'
            if template_name in self.template_dict:
                atomic_kpi_name = self.template_dict[template_name]
            if atomic_kpi_name != 'no':
                # get the atomic kpi fk of template name
                atomic_kpi_fk = self.get_atomic_kpi_fk(atomic_kpi_name, PLACEMENT_COUNT)
                Log.info(atomic_kpi_name + " " + str(atomic_kpi_fk) + " " + str(value))
                # add insert query for later use
                self.write_to_db_result(atomic_kpi_fk, value, self.LEVEL3)

    def get_product_list_field(self, kpi):
        """
        get the type to check according to excel template
        :param kpi: kpi name to check type for
        :return: type name if kpi exists
        """
        excel_df = self.get_df_from_excel_path()
        if kpi in excel_df[KPI_LEVEL_3_NAME].values:
            return excel_df['Product List Field'].loc[excel_df[KPI_LEVEL_3_NAME] == kpi].values[0]

    @staticmethod
    def get_type_dict(template_df, kpi_type):
        """
        this function returns dictionary with type and value to calculate kpi to
        :param template_df: excel file
        :param kpi_type: brand/sku
        :return: dictionary
        """
        type_dict = {}
        for i, row in template_df.iterrows():
            if str(row['Type']).strip().lower() == kpi_type:
                type_dict[row[KPI_LEVEL_3_NAME]] = row['Value']
        return type_dict

    def calculate_manufacturer(self, curr_probe, product_list_field):
        """
        this function calculates score for Red Bull manufacturer category in a specific probe
        """
        total_products = 0
        redbull_manufacturer_products = 0
        for i, row in curr_probe.iterrows():
            if row['stacking_layer'] == 1:
                facing_count = get_face_count(row.face_count)
                total_products += facing_count
                if (self.get_value_of_product_by_column_name(row, product_list_field) == u'Red Bull') & \
                        (~self.is_sub_category_excluded(row)):
                    redbull_manufacturer_products += facing_count
        return float(redbull_manufacturer_products) / total_products

    def calculate_redbull_manufacturer(self, shelf_occupation_dict, product_list_field):
        """
        this function calculates score for Red Bull manufacturer category
        it iterates shelves and bays as if they were matrix and aggregates score for each "cell" or "curr_probe"
        """
        score = 0
        for shelf_number in range(1, shelf_occupation_dict.get(NUM_OF_SHELVES) + 1):
            for bay_number in range(1, shelf_occupation_dict.get(NUM_OF_BAYS) + 1):
                # get the current probe to calculate - specific shelf, bay, and only in main_placement scene type
                curr_probe = get_curr_probe(shelf_occupation_dict.get(DF), shelf_number, bay_number,
                                            shelf_occupation_dict.get(MAIN_PLACEMENT_SCENES))
                if not curr_probe.empty:
                    score += self.calculate_manufacturer(curr_probe, product_list_field)
        Log.info("manufacturer score " + str(score))
        return score

    def calculate_category(self, curr_probe, product_list_field):
        """
        this function calculates score for energy drinks category in a specific probe
        """
        energy_products = 0
        total_products = 0
        # iterate curr_probe rows
        for i, row in curr_probe.iterrows():
            if row['stacking_layer'] == 1:
                facing_count = get_face_count(row.face_count)
                total_products += facing_count
                if (self.get_value_of_product_by_column_name(row, product_list_field) == u'Energy') & \
                        (~self.is_sub_category_excluded(row)):
                    energy_products += facing_count
        return float(energy_products) / total_products

    def calculate_energy_drinks(self, shelf_occupation_dict, product_list_field):
        """
        this function calculates score for energy drinks category
        """
        score = 0
        for shelf_number in range(1, shelf_occupation_dict.get(NUM_OF_SHELVES) + 1):
            for bay_number in range(1, shelf_occupation_dict.get(NUM_OF_BAYS) + 1):
                # get the current probe to calculate - specific shelf, bay, and only in main_placement scene type
                curr_probe = get_curr_probe(shelf_occupation_dict.get(DF), shelf_number, bay_number,
                                            shelf_occupation_dict.get(MAIN_PLACEMENT_SCENES))
                if not curr_probe.empty:
                    score += self.calculate_category(curr_probe, product_list_field)
        Log.info("category score " + str(score))
        return score

    def calculate_brand(self, curr_probe, brand_value, product_list_field):
        """
        this function iterates current probe and calculates score for kpi of type brand
        """
        total_products = 0
        curr_brand_count = 0
        for i, row in curr_probe.iterrows():
            if row['stacking_layer'] == 1:
                facing_count = get_face_count(row.face_count)
                total_products += facing_count
                sub_brand = self.get_value_of_product_by_column_name(row, product_list_field)
                if (sub_brand == brand_value) & (~self.is_sub_category_excluded(row)):
                    curr_brand_count += facing_count
        return float(curr_brand_count) / total_products

    def calculate_brand_by_name(self, kpi, shelf_occupation_dict, brand_name, product_list_field):
        """
        this function calculates the score for atomic kpi of type brand
        after it gets the score it adds query for later insert to DB
        """
        score = 0
        for shelf_number in range(1, shelf_occupation_dict.get(NUM_OF_SHELVES) + 1):
            for bay_number in range(1, shelf_occupation_dict.get(NUM_OF_BAYS) + 1):
                # get the current probe to calculate - specific shelf, bay, and only in main_placement scene type
                curr_probe = get_curr_probe(shelf_occupation_dict.get(DF), shelf_number,
                                            bay_number, shelf_occupation_dict.get(MAIN_PLACEMENT_SCENES))
                if not curr_probe.empty:
                    score += self.calculate_brand(curr_probe, brand_name, product_list_field)
        Log.info("brand score " + brand_name + " " + str(score))
        self.write_to_db_result(self.get_atomic_kpi_fk(kpi, SHELF_OCCUPATION), score, self.LEVEL3)

    def is_sub_category_excluded(self, row):
        """
        this method checks if this product sub-category is in the list of sub-categories to exclude from calculation
        :param row: represent 1 item
        :return: bool
        """
        return self.all_products.sub_category.loc[self.all_products.product_fk ==
                                                  row.product_fk].isin(self.excluded_sub_categories).values[0]

    def get_value_of_product_by_column_name(self, row, column_name):
        """
        this method calculates value rom specific column in self.all_products
        :param row: current row in curr_probe data frame(see other method to understand what is curr_probe) ,
                    represent 1 item
        :param column_name: column name ot get the value from
        :return: value
        """
        product_fk = row.product_fk
        all_products_row = self.all_products.loc[self.all_products.product_fk == product_fk]
        if column_name == 'Sub Brand':
            column_name = 'sub_brand_name'
        value = all_products_row[column_name].values[0]
        return value

    def calculate_sku(self, curr_probe, sku_name, product_list_field):
        """
        this function iterates current probe and calculates score for kpi of type sku
        """
        total_products = 0
        curr_sku_count = 0
        for i, row in curr_probe.iterrows():
            if row['stacking_layer'] == 1:
                facing_count = get_face_count(row.face_count)
                total_products += facing_count
                if (self.get_value_of_product_by_column_name(row, product_list_field) == sku_name) & \
                        (~self.is_sub_category_excluded(row)):
                    curr_sku_count += facing_count
        return float(curr_sku_count) / total_products

    def calculate_sku_by_name(self, kpi, shelf_occupation_dict, sku_name, product_list_field):
        """
        this function calculates the score for atomic kpi of type SKU
        after it gets the score it adds query for later insert to DB
        """
        score = 0
        for shelf_number in range(1, shelf_occupation_dict.get(NUM_OF_SHELVES) + 1):
            for bay_number in range(1, shelf_occupation_dict.get(NUM_OF_BAYS) + 1):
                # get the current probe to calculate - specific shelf, bay, and only in main_placement scene type
                curr_probe = get_curr_probe(shelf_occupation_dict.get(DF), shelf_number,
                                            bay_number, shelf_occupation_dict.get(MAIN_PLACEMENT_SCENES))
                if not curr_probe.empty:
                    score += self.calculate_sku(curr_probe, sku_name, product_list_field)
        Log.info("sku score " + sku_name + " " + str(score))
        self.write_to_db_result(self.get_atomic_kpi_fk(kpi, SHELF_OCCUPATION), score, self.LEVEL3)

    def get_df_from_excel_path(self):
        """
        this method finds the template path and converts it to Data frame
        :return: Data frame
        """
        # path to template file
        excel_path = os.path.join(self.templates_path, 'KPIs Template RB 180405.xlsx')
        # convert template to Data frame
        excel_df = pd.read_excel(excel_path, sheetname='Sheet1')
        return excel_df

    def calculate_shelf_occupation(self):
        """
        this function iterates the atomic kpis in Shelf occupation and "sends" each kpi to it's correct method
        for getting it's score.
        after it gets the score it adds query for later insert to DB for the first 3 KPIs
        all other LPI results will be added to the insert query later
        """
        shelf_occupation_dict = self.get_shelf_occupation_dictionary()
        manufacturer_score, category_score = 0, 0
        # iterate all atomic KPIs from this KPI set
        for kpi in shelf_occupation_dict[SHELF_OCCUPATIONS_KPIS]:
            product_list_field = self.get_product_list_field(kpi)
            # calculate redbull manufacturer score
            if kpi == 'K001':
                manufacturer_score = self.calculate_redbull_manufacturer(shelf_occupation_dict, product_list_field)
                self.write_to_db_result(self.get_atomic_kpi_fk(kpi, shelf_occupation_dict.get(KPI_SET)),
                                        manufacturer_score, self.LEVEL3)
            # calculate energy category score
            elif kpi == 'K002':
                category_score = self.calculate_energy_drinks(shelf_occupation_dict, product_list_field)
                self.write_to_db_result(self.get_atomic_kpi_fk(kpi, shelf_occupation_dict.get(KPI_SET)),
                                        category_score, self.LEVEL3)
            # calculate score for all energy drinks that are NOT from red bull manufacturer(K002 score - K001 score)
            elif kpi == 'K003':
                self.write_to_db_result(self.get_atomic_kpi_fk(kpi, shelf_occupation_dict.get(KPI_SET)),
                                        category_score - manufacturer_score, self.LEVEL3)
                Log.info("K003 Score " + str(category_score - manufacturer_score))
            # calculate brands score
            elif kpi in shelf_occupation_dict.get(BRANDS_DICT):
                brand_name = shelf_occupation_dict.get(BRANDS_DICT).get(kpi)
                self.calculate_brand_by_name(kpi, shelf_occupation_dict, brand_name, product_list_field)
            # calculate sku score
            elif kpi in shelf_occupation_dict.get(SKUS_DICT):
                sku_name = shelf_occupation_dict.get(SKUS_DICT).get(kpi)
                self.calculate_sku_by_name(kpi, shelf_occupation_dict, sku_name, product_list_field)

    def get_shelf_occupation_dictionary(self):
        """
        this method prepares dictionary that contains all necessary data for calculating shelf occupation KPI
        :return: shelf occupation dictionary
        """
        shelf_occupation_dict = {}
        # get a list of shelf occupation atomic KPIs
        shelf_occupations_kpis = self.kpi_static_data[self.kpi_static_data[KPI_SET_NAME] ==
                                                      SHELF_OCCUPATION]['atomic_kpi_name'].values
        shelf_occupation_dict[SHELF_OCCUPATIONS_KPIS] = shelf_occupations_kpis
        # get only main_placement scenes because we need only them for this set
        main_placement_scenes = self.scif.scene_id[(self.scif['template_name'] == MAIN_PLACEMENT)].unique()
        shelf_occupation_dict[MAIN_PLACEMENT_SCENES] = main_placement_scenes
        # shortcut for this table name
        df = self.match_product_in_scene
        shelf_occupation_dict[DF] = df
        # get number of shelves
        num_of_shelves = df.loc[df[SHELF_NUMBER].idxmax()][SHELF_NUMBER]
        shelf_occupation_dict[NUM_OF_SHELVES] = num_of_shelves
        # get number of bays
        num_of_bays = df.loc[df[BAY_NUMBER].idxmax()][BAY_NUMBER]
        shelf_occupation_dict[NUM_OF_BAYS] = num_of_bays
        # get the template file as Data frame
        excel_df = self.get_df_from_excel_path()
        shelf_occupation_dict[EXCEL_DF] = excel_df
        # get dictionary of brand name and values
        brands_dict = self.get_type_dict(excel_df, BRAND)
        shelf_occupation_dict[BRANDS_DICT] = brands_dict
        # get dictionary of sku name and value
        skus_dict = self.get_type_dict(excel_df, SKU)
        shelf_occupation_dict[SKUS_DICT] = skus_dict
        # kpi set for later use in "get_atomic_kpi_fk" method
        shelf_occupation_dict[KPI_SET] = SHELF_OCCUPATION
        return shelf_occupation_dict

    @staticmethod
    def get_excluded_sub_categories(excel_df):
        excluding_sub_categories = excel_df['Excluding Sub Category'].unique()
        return excluding_sub_categories

    def get_atomic_kpi_fk(self, atomic_kpi, kpi_set):
        """
        this function returns atomic_kpi_fk by kpi name
        :param atomic_kpi: atomic kpi name
        :param kpi_set: kpi set name
        :return: atomic_kpi_fk
        """
        atomic_kpi_fk = \
            self.kpi_static_data[(self.kpi_static_data[KPI_SET_NAME] == kpi_set) &
                                 (self.kpi_static_data[ATOMIC_KPI_NAME] == atomic_kpi)][ATOMIC_KPI_FK].values[0]
        return atomic_kpi_fk

    def main_calculation(self, set_name):
        """
        this function chooses the correct set name and calculates it's scores
        :param set_name: set name to calculate score for
        """
        if set_name == PLACEMENT_COUNT:
            self.calculate_placement_count()
        elif set_name == SHELF_OCCUPATION:
            self.calculate_shelf_occupation()

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
        Yoav changed it to write in level3 also to result column
        """
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
                                        self.visit_date.isoformat(), datetime.utcnow().isoformat(),
                                        score, str(score), kpi_fk, fk)],
                                      columns=['display_text', 'session_uid', 'kps_name', 'store_fk', 'visit_date',
                                               'calculation_time', 'score', 'result', 'kpi_fk', 'atomic_kpi_fk'])
        else:
            attributes = pd.DataFrame()
        return attributes.to_dict()

    @log_runtime('Saving to DB')
    def commit_results_data(self):
        """
        This function writes all KPI results to the DB, and commits the changes.
        """
        insert_queries = self.merge_insert_queries(self.kpi_results_queries)
        cur = self.rds_conn.db.cursor()
        delete_queries = RBUSRBUSQueries.get_delete_session_results_query(self.session_uid)
        for query in delete_queries:
            cur.execute(query)
        for query in insert_queries:
            cur.execute(query)
        self.rds_conn.db.commit()

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
