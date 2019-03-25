import pandas as pd
import argparse
from KPIUtils_v2.DB.PsProjectConnector import PSProjectConnector
from Trax.Cloud.Services.Connector.Keys import DbUsers
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer, Log
from datetime import datetime, timedelta
from Trax.Data.Utils.MySQLservices import get_table_insertion_query as insert

OUTLET_ID = 'Outlet ID'
EAN_CODE = 'product_ean_code'
STORE_NUMBER = 'Store Number'
STORE_ASSORTMENT_TABLE = 'pservice.custom_osa'
INVALID_STORES = 'invalid_stores'
INVALID_PRODUCTS = 'invalid_products'


def _parse_arguments():
    parser = argparse.ArgumentParser(description='Upload assortment for Batru')
    parser.add_argument('--env', '-e', type=str, help='The environment - dev/int/prod')
    parser.add_argument('--project', '-p', type=str, required=True, help='The name of the project')
    parser.add_argument('--file', type=str, required=True, help='The assortment template')
    parser.add_argument('--date', type=str, required=False, help='Start date YYYY-MM-DD')
    parser.add_argument('--update', type=str, required=False, help='True - Partial / False - Full')
    return parser.parse_args()


class BATRU_SANDAssortment:

    def __init__(self):
        self.parsed_args = _parse_arguments()
        self.project = self.parsed_args.project
        self.rds_conn = self.rds_connect
        self.file_path = self.parsed_args.file
        self.start_date = self.parsed_args.date
        self.partial_update = self.parsed_args.update
        self.store_data = self.get_store_data
        self.all_products = self.get_product_data
        self.current_top_skus = self.get_current_top_skus
        self.stores = {}
        self.products = {}
        self.all_queries = []

        if self.start_date is None:
            self.current_date = datetime.now().date()
        else:
            self.current_date = datetime.strptime(self.start_date, '%Y-%m-%d').date()
        self.deactivate_date = self.current_date - timedelta(1)
        self.activate_date = self.current_date

        if self.partial_update in ('1', 'True', 'Yes', 'Y'):
            self.partial_update = True
        else:
            self.partial_update = False

    def upload_assortment(self):
        """
        This is the main function of the assortment.
        It does the validation and then upload the assortment.
        :return:
        """
        Log.debug("Parsing and validating the assortment template")
        is_valid, invalid_inputs = self.p1_assortment_validator()

        Log.info("Assortment upload is started")
        self.upload_store_assortment_file()
        if not is_valid:
            Log.warning("Errors were found during the template validation")
            if invalid_inputs[INVALID_STORES]:
                Log.warning("The following stores don't exist in the DB: {}"
                            "".format(invalid_inputs[INVALID_STORES]))
            if invalid_inputs[INVALID_PRODUCTS]:
                Log.warning("The following products don't exist in the DB: {}"
                            "".format(invalid_inputs[INVALID_PRODUCTS]))
        Log.info("Assortment upload is finished")

    @property
    def rds_connect(self):
        self.rds_conn = PSProjectConnector(self.project, DbUsers.CalculationEng)
        try:
            pd.read_sql_query('select pk from probedata.session limit 1', self.rds_conn.db)
        except Exception as e:
            self.rds_conn.disconnect_rds()
            self.rds_conn = PSProjectConnector(self.project, DbUsers.CalculationEng)
        return self.rds_conn

    @property
    def get_store_data(self):
        query = "select pk as store_fk, store_number_1 as store_number from static.stores"
        self.store_data = pd.read_sql_query(query, self.rds_conn.db)
        return self.store_data

    @property
    def get_product_data(self):
        query = "select pk as product_fk, product_ean_code from static.product " \
                "where delete_date is null"
        self.all_products = pd.read_sql_query(query, self.rds_conn.db)
        return self.all_products

    @property
    def get_current_top_skus(self):
        query = """select store_fk, product_fk
                   from pservice.custom_osa
                   where end_date is null"""
        data = pd.read_sql_query(query, self.rds_conn.db)
        return data

    def p1_assortment_validator(self):
        """
        This function validates the store assortment template.
        It compares the OUTLET_ID (= store_number_1) and the products ean_code to the stores and products from the DB
        :return: False in case of an error and True in case of a valid template
        """
        raw_data = self.parse_assortment_template()
        legal_template = True
        invalid_inputs = {INVALID_STORES: [], INVALID_PRODUCTS: []}
        valid_stores = self.store_data.loc[self.store_data['store_number'].isin(
            raw_data[OUTLET_ID])]
        if len(valid_stores) != len(raw_data[OUTLET_ID].unique()):
            invalid_inputs[INVALID_STORES] = list(
                set(raw_data[OUTLET_ID].unique()) - set(valid_stores['store_number']))
            Log.debug("The following stores don't exist in the DB: {}".format(
                invalid_inputs[INVALID_STORES]))
            legal_template = False

        valid_product = self.all_products.loc[self.all_products[EAN_CODE].isin(raw_data[EAN_CODE])]
        if len(valid_product) != len(raw_data[EAN_CODE].unique()):
            invalid_inputs[INVALID_PRODUCTS] = list(
                set(raw_data[EAN_CODE].unique()) - set(valid_product[EAN_CODE]))
            Log.debug("The following products don't exist in the DB: {}".format(
                invalid_inputs[INVALID_PRODUCTS]))
            legal_template = False
        return legal_template, invalid_inputs

    def parse_assortment_template(self):
        """
        This functions turns the csv into DF
        It tries to handle all of the possible format situation that I encountered yet (different delimiter and unicode)
        :return: DF that contains the store_number_1 (Outlet ID) and the product_ean_code of the assortments
        """
        data = pd.read_csv(self.file_path, sep='\t')
        if OUTLET_ID not in data.columns or EAN_CODE not in data.columns:
            data = pd.read_csv(self.file_path)
        if OUTLET_ID not in data.columns or EAN_CODE not in data.columns:
            data = pd.read_csv(self.file_path, encoding='utf-7')
        data = data.drop_duplicates(subset=data.columns, keep='first')
        data = data.fillna('')
        return data

    def set_end_date_for_irrelevant_assortments(self, stores_list):
        """
        This function sets an end_date to all of the irrelevant stores in the assortment.
        :param stores_list: List of the stores from the assortment template
        """
        Log.debug("Closing assortment for stores out of template")
        irrelevant_stores = self.store_data.loc[~self.store_data['store_number'].isin(
            stores_list)]['store_fk'].unique().tolist()
        current_assortment_stores = self.current_top_skus['store_fk'].unique().tolist()
        stores_to_remove = list(set(irrelevant_stores).intersection(set(current_assortment_stores)))
        for store in stores_to_remove:
            query = [self.get_store_deactivation_query(store, self.deactivate_date)]
            self.commit_results(query)
        Log.debug("Assortment is closed for ({}) stores".format(len(stores_to_remove)))

    def upload_store_assortment_file(self):
        raw_data = self.parse_assortment_template()
        data = []
        list_of_stores = raw_data[OUTLET_ID].unique().tolist()

        if not self.partial_update:
            self.set_end_date_for_irrelevant_assortments(list_of_stores)

        Log.debug("Preparing assortment data for update")
        store_counter = 0
        for store in list_of_stores:
            store_data = {}
            store_products = raw_data.loc[raw_data[OUTLET_ID] == store][EAN_CODE].tolist()
            store_data[store] = store_products
            data.append(store_data)

            store_counter += 1
            if store_counter % 1000 == 0 or store_counter == len(list_of_stores):
                Log.debug(
                    "Assortment is prepared for {}/{} stores".format(store_counter, len(list_of_stores)))

        Log.debug("Updating assortment data in DB")
        store_counter = 0
        for store_data in data:

            self.update_db_from_json(store_data)

            if self.all_queries:
                queries = self.merge_insert_queries(self.all_queries)
                self.commit_results(queries)
                self.all_queries = []

            store_counter += 1
            if store_counter % 1000 == 0 or store_counter == len(data):
                Log.debug("Assortment is updated in DB for {}/{} stores".format(store_counter, len(data)))

    @staticmethod
    def merge_insert_queries(queries):
        """
        This function aggregates all of the insert queries
        :param queries: all of the queries (update and insert) for the assortment
        :return: The merged insert queries
        """
        query_groups = {}
        other_queries = []
        for query in queries:
            if 'VALUES' not in query:
                other_queries.append(query)
                continue
            static_data, inserted_data = query.split('VALUES ')
            if static_data not in query_groups:
                query_groups[static_data] = []
            query_groups[static_data].append(inserted_data)
        merged_queries = []
        for group in query_groups:
            for group_index in xrange(0, len(query_groups[group]), 10 ** 4):
                merged_queries.append('{0} VALUES {1}'.format(group, ',\n'.join(query_groups[group]
                                                                                [group_index:group_index + 10 ** 4])))
        return other_queries + merged_queries

    def update_db_from_json(self, data):
        update_products = set()
        missing_products = set()

        store_number = data.keys()[0]
        if store_number is None:
            Log.debug("'{}' column or value is missing".format(STORE_NUMBER))
            return

        store_fk = self.get_store_fk(store_number)
        if store_fk is None:
            Log.debug('Store Number {} does not exist in DB'.format(store_number))
            return

        for key in data[store_number]:
            validation = False
            if isinstance(key, (float, int)):
                validation = True
            elif isinstance(key, (str, unicode)):
                validation = True
            if validation:
                product_ean_code = str(key).split(',')[-1]
                product_fk = self.get_product_fk(product_ean_code)
                if product_fk is None:
                    missing_products.add(product_ean_code)
                else:
                    update_products.add(product_fk)

        if missing_products:
            Log.debug('The following EAN Codes for Store Number {} do not exist in DB: {}.'
                      ''.format(store_number, list(missing_products)))
        queries = []
        current_products = self.current_top_skus[self.current_top_skus['store_fk']
                                                 == store_fk]['product_fk'].tolist()

        products_to_deactivate = tuple(set(current_products).difference(update_products))
        products_to_activate = tuple(set(update_products).difference(current_products))

        if products_to_deactivate:
            if len(products_to_deactivate) == 1:
                queries.append(self.get_deactivation_query(
                    store_fk, "(" + str(products_to_deactivate[0]) + ")", self.deactivate_date))
            else:
                queries.append(self.get_deactivation_query(
                    store_fk, tuple(products_to_deactivate), self.deactivate_date))

        for product_fk in products_to_activate:
            queries.append(self.get_activation_query(store_fk, product_fk, self.activate_date))

        self.all_queries.extend(queries)
        Log.debug('Store Number {} - Products to update {}: Deactivated {}, Activated {}'
                  ''.format(store_number, len(update_products), len(products_to_deactivate), len(products_to_activate)))

    def get_store_fk(self, store_number):
        """
        This functions returns the store's fk
        :param store_number: 'store_number_1' attribute of the store
        :return: store fk
        """
        store_number = str(store_number)
        if store_number in self.stores:
            store_fk = self.stores[store_number]
        else:
            store_fk = self.store_data[self.store_data['store_number'] == store_number]
            if not store_fk.empty:
                store_fk = store_fk['store_fk'].values[0]
                self.stores[store_number] = store_fk
            else:
                store_fk = None
        return store_fk

    def get_product_fk(self, product_ean_code):
        product_ean_code = str(product_ean_code).strip()
        if product_ean_code in self.products:
            product_fk = self.products[product_ean_code]
        else:
            product_fk = self.all_products[self.all_products['product_ean_code']
                                           == product_ean_code]
            if not product_fk.empty:
                product_fk = product_fk['product_fk'].values[0]
                self.products[product_ean_code] = product_fk
            else:
                product_fk = None
        return product_fk

    @staticmethod
    def get_deactivation_query(store_fk, product_fks, date):
        query = \
            """
            update {} set end_date = '{}', is_current = NULL 
            where store_fk = {} and product_fk in {} and end_date is null;
            """\
            .format(STORE_ASSORTMENT_TABLE, date, store_fk, product_fks)
        return query

    @staticmethod
    def get_store_deactivation_query(store_fk, date):
        query = \
            """
            update {} set end_date = '{}', is_current = NULL
            where store_fk = {} and end_date is null;
            """.format(STORE_ASSORTMENT_TABLE, date, store_fk)
        return query

    @staticmethod
    def get_activation_query(store_fk, product_fk, date):
        attributes = pd.DataFrame([(store_fk, product_fk, str(date), 1)],
                                  columns=['store_fk', 'product_fk', 'start_date', 'is_current'])
        query = insert(attributes.to_dict(), STORE_ASSORTMENT_TABLE)
        return query

    def commit_results(self, queries):
        """
        This function commits the results into the DB in batches.
        query_num is the number of queires that were executed in the current batch
        After batch_size is reached, the function re-connects the DB and cursor.
        """
        self.rds_conn.connect_rds()
        cursor = self.rds_conn.db.cursor()
        batch_size = 1000
        query_num = 0
        failed_queries = []
        for query in queries:
            try:
                cursor.execute(query)
                # print query
            except Exception as e:
                Log.warning('Committing to DB failed to due to: {}. Query: {}'.format(e, query))
                self.rds_conn.db.commit()
                failed_queries.append(query)
                self.rds_conn.connect_rds()
                cursor = self.rds_conn.db.cursor()
                continue
            if query_num > batch_size:
                self.rds_conn.db.commit()
                self.rds_conn.connect_rds()
                cursor = self.rds_conn.db.cursor()
                query_num = 0
            query_num += 1
        self.rds_conn.db.commit()


if __name__ == '__main__':
    LoggerInitializer.init('Upload assortment for BATRU')
    BATRU_SANDAssortment().upload_assortment()
    # # # To run it locally just copy: -e prod -p batru --file your_file_path --date YYYY-MM-DD_start_date --update 1/0_partial/full_update to the configuration
