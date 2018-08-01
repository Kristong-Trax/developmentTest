import pandas as pd
import argparse
from Trax.Data.Projects.Connector import ProjectConnector
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
    return parser.parse_args()


class BatruAssortment:

    def __init__(self):
        self.parsed_args = _parse_arguments()
        self.project = self.parsed_args.project
        self.rds_conn = self.rds_connect
        self.file_path = self.parsed_args.file
        self.store_data = self.get_store_data
        self.all_products = self.get_product_data
        self.current_top_skus = self.get_current_top_skus
        self.stores = {}
        self.products = {}
        self.all_queries = []
        self.update_queries = []

    def upload_assortment(self):
        """
        This is the main function of the assortment.
        It does the validation and then upload the assortment.
        :return:
        """
        Log.info("Validating the assortment template")
        is_valid, invalid_inputs = self.p1_assortment_validator()
        self.upload_store_assortment_file()
        Log.info('Done uploading assortment for Batru')
        if not is_valid:
            Log.warning("Errors were found during the template validation")
            if invalid_inputs[INVALID_STORES]:
                Log.warning("The following stores doesn't exist in the DB: {}".format(invalid_inputs[INVALID_STORES]))
            if invalid_inputs[INVALID_PRODUCTS]:
                Log.warning(
                    "The following products doesn't exist in the DB: {}".format(invalid_inputs[INVALID_PRODUCTS]))

    @property
    def rds_connect(self):
        self.rds_conn = ProjectConnector(self.project, DbUsers.CalculationEng)
        try:
            pd.read_sql_query('select pk from probedata.session limit 1', self.rds_conn.db)
        except Exception as e:
            self.rds_conn.disconnect_rds()
            self.rds_conn = ProjectConnector(self.project, DbUsers.CalculationEng)
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
        valid_stores = self.store_data.loc[self.store_data['store_number'].isin(raw_data[OUTLET_ID])]
        if len(valid_stores) != len(raw_data[OUTLET_ID].unique()):
            invalid_inputs[INVALID_STORES] = list(set(raw_data[OUTLET_ID].unique()) - set(valid_stores['store_number']))
            Log.warning("Those stores don't exist in the DB: {}".format(invalid_inputs[INVALID_STORES]))
            legal_template = False

        valid_product = self.all_products.loc[self.all_products[EAN_CODE].isin(raw_data[EAN_CODE])]
        if len(valid_product) != len(raw_data[EAN_CODE].unique()):
            invalid_inputs[INVALID_PRODUCTS] = list(set(raw_data[EAN_CODE].unique()) - set(valid_product[EAN_CODE]))
            Log.warning("Those products don't exist in the DB: {}".format(invalid_inputs[INVALID_PRODUCTS]))
            legal_template = False
        return legal_template, invalid_inputs

    def parse_assortment_template(self):
        """
        This functions turns the csv into DF
        :return: DF that contains the store_number_1 (Outlet ID) and the product_ean_code of the assortments
        """
        data = pd.read_csv(self.file_path, sep='\t')
        data = data.drop_duplicates(subset=data.columns, keep='first')
        data = data.fillna('')
        if len(data.columns) != 2:
            data = pd.read_csv(self.file_path)
            data = data.drop_duplicates(subset=data.columns, keep='first')
            data = data.fillna('')
        return data

    def set_end_date_for_irrelevant_assortments(self, stores_list):
        """
        This function sets an end_date to all of the irrelevant stores in the assortment.
        :param stores_list: List of the stores from the assortment template
        """
        Log.info("Starting to set an end date for irrelevant stores")
        queries_to_execute = []
        irrelevant_stores = self.store_data.loc[
            ~self.store_data['store_number'].isin(stores_list)]['store_fk'].unique().tolist()
        current_assortment_stores = self.current_top_skus['store_fk'].unique().tolist()
        stores_to_remove = list(set(irrelevant_stores).intersection(set(current_assortment_stores)))
        for store in stores_to_remove:
            query = self.get_store_deactivation_query(store)
            queries_to_execute.append(query)
        self.commit_results(queries_to_execute)
        Log.info("Done setting end dates for irrelevant stores")

    def upload_store_assortment_file(self):
        raw_data = self.parse_assortment_template()
        data = []
        list_of_stores = raw_data[OUTLET_ID].unique().tolist()
        self.set_end_date_for_irrelevant_assortments(list_of_stores)
        for store in list_of_stores:
            store_data = {}
            store_products = raw_data.loc[raw_data[OUTLET_ID] == store][EAN_CODE].tolist()
            store_data[store] = store_products
            data.append(store_data)
        for store_data in data:
            self.update_db_from_json(store_data, immediate_change=True)
        queries = self.merge_insert_queries(self.all_queries)
        Log.info("Queries aggregation is over, starting commiting the assortment")
        self.commit_results(queries)
        Log.info("Done commiting results")

    def merge_insert_queries(self, insert_queries):
        """
        This function aggregates all of the insert queries
        :param insert_queries: all of the queries (update and insert) for the assortment
        :return: The merged insert queries
        """
        query_groups = {}
        for query in insert_queries:
            if 'update' in query:
                self.update_queries.append(query)
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
        return merged_queries

    def update_db_from_json(self, data, immediate_change=False, discard_missing_products=False):
        products = set()
        missing_products = set()
        store_number = data.keys()[0]
        if store_number is None:
            Log.warning("'{}' is required in data".format(STORE_NUMBER))
            return
        store_fk = self.get_store_fk(store_number)
        if store_fk is None:
            Log.warning('Store {} does not exist. Exiting...'.format(store_number))
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
                    Log.warning('Product EAN {} does not exist'.format(product_ean_code))
                    missing_products.add(product_ean_code)
                    continue
                products.add(product_fk)
        if missing_products and not discard_missing_products:
            Log.warning('Some EANs do not exist: {}. Exiting...'.format('; '.join(missing_products)))
            return
        if products:
            current_date = datetime.now().date()
            if immediate_change:
                deactivate_date = current_date - timedelta(1)
                activate_date = current_date
            else:
                deactivate_date = current_date
                activate_date = current_date + timedelta(1)
            queries = []
            current_skus = self.current_top_skus[self.current_top_skus['store_fk'] == store_fk]['product_fk'].tolist()
            products_to_deactivate = set(current_skus).difference(products)
            products_to_activate = set(products).difference(current_skus)
            # for product_fk in products_to_deactivate:
            if products_to_deactivate:
                if len(products_to_deactivate) != 1:
                    queries.append(
                        self.get_deactivation_query(store_fk, tuple(products_to_deactivate), deactivate_date))
                else:
                    queries.append(self.get_deactivation_query(store_fk, '({})'.format(list(products_to_deactivate)[0]),
                                                               deactivate_date))
            for product_fk in products_to_activate:
                queries.append(self.get_activation_query(store_fk, product_fk, activate_date))
            self.all_queries.extend(queries)
            Log.info('{} - Out of {} products, {} products were deactivated and {} products were activated'.format(
                store_number, len(products), len(products_to_deactivate), len(products_to_activate)))
        else:
            Log.info('{} - No products are configured as Top SKUs'.format(store_number))

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
            product_fk = self.all_products[self.all_products['product_ean_code'] == product_ean_code]
            if not product_fk.empty:
                product_fk = product_fk['product_fk'].values[0]
                self.products[product_ean_code] = product_fk
            else:
                product_fk = None
        return product_fk

    @staticmethod
    def get_deactivation_query(store_fk, product_fk, date):
        query = """update {} set end_date = '{}', is_current = NULL
                   where store_fk = {} and product_fk in {} and end_date is null;"""\
            .format(STORE_ASSORTMENT_TABLE, date, store_fk, product_fk)
        return query

    @staticmethod
    def get_store_deactivation_query(store_fk):
        current_date = datetime.now().date() - timedelta(3)
        query = """update {} set end_date = '{}', is_current = NULL where store_fk = {} and end_date is null;"""\
            .format(STORE_ASSORTMENT_TABLE, current_date, store_fk)
        return query

    @staticmethod
    def get_activation_query(store_fk, product_fk, date):
        attributes = pd.DataFrame([(store_fk, product_fk, str(date), 1)],
                                  columns=['store_fk', 'product_fk', 'start_date', 'is_current'])
        query = insert(attributes.to_dict(), STORE_ASSORTMENT_TABLE)
        return query

    def connection_ritual(self):
        """
        This function connects to the DB and cursor
        :return: rds connection and cursor connection
        """
        self.rds_conn.disconnect_rds()
        rds_conn = ProjectConnector('batru', DbUsers.CalculationEng)
        cur = rds_conn.db.cursor()
        return rds_conn, cur

    def commit_results(self, queries):
        """
        This function commits the results into the DB in batches.
        query_num is the number of queires that were executed in the current batch
        After batch_size is reached, the function re-connects the DB and cursor.
        """
        rds_conn, cur = self.connection_ritual()
        batch_size = 1000
        query_num = 0
        for query in self.update_queries:
            try:
                cur.execute(query)
                print query
            except Exception as e:
                Log.info('Inserting to DB failed due to: {}'.format(e))
                rds_conn, cur = self.connection_ritual()
                continue
            if query_num > batch_size:
                query_num = 0
                rds_conn, cur = self.connection_ritual()
                rds_conn.db.commit()
            query_num += 1
        rds_conn.db.commit()
        rds_conn, cur = self.connection_ritual()
        query_num = 0
        for query in queries:
            try:
                cur.execute(query)
                print query
            except Exception as e:
                Log.info('Inserting to DB failed due to: {}'.format(e))
                rds_conn, cur = self.connection_ritual()
                continue
            if query_num > batch_size:
                query_num = 0
                rds_conn, cur = self.connection_ritual()
                rds_conn.db.commit()
            query_num += 1
        rds_conn.db.commit()


if __name__ == '__main__':
    LoggerInitializer.init('Upload assortment for Batru')
    BatruAssortment().upload_assortment()
    # # # To run it locally just copy: -e prod -p batru --file **your file path** to the configuration
