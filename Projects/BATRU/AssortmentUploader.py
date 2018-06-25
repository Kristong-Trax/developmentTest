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


def _parse_arguments():
    parser = argparse.ArgumentParser(description='Upload assortment for Batru')
    parser.add_argument('--env', '-e', type=str, help='The environment - dev/int/prod')
    parser.add_argument('--project', '-p', type=str, required=True, help='The name of the project')
    parser.add_argument('--file', '-f', type=str, required=True, help='The assortment template')
    parser.add_argument('--validator', '-v', type=int, help='Use the validator: 1=true, 0=false')
    parser.set_defaults(validator=1)
    return parser.parse_args()


class BatruAssortment:

    def __init__(self, project_name, file_path, to_validate):
        self.project = project_name
        self.file_path = file_path
        self.use_validator = to_validate
        self.store_data = self.get_store_data
        self.all_products = self.get_product_data
        self.current_top_skus = self.get_current_top_skus
        self.rds_conn = self.rds_connect
        self.stores = {}
        self.products = {}
        self.all_queries = []
        self.update_queries = []

    def upload_assortment(self):
        if self.use_validator:
            if self.p1_assortment_validator():
                print "Please fix the template and try again"
                return

    @property
    def rds_connect(self):
        if not hasattr(self, '_rds_conn'):
            self.rds_conn = ProjectConnector(self.project, DbUsers.CalculationEng)
        try:
            pd.read_sql_query('select pk from probedata.session limit 1', self.rds_conn.db)
        except Exception as e:
            self.rds_conn.disconnect_rds()
            self.rds_conn = ProjectConnector(self.project, DbUsers.CalculationEng)
        return self.rds_conn

    @property
    def get_store_data(self):
        if not hasattr(self, '_store_data'):
            query = "select pk as store_fk, store_number_1 as store_number from static.stores"
            self.store_data = pd.read_sql_query(query, self.rds_conn.db)
        return self.store_data

    @property
    def get_product_data(self):
        if not hasattr(self, '_product_data'):
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
        valid_stores = self.store_data.loc[self.store_data['store_number'].isin(raw_data[OUTLET_ID])]
        if len(valid_stores) != len(raw_data[OUTLET_ID].unique()):
            print "Those stores don't exist in the DB: {}".format(list(set(raw_data[OUTLET_ID].unique()) -
                                                                       set(valid_stores['store_number'])))
            legal_template = False

        valid_product = self.all_products.loc[self.all_products[EAN_CODE].isin(raw_data[EAN_CODE])]
        if len(valid_product) != len(raw_data[EAN_CODE].unique()):
            print "Those products don't exist in the DB: {}".format(list(set(raw_data[EAN_CODE].unique()) -
                                                                         set(valid_product[EAN_CODE])))
            legal_template = False
        return legal_template

    def parse_assortment_template(self):
        data = pd.read_csv(self.file_path, sep='\t')
        data = data.drop_duplicates(subset=data.columns, keep='first')
        data = data.fillna('')
        return data

    def upload_store_assortment_file(self):
        # raw_data = pd.read_excel(file_path)
        raw_data = self.parse_assortment_template()
        data = []
        for store in raw_data[OUTLET_ID].unique().tolist():
            store_data = {}
            store_products = raw_data.loc[raw_data[OUTLET_ID] == store][EAN_CODE].tolist()
            store_data[store] = store_products
            data.append(store_data)
        for store_data in data:
            self.update_db_from_json(store_data, immediate_change=True)
        queries = self.merge_insert_queries(self.all_queries)
        self.commit_results(queries)
        return data

    def merge_insert_queries(self, insert_queries):
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
                   where store_fk = {} and product_fk in {} and end_date is null""".format(STORE_ASSORTMENT_TABLE, date,
                                                                                           store_fk, product_fk)
        return query

    @staticmethod
    def get_activation_query(store_fk, product_fk, date):
        attributes = pd.DataFrame([(store_fk, product_fk, str(date), 1)],
                                  columns=['store_fk', 'product_fk', 'start_date', 'is_current'])
        query = insert(attributes.to_dict(), STORE_ASSORTMENT_TABLE)
        return query

    def commit_results(self, queries):
        self.rds_conn.disconnect_rds()
        rds_conn = ProjectConnector('batru', DbUsers.CalculationEng)
        cur = rds_conn.db.cursor()
        for query in self.update_queries:
            try:
                cur.execute(query)
                print query
            except Exception as e:
                Log.info('Inserting to DB failed due to: {}'.format(e))
                rds_conn.disconnect_rds()
                rds_conn = ProjectConnector('batru', DbUsers.CalculationEng)
                cur = rds_conn.db.cursor()
                continue
        rds_conn.db.commit()
        rds_conn.disconnect_rds()
        rds_conn = ProjectConnector('batru', DbUsers.CalculationEng)
        cur = rds_conn.db.cursor()
        for query in queries:
            try:
                cur.execute(query)
                print query
            except Exception as e:
                Log.info('Inserting to DB failed due to: {}'.format(e))
                rds_conn.disconnect_rds()
                rds_conn = ProjectConnector('batru', DbUsers.CalculationEng)
                cur = rds_conn.db.cursor()
                continue
        rds_conn.db.commit()


if __name__ == '__main__':
    LoggerInitializer.init('Upload assortment for Batru')
    # # # Local version # # #
    # project = 'batru'
    # assortment_file_path = '/home/idanr/Desktop/StoreAssortment.csv'
    # use_template_validator = 1
    # BatruAssortment(project, assortment_file_path, use_template_validator).upload_assortment()

    # # # Server's version # # #
    parsed_args = _parse_arguments()
    BatruAssortment(parsed_args.project, parsed_args.file, parsed_args.validator).upload_assortment()
