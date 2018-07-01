import argparse
from datetime import timedelta
import pandas as pd
from Trax.Cloud.Services.Connector.Keys import DbUsers
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Trax.Data.Projects.Connector import ProjectConnector
from Trax.Data.Utils.MySQLservices import get_table_insertion_query as insert
from Trax.Utils.Logging.Logger import Log


PROJECT = 'ccru-sand'
TOP_SKU_TABLE = 'pservice.custom_osa'
CUSTOM_SCIF_TABLE = 'pservice.custom_scene_item_facts'
CORRELATION_FIELD = 'att5'
STORE_NUMBER = 'Store Number'
PRODUCT_EAN_CODE = 'Product EAN'
START_DATE = 'Start Date'
END_DATE = 'End Date'


class CCRU_SANDTopSKUAssortment:

    def __init__(self):
        self.parsed_args = self.parse_arguments()
        self.file_path = self.parsed_args.file
        self.update_correlations = self.parsed_args.update_correlations
        self._rds_conn = self.rds_conn
        self._current_top_skus = self.current_top_skus
        self._store_data = self.store_data
        self._product_data = self.product_data
        self.stores = {}
        self.stores_with_invalid_dates = []
        self.invalid_stores = []
        self.invalid_products = []
        self.products = {}
        self.extension_queries = []
        self.insert_queries = []

    @staticmethod
    def parse_arguments():
        """
        This function gets the arguments from the command line / configuration in case of a local run and manage them.
        :return:
        """
        parser = argparse.ArgumentParser(description='Top SKU CCRU')
        parser.add_argument('--env', '-e', type=str, help='The environment - dev/int/prod')
        parser.add_argument('--file', type=str, required=True, help='The assortment template')
        parser.add_argument('--update_correlations', '-uc', type=int, required=True,
                            help='Should we update correlations?'
                                 'as well - 0 = False, 1 = True')
        return parser.parse_args()

    @property
    def current_top_skus(self):
        if not hasattr(self, '_current_top_skus'):
            self._current_top_skus = self.get_current_top_skus()
        return self._current_top_skus

    @property
    def rds_conn(self):
        if not hasattr(self, '_rds_conn'):
            self._rds_conn = ProjectConnector(PROJECT, DbUsers.CalculationEng)
        try:
            pd.read_sql_query('select pk from probedata.session limit 1', self._rds_conn.db)
        except Exception as e:
            self._rds_conn.disconnect_rds()
            self._rds_conn = ProjectConnector(PROJECT, DbUsers.CalculationEng)
        return self._rds_conn

    @property
    def store_data(self):
        if not hasattr(self, '_store_data'):
            query = "select pk as store_fk, store_number_1 as store_number from static.stores"
            self._store_data = pd.read_sql_query(query, self.rds_conn.db)
        return self._store_data

    @property
    def product_data(self):
        if not hasattr(self, '_product_data'):
            query = "select pk as product_fk, product_ean_code, {} as correlation from static.product " \
                    "where delete_date is null".format(CORRELATION_FIELD)
            self._product_data = pd.read_sql_query(query, self.rds_conn.db)
        return self._product_data

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
            product_fk = self.product_data[self.product_data['product_ean_code'] == product_ean_code]
            if not product_fk.empty:
                product_fk = product_fk['product_fk'].values[0]
                self.products[product_ean_code] = product_fk
            else:
                product_fk = None
        return product_fk

    def get_current_top_skus(self):
        query = """select store_fk, product_fk
                   from pservice.custom_osa
                   where end_date is null"""
        data = pd.read_sql_query(query, self.rds_conn.db)
        return data

    def get_store_top_skus(self, store_fk, curr_end_date_minus_a_day, curr_end_date):
        query = """select store_fk, product_fk, start_date, end_date
                   from pservice.custom_osa where store_fk = {} and end_date between '{}' and '{}';
                   """.format(store_fk, curr_end_date_minus_a_day, curr_end_date)
        data = pd.read_sql_query(query, self.rds_conn.db)
        return data

    def update_db_from_json(self, data):
        products = set()
        store_number = data.pop(STORE_NUMBER, None)
        if store_number is None:
            Log.warning("'{}' is required in data".format(STORE_NUMBER))
            return
        store_fk = self.get_store_fk(store_number)
        start_date = data.pop(START_DATE, None)
        start_date_minus_day = start_date.date() - timedelta(1)
        end_date = data.pop(END_DATE, None)
        for key in data.keys():
            validation = False
            if not data[key]:
                validation = False
            elif isinstance(data[key], (float, int)) and data[key]:
                validation = True
            elif isinstance(data[key], (str, unicode)) and data[key].isdigit() and int(data[key]):
                validation = True
            if validation:
                product_ean_code = str(key).split(',')[-1]
                product_fk = self.get_product_fk(product_ean_code)
                products.add(product_fk)
        if products:
            current_store_top_sku = self.get_store_top_skus(store_fk, start_date_minus_day,
                                                            start_date.date())['product_fk'].tolist()
            products_to_extend = set(products).intersection(current_store_top_sku)
            products_to_activate = set(products).difference(current_store_top_sku)
            for product_fk in products_to_activate:
                self.insert_queries.append(self.get_activation_query(store_fk, product_fk, start_date, end_date))
            for product_fk in products_to_extend:
                self.extension_queries.append(
                    self.get_extension_query(store_fk, product_fk, end_date, start_date_minus_day, start_date))
        else:
            Log.info('{} - No products are configured as Top SKUs'.format(store_number))

    def products_validator(self, raw_data):
        """
        This function check if there's a product in the template that doesn't exist in the DB
        :param raw_data: The store assortment DF
        :return: A fix DF without the invalid columns
        """
        data = raw_data.rename_axis(str.replace(' ', ' ', ''), axis=1)
        products_from_template = data.columns.tolist()
        products_from_template.remove(STORE_NUMBER)
        products_from_template.remove(START_DATE)
        products_from_template.remove(END_DATE)
        for product in products_from_template:
            product = str(product)
            if product.count(','):
                products = product.replace(' ', '').split(',')
                for prod in products:
                    if self._product_data.loc[self._product_data['product_ean_code'] == prod].empty:
                        Log.warning("Product with ean code = {} does not exist in the DB".format(prod))
                        self.invalid_products.append(prod)
                        data = data.drop(product, axis=1)
            else:
                if self._product_data.loc[self._product_data['product_ean_code'] == product].empty:
                    Log.warning("Product with ean code = {} does not exist in the DB".format())
                    self.invalid_products.append(product)
                    try:
                        data = data.drop(int(product), axis=1)
                    except Exception as e:
                        data = data.drop(product, axis=1)
        return data

    def store_row_validator(self, store_row):
        """
        This function validates each template row: It checks if the store exists in the DB and if the dates are exist
        and logic (start date <= end date).
        :return: True in case of a valid row, Else: False.
        """
        store_number_1 = store_row[STORE_NUMBER]
        stores_start_date = store_row[START_DATE]
        stores_end_date = store_row[END_DATE]
        if self._store_data.loc[self._store_data['store_number'] == str(store_number_1)].empty:
            Log.warning('Store number {} does not exist in the DB'.format(store_number_1))
            self.invalid_stores.append(store_number_1)
            return False
        if not stores_start_date or not stores_end_date:
            Log.warning("Missing dates for store number {}".format(store_number_1))
            self.stores_with_invalid_dates.append(store_number_1)
            return False
        if type(stores_start_date) in [str, unicode] or type(stores_end_date) in [str, unicode]:
            Log.warning("The dates for store number {} are in the wrong format".format(store_number_1))
            self.stores_with_invalid_dates.append(store_number_1)
            return False
        if stores_start_date > stores_end_date:
            Log.warning("Invalid dates for store number {}".format(store_number_1))
            self.stores_with_invalid_dates.append(store_number_1)
            return False

        return True

    def parse_and_validate(self):
        """
        This function gets the data from the excel file, validates it and return a valid DataFrame
        :return: A  Dataframe with valid products
        """
        Log.info("Starting to read and validate the template")
        raw_data = pd.read_excel(self.file_path)
        raw_data = raw_data.drop_duplicates(subset=['Store Number', START_DATE, END_DATE], keep='first')
        raw_data = raw_data.fillna('')
        raw_data.columns.str.replace(' ', '')
        raw_data = self.products_validator(raw_data)
        return raw_data

    def upload_top_sku_file(self):
        raw_data = self.parse_and_validate()
        Log.info("Template's validation is done! Starting to prepare the data")
        data = []
        for index_data, store_raw_data in raw_data.iterrows():
            if not self.store_row_validator(store_raw_data):
                continue
            store_data = {}
            columns = list(store_raw_data.keys())
            for column in columns:
                store_data[column] = store_raw_data[column]
            data.append(store_data)
        Log.info("Data's preparation and validation is done")
        if self.update_correlations:
            self.update_correlations_func(data[0].keys())
        Log.info("Starting to prepare the queries")
        for store_data in data:
            self.update_db_from_json(store_data)
        queries = self.merge_insert_queries(self.insert_queries)
        self.commit_results(queries)
        Log.info("Top SKU is done!")
        if self.invalid_products:
            Log.warning("The following products does not exist in the DB: {}".format(self.invalid_products))
        if self.invalid_stores:
            Log.warning("The following stores does not exist in the DB: {}".format(self.invalid_stores))
        if self.stores_with_invalid_dates:
            Log.warning("The following stores had invalid dates: {}".format(self.stores_with_invalid_dates))
        return

    def update_correlations_func(self, products_data):
        """
        2 products in the same columns (that seperated by ',') will counted as correlated products.
        The function tracks them and updates static.product in attr5 of the main product (the first one in the dual)
        :param products_data: all of the products that has a correlated product with them in their column
        """
        Log.info("Starting update correlated products")
        correlations = {}
        for products in products_data:
            products = str(products)
            if products.count(','):
                correlated_products = set()
                products = products.split(',')
                main_product = products.pop(-1).strip()
                for product in products:
                    product_fk = self.get_product_fk(product)
                    if product_fk is not None:
                        correlated_products.add(product_fk)
                if correlated_products:
                    correlations[main_product] = list(correlated_products)
        if correlations:
            queries = [self.get_delete_correlation_query()]
            for product_ean_code in correlations:
                queries.append(self.get_correlation_query(product_ean_code, correlations[product_ean_code]))
            self.commit_results(queries)
            delattr(self, '_product_data')

    @staticmethod
    def get_extension_query(store_fk, product_fk, new_end_date, curr_end_date_minus_day, curr_end_date):
        query = """update {} set end_date = '{}'
                   where store_fk = {} and product_fk in {} and end_date 
                   between '{}' and '{}';""".format(TOP_SKU_TABLE, new_end_date, store_fk, product_fk,
                                                    curr_end_date_minus_day, curr_end_date)
        return query

    @staticmethod
    def get_activation_query(store_fk, product_fk, start_date, end_date):
        attributes = pd.DataFrame([(store_fk, product_fk, str(start_date), str(end_date), 1)],
                                  columns=['store_fk', 'product_fk', 'start_date', 'end_date', 'is_current'])
        query = insert(attributes.to_dict(), TOP_SKU_TABLE)
        return query

    @staticmethod
    def get_delete_correlation_query():
        query = 'update static.product set {0} = null where {0} is not null'.format(CORRELATION_FIELD)
        return query

    @staticmethod
    def get_correlation_query(anchor_ean_code, correlated_products):
        if len(correlated_products) == 1:
            condition = 'pk = {}'.format(correlated_products[0])
        else:
            condition = 'pk in ({})'.format(tuple(correlated_products))
        query = "update static.product set {} = '{}' where {}".format(CORRELATION_FIELD, anchor_ean_code, condition)
        return query

    def connection_ritual(self):
        """
        This function connects to the DB and cursor
        :return: rds connection and cursor connection
        """
        self.rds_conn.disconnect_rds()
        rds_conn = ProjectConnector(PROJECT, DbUsers.CalculationEng)
        cur = rds_conn.db.cursor()
        return rds_conn, cur

    def commit_results(self, merged_insert_queries):
        Log.info("Starting to commit the queries")
        rds_conn, cur = self.connection_ritual()
        batch_size = 1000
        query_num = 0
        for query in self.extension_queries:
            print query
            try:
                cur.execute(query)
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
        for query in merged_insert_queries:
            print query
            try:
                cur.execute(query)
            except Exception as e:
                Log.info('Inserting to DB failed due to: {}'.format(e))
                rds_conn, cur = self.connection_ritual()
                continue
        rds_conn.db.commit()

    def get_top_skus_for_store(self, store_fk, visit_date):
        query = """
                select ts.product_fk, p.product_ean_code
                from {} ts
                join static.product p on p.pk = ts.product_fk
                where ts.store_fk = {} and '{}' between ts.start_date and ifnull(ts.end_date, curdate())
                """.format(TOP_SKU_TABLE, store_fk, visit_date)
        data = pd.read_sql_query(query, self.rds_conn.db)
        return data.groupby('product_fk')['product_ean_code'].first().to_dict()

    def get_correlated_products(self, product_ean_code):
        return self.product_data[self.product_data['correlation'] == product_ean_code]['product_fk'].tolist()

    @staticmethod
    def get_custom_scif_query(session_fk, scene_fk, product_fk, in_assortment, distributed):
        in_assortment = 1 if in_assortment else 0
        out_of_stock = 1 if not distributed else 0
        attributes = pd.DataFrame([(session_fk, scene_fk, product_fk, in_assortment, out_of_stock)],
                                  columns=['session_fk', 'scene_fk', 'product_fk', 'in_assortment_osa', 'oos_osa'])
        query = insert(attributes.to_dict(), CUSTOM_SCIF_TABLE)
        return query

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
            for group_index in xrange(0, len(query_groups[group]), 10**4):
                merged_queries.append('{0} VALUES {1}'.format(group, ',\n'.join(query_groups[group]
                                                                                [group_index:group_index+10**4])))
        return merged_queries

if __name__ == '__main__':
    LoggerInitializer.init('Top SKU CCRU-SAND')
    ts = CCRU_SANDTopSKUAssortment()
    ts.upload_top_sku_file()
# # # To run it locally just copy: -e prod --file **your file path** -uc 1 or 0 to the configuration
# # # At the end of the script there are logs with all of the invalid products, store numbers and dates
