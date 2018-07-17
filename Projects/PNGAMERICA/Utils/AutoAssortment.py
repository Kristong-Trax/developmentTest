
import os
import pandas as pd
from datetime import datetime, timedelta
from openpyxl.utils import column_index_from_string, coordinate_from_string

from Trax.Cloud.Services.Connector.Keys import DbUsers
from Trax.Utils.Logging.Logger import Log
# from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Trax.Data.Projects.Connector import ProjectConnector
from Trax.Data.Utils.MySQLservices import get_table_insertion_query as insert

PROJECT = 'pngamerica'
TOP_SKU_TABLE = 'pservice.custom_osa'
CUSTOM_SCIF_TABLE = 'pservice.custom_scene_item_facts'
CORRELATION_FIELD = 'att5'


class AutoAssortmentHandler:

    STORE_NUMBER = 'Store Number'
    PRODUCT_EAN_CODE = 'Product EAN'

    def __init__(self, rds_conn=None):
        if rds_conn is not None:
            self._rds_conn = rds_conn
        self.stores = {}
        self.products = {}
        self.all_queries = []
        self.update_queries = []

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
        except:
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
            query = "select pk as product_fk, ean_code from static_new.product " \
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
            if product_ean_code.isdigit():
                ean_to_check = int(product_ean_code)
            else:
                ean_to_check = product_ean_code
            product_fk = self.product_data[self.product_data['ean_code'] == ean_to_check]
            if not product_fk.empty:
                product_fk = product_fk['product_fk'].values[0]
                self.products[product_ean_code] = product_fk
            else:
                product_fk = None
        return product_fk

    def get_product_ean_code(self, product_fk):
        product_fk = str(product_fk).strip()
        if product_fk in self.products:
            product_ean_code = self.products[product_fk]
        else:
            if product_fk.isdigit():
                ean_to_check = int(product_fk)
            else:
                ean_to_check = product_fk
            product_fk = self.product_data[self.product_data['product_fk'] == ean_to_check]
            if not product_fk.empty:
                product_ean_code = product_fk['ean_code'].values[0]
                self.products[product_fk] = product_ean_code
            else:
                product_ean_code = None
        return product_ean_code

    def get_current_top_skus(self):
        query = """select store_fk, product_fk
                   from pservice.custom_osa
                   where end_date is null"""
        data = pd.read_sql_query(query, self.rds_conn.db)
        return data

    def get_current_assortment_per_store(self, store_fk, visit_date):
        query = """select product_fk
                   from pservice.custom_osa
                   where end_date is null and store_fk = {} and start_date <= '{}'""".format(store_fk, visit_date)
        data = pd.read_sql_query(query, self.rds_conn.db)
        return data['product_fk'].unique().tolist()

    def update_db_from_json(self, data, immediate_change=False, discard_missing_products=False):
        products = set()
        missing_products = set()
        store_number = data.pop(self.STORE_NUMBER, None)
        if store_number is None:
            Log.warning("'{}' is required in data".format(self.STORE_NUMBER))
            return
        # store_fk = self.get_store_fk(store_number)
        store_fk = store_number
        if store_fk is None:
            Log.warning('Store {} does not exist. Exiting...'.format(store_number))
            return
        for key in data.keys():
            validation = False
            # if not data[key]:
            #     validation = False
            # elif isinstance(data[key], (float, int)) and data[key]:
            #     validation = True
            # elif isinstance(data[key], (str, unicode)) and data[key].isdigit() and int(data[key]):
            #     validation = True
            if data[key]:
                if not key == 'date':
                    validation = True
            if validation:
                # product_ean_code = str(key).split(',')[-1]
                product_fk = str(key).split(',')[-1]
                # product_fk = self.get_product_fk(product_ean_code)
                # product_ean_code = self.get_product_ean_code(product_fk)
                if product_fk is None:
                    Log.warning('Product PK {} does not exist'.format(product_fk))
                    missing_products.add(product_fk)
                    continue
                try:
                    products.add(int(product_fk))
                except Exception as e:
                    continue
        if missing_products and not discard_missing_products:
            Log.warning('Some EANs do not exist: {}. Exiting...'.format('; '.join(missing_products)))
            return

        if products:
            current_date = data['date']
            if immediate_change:
                deactivate_date = current_date
                activate_date = current_date
            else:
                deactivate_date = current_date
                activate_date = current_date

            queries = []
            current_skus = self.current_top_skus[self.current_top_skus['store_fk'] == store_fk]['product_fk'].tolist()
            products_to_deactivate = []
            # products_to_deactivate = set(current_skus).difference(products)
            products_to_activate = set(products).difference(current_skus)
            # for product_fk in products_to_deactivate:
            #     if product_fk.isdigit():
            #         queries.append(self.get_deactivation_query(store_fk, product_fk, deactivate_date))
            for product_fk in products_to_activate:
                try:
                    if activate_date in self.stores_min_dates[product_fk, store_fk]:
                        queries.append(self.get_activation_query(store_fk, product_fk, activate_date))
                except Exception as e:
                    queries.append(self.get_activation_query(store_fk, product_fk, activate_date))
            # self.commit_results(queries)
            self.all_queries.extend(queries)
            Log.debug('{} - Out of {} products, {} products were deactivated and {} products were activated'.format(
                store_number, len(products), len(products_to_deactivate), len(products_to_activate)))
        else:
            Log.debug('{} - No products are configured as Top SKUs'.format(store_number))

    def upload_top_sku_file(self, file_path, data_first_cell, ean_row_index, store_number_column_index,
                            update_correlations=False):

        raw_data = pd.read_csv(file_path)
        raw_data = raw_data.fillna('')
        data = []
        self.stores_min_dates = {}
        for index_data, store_raw_data in raw_data.iterrows():
            # store_data = {self.STORE_NUMBER: index_data[store_number_column_index]}
            store_data = {self.STORE_NUMBER: store_raw_data['store_fk'], 'date': store_raw_data['date']}
            columns = list(store_raw_data.keys())
            columns.remove('store_fk')
            columns.remove('woy')
            columns.remove('date')
            try:
                columns.remove('stores_name')
            except Exception as e:
                pass

            for column in columns:
                store_data[column] = store_raw_data[column]
                if store_raw_data[column]:
                    if (column, store_raw_data['store_fk']) in self.stores_min_dates.keys():
                        if datetime.strptime(self.stores_min_dates[column, store_raw_data['store_fk']], "%Y-%m-%d") > datetime.strptime(store_raw_data['date'], "%Y-%m-%d"):
                            self.stores_min_dates[column, store_raw_data['store_fk']] = store_raw_data['date']
                    else:
                        self.stores_min_dates[column, store_raw_data['store_fk']] = store_raw_data['date']
            data.append(store_data)

        if update_correlations:
            self.update_correlations(data[0].keys())
        for store_data in data:
            self.update_db_from_json(store_data, immediate_change=True)

        queries = self.merge_insert_queries(self.all_queries)
        self.commit_results(queries)
        return data

    def update_correlations(self, products_data):
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
    def get_deactivation_query(store_fk, product_fk, date):
        query = """update {} set end_date = '{}', is_current = NULL
                   where store_fk = {} and product_fk = {} and end_date is null""".format(TOP_SKU_TABLE, date,
                                                                                          store_fk, product_fk)
        return query

    def get_activation_query(self, store_fk, product_fk, date):
        # if date in self.stores_min_dates[store_fk]:
        attributes = pd.DataFrame([(store_fk, product_fk, str(date), None)],
                                  columns=['store_fk', 'product_fk', 'start_date', 'is_current'])
        # else:
        #     end_date = datetime.strptime(date, "%Y-%m-%d") + timedelta(7)
        #     attributes = pd.DataFrame([(store_fk, product_fk, str(date), None, str(end_date))],
        #                               columns=['store_fk', 'product_fk', 'start_date', 'is_current', 'end_date'])
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

    def commit_results(self, queries):
        self.rds_conn.disconnect_rds()
        rds_conn = ProjectConnector(PROJECT, DbUsers.CalculationEng)
        cur = rds_conn.db.cursor()
        for query in self.update_queries:
            print query
            try:
                cur.execute(query)
            except Exception as e:
                Log.debug('Inserting to DB failed due to: {}'.format(e))
                rds_conn.disconnect_rds()
                rds_conn = ProjectConnector(PROJECT, DbUsers.CalculationEng)
                cur = rds_conn.db.cursor()
                continue
        rds_conn.db.commit()
        rds_conn.disconnect_rds()
        rds_conn = ProjectConnector(PROJECT, DbUsers.CalculationEng)
        cur = rds_conn.db.cursor()
        for query in queries:
            print query
            try:
                cur.execute(query)
            except Exception as e:
                Log.debug('Inserting to DB failed due to: {}'.format(e))
                rds_conn.disconnect_rds()
                rds_conn = ProjectConnector(PROJECT, DbUsers.CalculationEng)
                cur = rds_conn.db.cursor()
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

    def merge_insert_queries(self, insert_queries):
        # other_queries = []
        query_groups = {}
        for query in insert_queries:
            if 'update' in query:
                self.update_queries.append(query)
            else:
                static_data, inserted_data = query.split('VALUES ')
                if static_data not in query_groups:
                    query_groups[static_data] = []
                query_groups[static_data].append(inserted_data)
        merged_queries = []
        for group in query_groups:
            for group_index in xrange(0, len(query_groups[group]), 10**4):
                merged_queries.append('{0} VALUES {1}'.format(group, ',\n'.join(query_groups[group]
                                                                                [group_index:group_index+10**4])))
        # merged_queries.extend(other_queries)
        return merged_queries

# if __name__ == '__main__':
#     LoggerInitializer.init('test')
#     rds_conn = ProjectConnector(PROJECT, DbUsers.CalculationEng)
#     ts = AutoAssortmentHandler(rds_conn=rds_conn)
#     ts.upload_top_sku_file(file_path='/home/uri/Documents/pngamerica/assortments_300318/assortment_2.csv', data_first_cell='D2',
#                            ean_row_index=1, store_number_column_index='B')
