import argparse
import pandas as pd
import datetime as dt

from Trax.Cloud.Services.Connector.Keys import DbUsers
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Trax.Data.Utils.MySQLservices import get_table_insertion_query as insert
from Trax.Utils.Logging.Logger import Log
from KPIUtils_v2.DB.PsProjectConnector import PSProjectConnector


PROJECT = 'ccru_sand'
TOP_SKU_TABLE = 'pservice.custom_osa'
CUSTOM_SCIF_TABLE = 'pservice.custom_scene_item_facts'
CORRELATION_FIELD = 'substitution_product_fk'
TARGETS_SHEET_NAME = 'targets'


class CCRU_SANDTopSKUAssortment:
    STORE_NUMBER = 'Store Number'
    PRODUCT_EAN_CODE = 'Product EAN'
    START_DATE = 'Start Date'
    END_DATE = 'End Date'

    def __init__(self, rds_conn=None):
        if rds_conn is not None:
            self._rds_conn = rds_conn
        self.stores = {}
        self.stores_processed = []
        self.stores_with_invalid_dates = []
        self.invalid_stores = []
        self.invalid_products = []
        self.duplicate_columns = []
        self.products = {}
        self.deactivation_queries = []
        self.deactivation_queries_count = 0
        self.extension_queries = []
        self.extension_queries_count = 0
        self.insert_queries = []
        self.insert_queries_count = 0
        self.merged_insert_queries = []

    @staticmethod
    def parse_arguments():
        """
        This function gets the arguments from the command line / configuration in case of a local run and manage them.
        :return:
        """
        parser = argparse.ArgumentParser(description='Top SKU CCRU')
        parser.add_argument('--env', '-e', type=str, help='The environment - dev/int/prod')
        parser.add_argument('--file', type=str, required=True, help='The assortment template')
        return parser.parse_args()

    @property
    def rds_conn(self):
        if not hasattr(self, '_rds_conn'):
            self._rds_conn = PSProjectConnector(PROJECT, DbUsers.CalculationEng)
        try:
            pd.read_sql_query('select pk from probedata.session limit 1', self._rds_conn.db)
        except Exception as e:
            if self._rds_conn.is_connected:
                self._rds_conn.disconnect_rds()
            self._rds_conn = PSProjectConnector(PROJECT, DbUsers.CalculationEng)
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
            query = "select pk as product_fk, ean_code as product_ean_code, {} as correlation from static_new.product " \
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
            product_fk = self.product_data[self.product_data['product_ean_code']
                                           == product_ean_code]
            if not product_fk.empty:
                product_fk = product_fk['product_fk'].values[0]
                self.products[product_ean_code] = product_fk
            else:
                product_fk = None
        return product_fk

    def get_store_top_skus(self, store_fk, curr_start_date, curr_end_date):
        query = """
                select concat(coalesce(product_fk,''), '|', 
                              coalesce(anchor_product_fk,''), '|', 
                              coalesce(min_facings,''), '|', 
                              coalesce(no_of_rows,'')) as top_sku_key
                from(
                select store_fk, product_fk, anchor_product_fk, min_facings, count(*) as no_of_rows
                from {} where store_fk = {} and start_date <= '{}' and (end_date >= '{}' or end_date is null)
                group by product_fk, anchor_product_fk, min_facings
                ) t;
                """\
            .format(TOP_SKU_TABLE, store_fk, curr_end_date, curr_start_date)
        data = pd.read_sql_query(query, self.rds_conn.db)
        return data

    def prepare_db_update_from_template(self, data):
        tmplt_top_sku_keys = set()
        store_number = data.pop(self.STORE_NUMBER, None)
        store_fk = self.get_store_fk(store_number)
        if store_fk is None:
            Log.warning("Store number '{}' is not defined in DB".format(self.STORE_NUMBER))
            return
        start_date = data.pop(self.START_DATE, None).date()
        start_date_minus_day = start_date - dt.timedelta(1)
        end_date = data.pop(self.END_DATE, None).date()
        for key in data.keys():
            validation = False
            if not data[key]:
                validation = False
            elif isinstance(data[key], (float, int)) and data[key]:
                validation = True
            elif isinstance(data[key], (str, unicode)) and data[key].isdigit() and int(data[key]):
                validation = True
            if validation:
                product_list = str(key).split(',')
                anchor_product_ean_code = product_list[0]
                anchor_product_fk = self.get_product_fk(anchor_product_ean_code)
                if anchor_product_fk is None:
                    # Log.warning("Anchor product EAN '{}' is not defined in DB".format(anchor_product_ean_code))
                    continue
                min_facings = data[key]
                for product in product_list:
                    product_fk = self.get_product_fk(product)
                    if product_fk is None:
                        # Log.warning("Product EAN '{}' is not defined in DB".format(product))
                        continue
                    tmplt_top_sku_keys.add(str(product_fk) + '|' +
                                           str(anchor_product_fk) + '|' +
                                           str(min_facings) + '|1')
        # if not tmplt_top_sku_keys:
        #     Log.debug('No products are configured as Top SKUs for store number {} and period {} - {}'
        #              ''.format(store_number, start_date, end_date))
        store_top_sku_keys = self.get_store_top_skus(store_fk, start_date, end_date)[
            'top_sku_key'].tolist()
        products_to_deactivate = set(store_top_sku_keys).difference(tmplt_top_sku_keys)
        products_to_extend = set(tmplt_top_sku_keys).intersection(store_top_sku_keys)
        products_to_activate = set(tmplt_top_sku_keys).difference(store_top_sku_keys)
        for product in products_to_deactivate:
            product_fk = product.split('|')[0]
            anchor_product_fk = product.split('|')[1]
            min_facings = product.split('|')[2]
            self.deactivation_queries.append(self.get_deactivation_query(store_fk, product_fk, anchor_product_fk, min_facings,
                                                                         start_date_minus_day, start_date, end_date))
        for product in products_to_extend:
            product_fk = product.split('|')[0]
            anchor_product_fk = product.split('|')[1]
            min_facings = product.split('|')[2]
            self.extension_queries.append(self.get_extension_query(store_fk, product_fk, anchor_product_fk, min_facings,
                                                                   start_date, end_date))
        for product in products_to_activate:
            product_fk = product.split('|')[0]
            anchor_product_fk = product.split('|')[1]
            min_facings = product.split('|')[2]
            self.insert_queries.append(self.get_activation_query(store_fk, product_fk, anchor_product_fk, min_facings,
                                                                 start_date, end_date))

        self.stores_processed += [store_number]

        return

    def products_validator(self, raw_data):
        """
        This function checks if there's a product in the template that doesn't exist in the DB
        :param raw_data: Store assortment DF
        :return: Fixed store assortment DF without the invalid columns
        """
        for col in raw_data.columns:
            if str(col).count('.'):
                # Log.warning("Duplicate column {} is encountered in the template and removed from loading"
                #             "".format(col.split('.')[0]))
                self.duplicate_columns.append(col)
        data = raw_data.drop(self.duplicate_columns, axis=1)
        data = data.rename_axis(str.replace(' ', ' ', ''), axis=1)
        products_from_template = data.columns.tolist()
        products_from_template.remove(self.STORE_NUMBER)
        products_from_template.remove(self.START_DATE)
        products_from_template.remove(self.END_DATE)
        for product in products_from_template:
            product = str(product)
            products = product.replace(' ', '').replace('\n', '').split(',')
            for prod in products:
                if self.product_data.loc[self.product_data['product_ean_code'] == prod].empty:
                    # Log.warning("Product with EAN Code = {} does not exist in the DB".format(prod))
                    self.invalid_products.append(prod)
        return data

    def store_row_validator(self, store_row):
        """
        This function validates each template row:
        It checks if the store exists in the DB and if the dates are valid (start date <= end date).
        :return: If the row is valid: True, Otherwise: False.
        """
        store_data = self.store_data
        store_number_1 = store_row[self.STORE_NUMBER]
        stores_start_date = store_row[self.START_DATE]
        stores_end_date = store_row[self.END_DATE]
        if store_data.loc[store_data['store_number'] == str(store_number_1)].empty:
            # Log.warning('Store number {} does not exist in the DB'.format(store_number_1))
            self.invalid_stores.append(store_number_1)
            return False
        if not stores_start_date or not stores_end_date:
            # Log.warning("Missing dates for store number {}".format(store_number_1))
            self.stores_with_invalid_dates.append(store_number_1)
            return False
        if type(stores_start_date) in [str, unicode] or type(stores_end_date) in [str, unicode]:
            # Log.warning("The dates for store number {} are in the wrong format".format(store_number_1))
            self.stores_with_invalid_dates.append(store_number_1)
            return False
        if stores_start_date > stores_end_date:
            # Log.warning("Invalid dates for store number {}".format(store_number_1))
            self.stores_with_invalid_dates.append(store_number_1)
            return False

        return True

    def parse_and_validate(self, file_path):
        """
        This function gets the data from the excel file, validates it and returns a valid DataFrame
        :return: A DataFrame with valid products
        """
        raw_data = pd.read_excel(file_path, sheetname=TARGETS_SHEET_NAME)
        raw_data = raw_data.drop_duplicates(
            subset=[self.STORE_NUMBER, self.START_DATE, self.END_DATE], keep='first')
        raw_data = raw_data.fillna('')
        raw_data.columns.str.replace(' ', '').str.replace('\n', '')
        raw_data = self.products_validator(raw_data)
        return raw_data

    def upload_top_sku_file(self):
        parsed_args = self.parse_arguments()
        file_path = parsed_args.file

        Log.debug("Starting template parsing and EAN Codes validation")
        raw_data = self.parse_and_validate(file_path)
        if self.duplicate_columns:
            Log.warning("The following columns are duplicate in the template and will be ignored ({}): "
                        "{}".format(len(self.duplicate_columns), self.duplicate_columns))
        if self.invalid_products:
            Log.warning("The following products do not exist in the DB and will be ignored ({}): "
                        "{}".format(len(self.invalid_products), self.invalid_products))

        Log.debug("Starting Stores validation")
        data = []
        for index_data, store_raw_data in raw_data.iterrows():
            if (index_data + 1) % 1000 == 0 or (index_data + 1) == raw_data.shape[0]:
                Log.debug(
                    "Number of stores validated: {}/{}".format(index_data + 1, raw_data.shape[0]))
            if not self.store_row_validator(store_raw_data):
                continue
            store_data = {}
            columns = list(store_raw_data.keys())
            for column in columns:
                store_data[column] = store_raw_data[column]
            data.append(store_data)
        if self.invalid_stores:
            Log.warning("The following stores do not exist in the DB and will be ignored ({}): "
                        "{}".format(len(self.invalid_stores), self.invalid_stores))
        if self.stores_with_invalid_dates:
            Log.warning("The following stores have invalid date period and will be ignored ({}): "
                        "{}".format(len(self.stores_with_invalid_dates), self.stores_with_invalid_dates))

        Log.debug("Starting data processing")
        count_stores_total = len(data)
        count_stores_processed = 0
        for store_data in data:

            self.prepare_db_update_from_template(store_data)
            count_stores_processed += 1

            if count_stores_processed % 1 == 0 or count_stores_processed == count_stores_total:

                queries = []

                queries += self.deactivation_queries
                self.deactivation_queries_count += len(self.deactivation_queries)
                self.deactivation_queries = []

                queries += self.extension_queries
                self.extension_queries_count += len(self.extension_queries)
                self.extension_queries = []

                queries += self.merge_insert_queries(self.insert_queries)
                self.insert_queries_count += len(self.insert_queries)
                self.insert_queries = []

                self.commit_results(queries)
                queries = []

            if count_stores_processed % 1000 == 0 or count_stores_processed == count_stores_total:
                Log.debug(
                    "Number of stores processed and committed to DB: {}/{}".format(count_stores_processed, count_stores_total))
                # Log.debug("Stores processed: {}".format(self.stores_processed))
                self.stores_processed = []

        if self.duplicate_columns:
            Log.warning("The following columns are duplicate in the template and were ignored ({}): "
                        "{}".format(len(self.duplicate_columns), self.duplicate_columns))

        if self.invalid_products:
            Log.warning("The following products do not exist in the DB and were ignored ({}): "
                        "{}".format(len(self.invalid_products), self.invalid_products))

        if self.invalid_stores:
            Log.warning("The following stores do not exist in the DB and were ignored ({}): "
                        "{}".format(len(self.invalid_stores), self.invalid_stores))

        if self.stores_with_invalid_dates:
            Log.warning("The following stores have invalid date period and were ignored ({}): "
                        "{}".format(len(self.stores_with_invalid_dates), self.stores_with_invalid_dates))

        Log.debug("Total Top SKU uploading status for Products in Stores: Deactivated = {}, Extended = {}, New = {}"
                  .format(self.deactivation_queries_count, self.extension_queries_count, self.insert_queries_count))

        Log.debug("Top SKU targets are uploaded successfully. " +
                  ("Incorrect template data were ignored (see above)" if self.duplicate_columns or self.invalid_products or self.invalid_stores or self.stores_with_invalid_dates else ""))

        return

    @staticmethod
    def get_deactivation_query(store_fk, product_fk, anchor_product_fk, min_facings, curr_start_date_minus_day, curr_start_date, curr_end_date):
        if len(anchor_product_fk) > 0:
            anchor_product_fk = '= ' + str(anchor_product_fk)
        else:
            anchor_product_fk = 'is null'

        if len(min_facings) > 0:
            min_facings = '= ' + str(min_facings)
        else:
            min_facings = 'is null'

        query = """
                update {} set end_date = '{}'
                where store_fk = {} and product_fk = {} and anchor_product_fk {} and min_facings {}
                and start_date <= '{}' and (end_date >= '{}' or end_date is null);
                """\
            .format(TOP_SKU_TABLE, curr_start_date_minus_day,
                    store_fk, product_fk, anchor_product_fk, min_facings,
                    curr_end_date, curr_start_date)
        return query

    @staticmethod
    def get_extension_query(store_fk, product_fk, anchor_product_fk, min_facings, curr_start_date, curr_end_date):
        if len(anchor_product_fk) > 0:
            anchor_product_fk = '= ' + str(anchor_product_fk)
        else:
            anchor_product_fk = 'is null'

        if len(min_facings) > 0:
            min_facings = '= ' + str(min_facings)
        else:
            min_facings = 'is null'

        query = """
                update {} set start_date = if(start_date <= '{}', start_date, '{}'), end_date = '{}'
                where store_fk = {} and product_fk = {} and anchor_product_fk {} and min_facings {}
                and start_date <= '{}' and (end_date >= '{}' or end_date is null);
                """\
            .format(TOP_SKU_TABLE, curr_start_date, curr_start_date, curr_end_date,
                    store_fk, product_fk, anchor_product_fk, min_facings,
                    curr_end_date, curr_start_date)
        return query

    @staticmethod
    def get_activation_query(store_fk, product_fk, anchor_product_fk, min_facings, start_date, end_date):
        attributes = pd.DataFrame([(store_fk, product_fk, str(start_date), str(end_date), None, anchor_product_fk, min_facings)],
                                  columns=['store_fk', 'product_fk', 'start_date', 'end_date', 'is_current', 'anchor_product_fk', 'min_facings'])
        query = insert(attributes.to_dict(), TOP_SKU_TABLE)
        return query

    def connection_ritual(self):
        """
        This function connects to the DB and cursor
        :return: rds connection and cursor connection
        """
        if self.rds_conn.is_connected:
            self.rds_conn.disconnect_rds()
        rds_conn = PSProjectConnector(PROJECT, DbUsers.CalculationEng)
        cur = rds_conn.db.cursor()
        return rds_conn, cur

    def commit_results(self, queries):
        batch_size = 1000

        rds_conn, cur = self.connection_ritual()
        query_num = 0
        for query in queries:
            # print query
            try:
                cur.execute(query)
            except Exception as e:
                Log.debug('DB update failed due to: {}'.format(e))
                rds_conn, cur = self.connection_ritual()
                cur.execute(query)
                continue
            if query_num > batch_size:
                query_num = 0
                rds_conn, cur = self.connection_ritual()
                rds_conn.db.commit()
            query_num += 1
        rds_conn.db.commit()

        return

    def get_top_skus_for_store(self, store_fk, visit_date):
        query = """
                select
                anchor_product_fk,
                group_concat(product_fk) as product_fks,
                max(min_facings) as min_facings
                from (
                    select          
                    ifnull(ts.anchor_product_fk, ts.product_fk) as anchor_product_fk,
                    ts.product_fk as product_fk,
                    ifnull(ts.min_facings, 1) as min_facings
                    from {} ts
                    where ts.store_fk = {}
                    and ts.start_date <= '{}' 
                    and ifnull(ts.end_date, curdate()) >= '{}'
                ) t
                group by anchor_product_fk;
                """.format(TOP_SKU_TABLE,
                           store_fk,
                           visit_date,
                           visit_date)
        data = pd.read_sql_query(query, self.rds_conn.db)
        return data.groupby(['anchor_product_fk']).agg({'product_fks': 'first', 'min_facings': 'first'}).to_dict()

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
    LoggerInitializer.init('Top SKU CCRU')
    ts = CCRU_SANDTopSKUAssortment()
    ts.upload_top_sku_file()
# # # To run it locally just copy: -e prod --file **your file path** to the configuration
# # # At the end of the script there are logs with all of the invalid products, store numbers and dates
