import pandas as pd
import argparse
from Trax.Data.Projects.Connector import ProjectConnector
from Trax.Cloud.Services.Connector.Keys import DbUsers
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer


STORE_ASSORTMENT_TABLE = 'pservice.custom_osa'
OUTLET_ID = 'Outlet ID'
EAN_CODE = 'product_ean_code'


def _parse_arguments():
    parser = argparse.ArgumentParser(description='Upload assortment for Batru')
    parser.add_argument('-e', '--env', type=str, help='The environment - dev/int/prod')
    parser.add_argument('--project', '-p', type=str, required=True, help='The name of the project')
    parser.add_argument('--file', '-f', type=str, required=True, help='The assortment template')
    parser.add_argument('--validator', '-v', type=int, help='Use the validator: 1=true, 0=false')
    parser.set_defaults(validator=1)
    return parser.parse_args()

class BatruAssortment:

    def __init__(self, project_name, file_path):
        self.project = project_name
        self.file_path = file_path
        self.store_data = self.get_store_data
        self.all_products = self.get_product_data
        self.rds_conn = self.rds_connect

    def upload_assortment(self):
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

    def p1_assortment_validator(self):
        """
        This function validates the store assortment template.
        It compares the OUTLET_ID (= store_number_1) and the products ean_code to the stores and products from the DB
        :param file_path: Store assortment template
        :return: False in case of an error and True in case of a valid template
        """
        raw_data = pd.read_csv(self.file_path, sep='\t')
        raw_data = raw_data.drop_duplicates(subset=raw_data.columns, keep='first')
        raw_data = raw_data.fillna('')
        stores = self.store_data
        valid_stores = stores.loc[stores['store_number'].isin(raw_data[OUTLET_ID])]
        if len(valid_stores) != len(raw_data[OUTLET_ID].unique()):
            print "Those stores don't exist in the DB: {}".format(list(set(raw_data[OUTLET_ID].unique()) -
                                                                         set(valid_stores['store_number'])))
            return False

        valid_product = self.all_products.loc[self.all_products[EAN_CODE].isin(raw_data[EAN_CODE])]
        if len(valid_product) != len(raw_data[EAN_CODE].unique()):
            print "Those products don't exist in the DB: {}".format(list(set(raw_data[EAN_CODE].unique()) -
                                                                         set(valid_product[EAN_CODE])))
            return False

        return True


if __name__ == '__main__':
    LoggerInitializer.init('Upload assortment for Batru')
    # # # Local version # # #
    # project = 'batru'
    # assortment_file_path = '/home/idanr/Desktop/StoreAssortment.csv'
    # BatruAssortment(project, assortment_file_path).upload_assortment()

    # # # Server's version # # #
    parsed_args = _parse_arguments()
    BatruAssortment(parsed_args.project, parsed_args.file).upload_assortment()