import pandas as pd
from Trax.Data.Projects.Connector import ProjectConnector
from Trax.Cloud.Services.Connector.Keys import DbUsers


STORE_ASSORTMENT_TABLE = 'pservice.custom_osa'
OUTLET_ID = 'Outlet ID'
EAN_CODE = 'product_ean_code'

class BatruAssortment:

    def __init__(self, project_name, file_path):
        self.project = project_name
        self.file_path = file_path
        self.store_data = 555
        self.all_products = 555
        self._rds_conn

    @property
    def rds_conn(self):
        if not hasattr(self, '_rds_conn'):
            self._rds_conn = ProjectConnector(self.project, DbUsers.CalculationEng)
        try:
            pd.read_sql_query('select pk from probedata.session limit 1', self._rds_conn.db)
        except Exception as e:
            self._rds_conn.disconnect_rds()
            self._rds_conn = ProjectConnector(self.project, DbUsers.CalculationEng)
        return self._rds_conn

    def p1_assortment_validator(self, file_path):
        """
        This function validates the store assortment template.
        It compares the OUTLET_ID (= store_number_1) and the products ean_code to the stores and products from the DB
        :param file_path: Store assortment template
        :return: False in case of an error and True in case of a valid template
        """
        raw_data = pd.read_csv(file_path, sep='\t')
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
    project = 'batru'
    assortment_file_path = '/home/idanr/Desktop/StoreAssortment.csv'
    BatruAssortment(project, assortment_file_path)