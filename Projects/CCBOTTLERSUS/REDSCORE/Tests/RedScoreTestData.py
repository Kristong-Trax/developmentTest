import pandas as pd


class RedScoreTestData(object):
    store_data1 = {'additional_attribute_1': {0: None},
                   'additional_attribute_12': {0: None},
                   'additional_attribute_2': {0: None},
                   'address_city': {0: None},
                   'branch_fk': {0: None},
                   'customer_name': {0: None},
                   'distribution_type': {0: None},
                   'manager_name': {0: None},
                   'region_code': {0: None},
                   'region_fk': {0: 1},
                   'region_name': {0: u'Test Bottler Region'},
                   'region_remarks': {0: None},
                   'sales_rep_name': {0: None},
                   'store_comment': {0: None},
                   'store_fk': {0: 11},
                   'store_name': {0: u'Bottler CR Store Test 1 - Geoff'},
                   'store_type': {0: 'Convenience Store/Pe'}}

    # test 2- incorrect values
    store_data2 = {'additional_attribute_1': {0: None},
                   'additional_attribute_12': {0: None},
                   'additional_attribute_2': {0: None},
                   'address_city': {0: None},
                   'branch_fk': {0: None},
                   'customer_name': {0: None},
                   'distribution_type': {0: None},
                   'manager_name': {0: None},
                   'region_code': {0: None},
                   'region_fk': {0: 1},
                   'region_name': {0: u'Test Bottler Region'},
                   'region_remarks': {0: None},
                   'sales_rep_name': {0: None},
                   'store_comment': {0: None},
                   'store_fk': {0: 11},
                   'store_name': {0: u'Bottler CR Store Test 1 - Geoff'},
                   'store_type': {0: u'CR'}}  # incorrect value

    def get_good_store_info(self):
        return pd.DataFrame(self.store_data1)

    def get_bad_store_info(self):
        return pd.DataFrame(self.store_data2)