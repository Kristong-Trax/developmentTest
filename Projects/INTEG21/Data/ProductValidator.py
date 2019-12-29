# -*- coding: utf-8 -*-
import os

import pandas as pd
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider
from Trax.Utils.Conf.Configuration import Config

from Trax.Utils.Logging.Logger import Log
from KPIUtils.Utils.Validators.Template.TemplateValidator import TemplateValidator

def productValidator():
    Config.init()
    project_name = 'integ21'

    session = 'e786fb4f-be86-44e1-9d75-917a14c48211'
    data_provider = KEngineDataProvider(project_name)
    # session = Common(data_provider).get_session_id(session)
    data_provider.load_session_data(session)
    file_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), '', 'products.xlsx')
    validate_file_path = '/home/Israel/Desktop/validate_marsus_products.xlsx'
    # validation_object = TemplateValidator(project_name, file_path, validate_file_path)
    # validation_object.run()
    a = pd.ExcelFile(file_path).parse('Revised Attributes')
    result = pd.DataFrame(data = a[['product_english_name', 'product_ean_code']], columns=a.columns)
    all_prod = data_provider.all_products[data_provider.all_products['category']=='Pet Food']
    for i in xrange(len(a)):
        row_excel = a.iloc[i]
        check = True
        row_prod = all_prod[all_prod['product_english_name'] == row_excel['product_english_name']]
        if not row_prod.empty:
            for column in a.columns:
                try:
                    if row_prod[column].values[0] and row_excel[column] != row_prod[column].values[0]:
                        result.loc[result['product_english_name'] == row_excel['product_english_name'], column] = 'excel-' + str(row_prod[column].values[0]) + ' VS db-' + str(row_excel[column])
                        check = False
                except:
                    result.loc[result['product_english_name'] == row_excel['product_english_name'], column] = 'error'
                    check = False
                    pass
        else:
            row_prod = all_prod[all_prod['product_ean_code'] == row_excel['product_ean_code']]
            if not row_prod.empty:
                for column in a.columns:
                    try:
                        if row_prod[column].values[0] and row_excel[column] != row_prod[column].values[0]:
                            result.loc[result['product_ean_code'] == row_excel[
                                'product_ean_code'], column] = 'excel-' + str(
                                row_prod[column].values[0]) + ' VS db-' + str(row_excel[column])
                            check = False
                    except:
                        result.loc[
                            result['product_ean_code'] == row_excel['product_ean_code'], column] = 'error'
                        check = False
                        pass
            else:
                result.loc[result['product_english_name'] == row_excel['product_english_name'], 'exists_in_db'] = 'No'
                check = False
        if check:
            result.loc[result['product_english_name'] == row_excel['product_english_name'], 'good'] = 'Yes'
    result.to_csv(validate_file_path)


# if __name__ == '__main__':
#     productValidator()
