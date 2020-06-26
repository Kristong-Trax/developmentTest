from pathlib import Path
from Trax.Utils.Logging.Logger import Log
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Trax.Utils.Conf.Configuration import Config
from Trax.Data.Projects.Connector import ProjectConnector
from Trax.Cloud.Services.Connector.Keys import DbUsers
from collections import Counter
from pandas import ExcelWriter
import pandas as pd
import datetime


class KPITemplateValidater:

    def __init__(self, project_name):
        self.project_name = project_name
        self._template_path = Path(__file__).parent.parent / "Data/TemplateQualitative.xlsx"
        self.hierarchy_template = self.load_template('Hierarchy')
        self.custom_product_groups = self.load_template('Product Groups')
        self.all_active_products = self.load_all_active_products()
        self.q_kpi_templates = self.load_q_kpi_templates()
        self.db_category_kpi = self.load_db_category_kpi()
        self.excel_writer = ExcelWriter('q_template_check_results.xlsx')
        self.data_mapping = [
            {
                "xl_column_name": "Category Name",
                "xl_column_type": "string",
                "table_name": "static_new.category",
                "db_column_name": "local_name",
                "mandatory": True
            },
            {
                "xl_column_name": "KPI name",
                "xl_column_type": "string",
                "table_name": "static.atomic_kpi",
                "db_column_name": "name",
                "mandatory": True
            },
            {
                "xl_column_name": "Scene Types to Include",
                "xl_column_type": "list",
                "table_name": "static.template",
                "db_column_name": "name",
                "mandatory": True
            },
            {
                "xl_column_name": "KPI Type",
                "xl_column_type": "string",
                "table_name": "static.kpi_set",
                "db_column_name": "name",
                "mandatory": True
            },
            {
                "xl_column_name": "Set Name",
                "xl_column_type": "string",
                "table_name": "static.kpi_set",
                "db_column_name": "name",
                "mandatory": True
            }
        ]

    @staticmethod
    def get_db_category_kpi_query():
        return """  
        select 
        k.display_text category_name,
        s.name kpi_type,
        replace(a.name, ' ','') kpi_name
        from 
        static.kpi k,
        static.kpi_set s,
        static.atomic_kpi a
        where 1=1 
        and k.kpi_set_fk = s.pk
        and a.kpi_fk = k.pk
        and s.name in ('Golden Zone', 'Adjacency', 'Perfect Execution', 'Anchor', 'Block')
        """

    def load_db_category_kpi(self):
        dbcon = ProjectConnector(self.project_name, DbUsers.CalculationEng)
        query = self.get_db_category_kpi_query()
        df_result = pd.read_sql(query, dbcon.db)
        if df_result.empty:
            Log.error("Category KPI mapping missing in DB")
            return pd.DataFrame()
        else:
            return df_result

    def load_q_kpi_templates(self):
        q_kpi_templates = {}
        sheet_names = self.hierarchy_template['KPI Type'].unique()
        for sheet_name in sheet_names:
            q_kpi_templates[sheet_name] = self.load_template(sheet_name)
        return q_kpi_templates

    def load_template(self, sheet_name):
        template = pd.read_excel(self._template_path, sheet_name=sheet_name, skiprows=0)
        return template

    def check_column_names(self):
        result = True
        valid_column_names = [column['xl_column_name'].strip().lower()
                              for column in self.data_mapping]
        xl_column_names = [column.strip().lower() for column in self.hierarchy_template.columns]

        diff = set(valid_column_names) - set(xl_column_names)
        if len(diff) != 0:
            Log.error("ERROR: Missing columns in Template")
            Log.error("{}".format(list(diff)))
            result = False
        else:
            Log.info("Columns check completed")
        return result

    def check_allowed_values(self):
        result = True
        allowed_values = [column for column in self.data_mapping if column.get(
            "allowed_values", False)]
        for allowed_value in allowed_values:
            name = allowed_value['xl_column_name']
            value = allowed_value['allowed_values']
            check = self.hierarchy_template[~self.hierarchy_template[name].str.lower().isin(value)]
            if not check.empty:
                Log.error("Invalid input, allowed_values: {}".format(value))
                for idx, row_data in check.iterrows():
                    Log.error("row_number: {}, column_name: {}, value: {}".format(
                        idx + 3, name, row_data[name]))
                result = False

        if result:
            Log.info("Allowed Values check completed")

        return result

    def check_db(self):
        result = True
        for check in self.data_mapping:
            if "table_name" in check.keys():
                xl_column_name = check['xl_column_name']
                xl_column_type = check['xl_column_type']
                db_column_name = check['db_column_name']
                table_name = check['table_name']
                if xl_column_name in ["KPI name"]:
                    result = self.check_db_values_no_space(
                        xl_column_name, xl_column_type, db_column_name, table_name)
                else:
                    result = self.check_db_values(
                        xl_column_name, xl_column_type, db_column_name, table_name)
        return result

    def check_db_values(self, xl_column_name, xl_column_type, db_column_name, table_name):
        result = True
        lst_rows= []
        dbcon = ProjectConnector(self.project_name, DbUsers.CalculationEng)
        for row_num, row_data in self.hierarchy_template.iterrows():
            try:
                if type(row_data[xl_column_name]) is str or type(row_data[xl_column_name]) is unicode:
                    values = row_data[xl_column_name].strip()
                else:
                    values = row_data[xl_column_name]

                if xl_column_type == "list":
                    values = tuple(["{}".format(v.strip()) for v in values.split(",")])
                    query = "SELECT {db_column_name} FROM {table_name} WHERE {db_column_name} IN {values}"

                else:
                    values = u"{}".format(values)
                    query = u"SELECT {db_column_name} FROM {table_name} WHERE "
                    query += u" {db_column_name}='{values}'"

                query = query.format(db_column_name=db_column_name,
                                     table_name=table_name,
                                     values=values)
                query = query.strip().replace(",)", ")")

                df_result = pd.read_sql(query, dbcon.db)
                dict_result = dict()
                if df_result.empty:
                    dict_result['column_name'] = xl_column_name
                    dict_result['values'] = values
                    dict_result['table_name'] = table_name
                    Log.error(dict_result)
                    lst_rows.append(dict_result)
                    result = False
                else:
                    if xl_column_type == "list":
                        if len(df_result) != len(values):
                            dict_result['column_name'] = xl_column_name
                            dict_result['values'] = values
                            dict_result['table_name'] = table_name
                            Log.error(dict_result)
                            lst_rows.append(dict_result)
                            result = False
            except Exception as ex:
                result = False
                Log.error("{} {}".format(ex.message, ex.args))
        return result

    def check_db_values_no_space(self, xl_column_name, xl_column_type, db_column_name, table_name):
        result = True
        lst_rows = []
        dbcon = ProjectConnector(self.project_name, DbUsers.CalculationEng)
        for row_num, row_data in self.hierarchy_template.iterrows():
            try:
                values = row_data[xl_column_name].strip()
                if xl_column_type == "list":
                    values = tuple(["{}".format(v.strip()).replace(" ", "")
                                    for v in values.split(",")])
                    query = "SELECT {db_column_name} FROM {table_name} WHERE {db_column_name} IN {values}"

                else:
                    values = u"{}".format(values.replace(" ", ""))
                    query = u"SELECT {db_column_name} FROM {table_name} WHERE "
                    query += u" replace({db_column_name},' ','')='{values}'"

                query = query.format(db_column_name=db_column_name,
                                     table_name=table_name,
                                     values=values)
                query = query.strip().replace(",)", ")")

                df_result = pd.read_sql(query, dbcon.db)
                dict_result = dict()
                if df_result.empty:
                    dict_result['column_name'] = xl_column_name
                    dict_result['values'] = values
                    dict_result['table_name'] = table_name
                    Log.error(dict_result)
                    lst_rows.append(dict_result)
                    result = False
                else:
                    if xl_column_type == "list":
                        if len(df_result) != len(values):
                            dict_result['column_name'] = xl_column_name
                            dict_result['values'] = values
                            dict_result['table_name'] = table_name
                            Log.error(dict_result)
                            lst_rows.append(dict_result)
                            result = False
            except Exception as ex:
                result = False
                Log.error("{} {}".format(ex.message, ex.args))
        return result

    def check_entity_values(self, entity_name_key, entity_value_key):
        result = True
        dbcon = ProjectConnector(self.project_name, DbUsers.CalculationEng)

        if entity_name_key == "others":
            entity = {
                'others': ('static_new.product', 'ean_code')
            }
        else:
            entity = {
                'product': ('static_new.product', 'ean_code'),
                'brand': ('static_new.brand', 'name'),
                'category': ('static_new.category', 'name'),
                'sub_category': ('static_new.sub_category', 'name')
            }

        for row_num, row_data in self.hierarchy_template.iterrows():
            try:
                if entity_name_key != "others":
                    xl_entity_type = row_data[entity_name_key]
                else:
                    xl_entity_type = "others"

                if pd.isnull(row_data[entity_value_key]):
                    continue

                xl_entity_values = row_data[entity_value_key].strip()

                if xl_entity_type not in entity.keys():
                    Log.error("{} not an valid entity, allowed entities={}".format(
                        xl_entity_type, entity.keys()))
                    result = False
                    continue

                entity_table_name, entity_column_name = entity[xl_entity_type]
                values = tuple(["{}".format(v.strip()) for v in xl_entity_values.split(",")])
                values_count = Counter(values)
                for value in values_count.items():
                    if value[1] > 1:
                        Log.error("row_number:{} Duplicate(s) values={} found in {}".format(row_num + 2,
                                                                                            entity_name_key,
                                                                                            value))
                        result = False

                query = "SELECT {db_column_name} FROM {table_name} WHERE {db_column_name} IN {values}"
                query = query.format(db_column_name=entity_column_name,
                                     table_name=entity_table_name, values=values)
                query = query.strip().replace(",)", ")")
                df_result = pd.read_sql(query, dbcon.db)
                if df_result.empty:
                    message = "row_num:{row_num} {xl_column_name}={values} ".format(row_num=row_num + 3,
                                                                                    xl_column_name=entity_column_name,
                                                                                    values=values)
                    message += " not in table {table_name}".format(table_name=entity_table_name)
                    Log.error(message)
                    result = False
                else:
                    entity_values_rt = set(df_result[entity_column_name].unique())
                    entity_values_ck = set(values)
                    missing_values = list(entity_values_ck - entity_values_rt)
                    if len(missing_values) != 0:
                        message = "row_number:{} {} =>{} missing values={}:".format(row_num + 3,
                                                                                    entity_name_key,
                                                                                    entity_column_name,
                                                                                    missing_values)
                        Log.error(message)
                        result = False
            except Exception as ex:
                result = False
                Log.error("{} {}".format(ex.message, ex.args))
        return result

    def check_perfect_execution(self):
        result = True
        sheet_name = 'Perfect Execution'
        k_column_names = ['KPI test name', 'Category Name']
        h_column_names = ['KPI name', 'Category Name']
        kpi_template = self.q_kpi_templates[sheet_name][k_column_names].copy()
        hierarchy_filter = self.hierarchy_template[h_column_names].copy()

        for kpi_row_num, kpi_row_data in kpi_template.iterrows():
            found = False
            for row_num, row_data in hierarchy_filter.iterrows():
                if kpi_row_data['KPI test name'] == row_data['KPI name'] and kpi_row_data['Category Name'] == row_data['Category Name']:
                    found = True
                    break
            if not found:
                result = False
                message = u"'Sheet name': '{}' |'Category Name': '{}' | 'KPI test name': '{}' not found"
                message = message.format(sheet_name, kpi_row_data['Category Name'], kpi_row_data['KPI test name'])
                Log.error(message)

        return result

    def reconcile_hierarchy_entries(self):
        result = True
        column_names = ['KPI name', 'Category Name']

        for sheet_name, q_kpi_template in self.q_kpi_templates.items():
            kpi_template = q_kpi_template[column_names].copy()
            hierarchy_filter = self.hierarchy_template[self.hierarchy_template['KPI Type'] == sheet_name][column_names].copy()

            for row_num, row_data in hierarchy_filter.iterrows():
                found = False
                for kpi_row_num, kpi_row_data in kpi_template.iterrows():
                    if row_data['KPI name'] == kpi_row_data['KPI name'] and row_data['Category Name'] == kpi_row_data['Category Name']:
                        found = True
                        break
                if not found:
                    result = False
                    message = u"'Sheet name': {} | 'Category Name':{} | 'KPI name': '{}' not found".format(sheet_name,
                                                                                                           row_data['Category Name'],
                                                                                                           row_data['KPI name'])
                    Log.error(message)
        return result

    def check_category_kpi_all_kpi_types(self):
        result = True
        hierarchy_kpi = self.hierarchy_template.copy()

        if self.check_category_kpi(hierarchy_kpi, "Hierarchy"):
            result = False

        for sheet_name, kpi_template in self.q_kpi_templates.items():
            df_kpi_template = kpi_template.copy()
            df_kpi_template['KPI Type'] = sheet_name
            if self.check_category_kpi(df_kpi_template, sheet_name):
                result = False
        return result

    def check_category_kpi(self, df_kpi, sheet_name):
        result = True
        dict_rows = []
        category_kpi = self.db_category_kpi.copy()
        for row_num, row_data in df_kpi.iterrows():
            found = False
            dict_row = {}
            for c_row_num, c_row_data in category_kpi.iterrows():
                if row_data['Category Name'] == c_row_data['category_name'] and row_data['KPI name'].replace(" ", "") == c_row_data['kpi_name'] and row_data['KPI Type'] == c_row_data['kpi_type']:
                    found = True
                    break
            if not found:
                result = False
                dict_row['Sheet Name'] = sheet_name
                dict_row['Category Name'] = row_data['Category Name']
                dict_row['KPI Type'] = row_data['KPI Type']
                dict_row['KPI name'] = row_data['KPI name']
                dict_rows.append(dict_row)
                Log.error(dict_row)
        df = pd.DataFrame(dict_rows)
        df.to_excel(self.excel_writer, 'check_category')
        self.excel_writer.save()
        return result

    def check_product_groups(self):
        result = True
        dict_no_prd_rows = []
        dict_multi_cat_rows = []
        dict_cat_name_not_equal = []
        df_pg = self.custom_product_groups[self.custom_product_groups['Product EAN Code'].notnull()].copy()
        for row_num, row_data in df_pg.iterrows():
            ean_codes = [x.strip() for x in row_data['Product EAN Code'].split(",")]
            df_products = self.all_active_products[self.all_active_products['ean_code'].isin(ean_codes)]
            category = [x.encode('utf-8') for x in list(df_products['category_name'].unique())]
            dict_no_prd = {}
            dict_multi_cat = {}
            dict_cat_name = {}
            if len(category) == 0:
                result = False
                dict_no_prd['Product Group Id'] = row_data['Group Id']
                dict_no_prd['Category Name'] = row_data['Category Name']
                dict_no_prd_rows.append(dict_no_prd)
            elif len(category) > 1:
                result = False
                for cat in category:
                    dict_multi_cat['Product Group Id'] = row_data['Group Id']
                    dict_multi_cat['Category Name'] = cat.decode('utf-8')
                    dict_multi_cat_rows.append(dict_multi_cat)
            else:
                if category[0].decode('utf-8') != row_data['Category Name']:
                    result = False
                    dict_cat_name['DB Category Name'] = category[0].decode('utf-8')
                    dict_cat_name['Temp Category Name'] = row_data['Category Name']
                    dict_cat_name_not_equal.append(dict_cat_name)
        df = pd.DataFrame(dict_no_prd_rows)
        df.to_excel(self.excel_writer, 'missing_products')

        df = pd.DataFrame(dict_multi_cat_rows)
        df.to_excel(self.excel_writer, 'multiple_categories')

        df = pd.DataFrame(dict_cat_name_not_equal)
        df.to_excel(self.excel_writer, 'missing_categories')

        self.excel_writer.save()

        return result

    @staticmethod
    def get_product_group_query():
        return """select
            c.local_name category_name,
            p.ean_code,
            p.local_name product_name,
            p.is_active,
            p.delete_date 
            from 
            static_new.product p,
            static_new.category c
            where 
            p.category_fk = c.pk
            and p.ean_code is not null
            and p.is_active =1
            and p.delete_date is null"""

    def load_all_active_products(self):
        result = pd.DataFrame()
        query = self.get_product_group_query()
        dbcon = ProjectConnector(self.project_name, DbUsers.CalculationEng)
        try:
            df_result = pd.read_sql(query, dbcon.db)
            if df_result.empty:
                Log.error("Category KPI mapping missing in DB")
                return result
            else:
                result = df_result
        except Exception as ex:
            Log.error("Error: {}".format(ex))
        return result

    @staticmethod
    def validate_date_format(date_text):
        result = True
        try:
            datetime.datetime.strptime(date_text, '%m/%d/%Y')  # 12/31/2030
        except ValueError:
            # Log.error("Error : {}".format(e))
            result = False
            Log.info("Incorrect data format for value {}, should be '%m/%d/%Y' - MM/DD/YYYY.".format(date_text))
        return result

    def validate(self):
        result = True
        if not self.check_product_groups():
            result = False
        if not self.check_category_kpi_all_kpi_types():
            result = False
        if not self.reconcile_hierarchy_entries():
            result = False
        if not self.check_perfect_execution():
            result = False
        if not self.check_column_names():
            result = False
        if not self.check_db():
            result = False
        return result


if __name__ == "__main__":
    LoggerInitializer.init("PNGJP")
    Config.init()
    project_name = "pngjp"
    a = KPITemplateValidater(project_name)
    if a.validate():
        Log.info("Template Validation check - Completed")
    else:
        Log.warning("Template Validation check - Failed")
