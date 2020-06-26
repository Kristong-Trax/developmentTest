from pathlib import Path
from Trax.Utils.Logging.Logger import Log
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Trax.Utils.Conf.Configuration import Config
from Trax.Data.Projects.Connector import ProjectConnector
from Trax.Cloud.Services.Connector.Keys import DbUsers
from collections import Counter
import pandas as pd
import datetime


class KPITemplateValidater:

    def __init__(self, project_name):
        self.project_name = project_name
        self._template_path = Path(__file__).parent.parent / "Data/TemplateQualitative.xlsx"
        self.template = self.load_template()
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

    def load_template(self):
        template = pd.read_excel(self._template_path, sheet_name='Hierarchy', skiprows=0)
        return template

    def check_column_names(self):
        result = True
        valid_column_names = [column['xl_column_name'].strip().lower()
                              for column in self.data_mapping]
        xl_column_names = [column.strip().lower() for column in self.template.columns]

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
            check = self.template[~self.template[name].str.lower().isin(value)]
            if not check.empty:
                Log.error("Invalid input, allowed_values: {}".format(value))
                for idx, row_data in check.iterrows():
                    Log.error("row_number: {}, column_name: {}, value: {}".format(
                        idx + 3, name, row_data[name]))
                result = False

        if result:
            Log.info("Allowed Values check completed")

        return result

    def check_db_values(self, xl_column_name, xl_column_type, db_column_name, table_name):
        result = True
        dbcon = ProjectConnector(self.project_name, DbUsers.CalculationEng)
        for row_num, row_data in self.template.iterrows():
            try:
                values = row_data[xl_column_name].strip()
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
                if df_result.empty:
                    message = u"{xl_column_name}={values} not in table {table_name}".format(
                        xl_column_name=xl_column_name,
                        values=values,
                        table_name=table_name)
                    Log.error(message.encode("utf-8"))
                    result = False
                else:
                    if xl_column_type == "list":
                        if len(df_result) != len(values):
                            message = u"{xl_column_name}={values}".format(xl_column_name=xl_column_name,
                                                                          values=values)
                            message += u" one or more values not found in table {table_name}".format(
                                table_name=table_name)
                            Log.error(message.encode("utf-8"))
                            result = False
            except Exception as ex:
                result = False
                Log.error("{} {}".format(ex.message, ex.args))
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

    def check_db_values_no_space(self, xl_column_name, xl_column_type, db_column_name, table_name):
        result = True
        dbcon = ProjectConnector(self.project_name, DbUsers.CalculationEng)
        for row_num, row_data in self.template.iterrows():
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
                if df_result.empty:
                    message = u"{xl_column_name}={values} not in table {table_name}".format(
                        xl_column_name=xl_column_name,
                        values=values,
                        table_name=table_name)
                    Log.error(message.encode("utf-8"))
                    result = False
                else:
                    if xl_column_type == "list":
                        if len(df_result) != len(values):
                            message = u"{xl_column_name}={values}".format(
                                xl_column_name=xl_column_name, values=values)
                            message += u" one or more values not found in table {table_name}".format(
                                table_name=table_name)
                            Log.error(message.encode("utf-8"))
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

        for row_num, row_data in self.template.iterrows():
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
