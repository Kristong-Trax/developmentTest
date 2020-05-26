from pathlib import Path
import pandas as pd
from Trax.Utils.Logging.Logger import Log
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Trax.Utils.Conf.Configuration import Config
from Trax.Data.Projects.Connector import ProjectConnector
from Trax.Cloud.Services.Connector.Keys import DbUsers


class KPITemplateValidater:
    def __init__(self, project_name):
        self.project_name = project_name
        self._template_path = Path(__file__).parent.parent/"Data/kpi_template.xlsx"
        self.template = self.load_template()
        self.data_mapping = [
            {
                "xl_column_name": "kpi_name",
                "xl_column_type": "string",
                "table_name": "static.kpi_level_2",
                "db_column_name": "type",
                "mandatory": True
            },
            {
                "xl_column_name": "report_label",
                "xl_column_type": "string",
                "table_name": "static.custom_entity",
                "db_column_name": "name",
                "mandatory": True
            },
            {
                "xl_column_name": "scene_type",
                "xl_column_type": "list",
                "table_name": "static.template",
                "db_column_name": "name",
                "mandatory": True
            },
            {
                "xl_column_name": "orientation",
                "xl_column_type": "string",
                "allowed_values": ["vertical", "horizontal"],
                "mandatory": True
            },
            {
                "xl_column_name": "include_empty",
                "xl_column_type": "string",
                "allowed_values": ["include", "exclude"],
                "mandatory": True
            },
            {
                "xl_column_name": "include_irrelevant",
                "xl_column_type": "string",
                "allowed_values": ["include", "exclude"],
                "mandatory": True
            },
            {
                "xl_column_name": "entity_1_type",
                "xl_column_type": "string",
                "allowed_values": ["product", "brand", "category", "sub_category"],
                "mandatory": True
            },
            {
                "xl_column_name": "entity_2_type",
                "xl_column_type": "string",
                "allowed_values": ["product", "brand", "category", "sub_category"],
                "mandatory": True
            },
            {
                "xl_column_name": "entity_1_values",
                "xl_column_type": "list",
                "mandatory": True
            },
            {
                "xl_column_name": "entity_2_values",
                "xl_column_type": "list",
                "mandatory": True
            },
            {
                "xl_column_name": "include_other_ean_codes",
                "xl_column_type": "list",
                "mandatory": False
            },
            {
                "xl_column_name": "start_date",
                "xl_column_type": "date",
                "mandatory": True
            },
            {
                "xl_column_name": "end_date",
                "xl_column_type": "date",
                "mandatory": True
            }
        ]

    def load_template(self):
        template = pd.read_excel(self._template_path, sheet_name='kpi_config', skiprows=1)
        return template

    def check_column_names(self):
        result = True
        valid_column_names = [column['xl_column_name'].strip().lower() for column in self.data_mapping]
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
        allowed_values = [column for column in self.data_mapping if column.get("allowed_values", False)]
        for allowed_value in allowed_values:
            name = allowed_value['xl_column_name']
            value = allowed_value['allowed_values']
            check = self.template[~self.template[name].str.lower().isin(value)]
            if not check.empty:
                Log.error("Invalid input, allowed_values: {}".format(value))
                for idx, row_data in check.iterrows():
                    Log.error("row_number: {}, column_name: {}, value: {}".format(idx+2, name, row_data[name]))
                result = False

        if result:
            Log.info("Allowed Values check completed")

        return result

    def check_db_values(self):
        result = True
        dbcon = ProjectConnector(self.project_name, DbUsers.CalculationEng)
        for check in self.data_mapping:
            if "table_name" in check.keys():
                xl_column_name = check['xl_column_name']
                xl_column_type = check['xl_column_type']
                db_column_name = check['db_column_name']
                table_name = check['table_name']
                for row_num, row_data in self.template.iterrows():
                    try:
                        values = row_data[xl_column_name].strip()
                        if xl_column_type == "list":
                            values = tuple(["{}".format(v.strip()) for v in values.split(",")])
                            query = "SELECT {db_column_name} FROM {table_name} WHERE {db_column_name} IN {values}"

                        else:
                            values = "{}".format(values)
                            query = "SELECT {db_column_name} FROM {table_name} WHERE {db_column_name}='{values}'"

                        query = query.format(db_column_name=db_column_name,
                                             table_name=table_name,
                                             values=values)
                        query = query.strip().replace(",)", ")")

                        df_result = pd.read_sql(query, dbcon.db)
                        if df_result.empty:
                            Log.error("{xl_column_name}={values} not in table {table_name}".format(xl_column_name=xl_column_name,
                                                                                                       values=values,
                                                                                                       table_name=table_name))
                            result = False
                        else:
                            if xl_column_type == "list":
                                if len(df_result) != len(values):
                                    message = "{xl_column_name}={values}".format(xl_column_name=xl_column_name,
                                                                                 values=values)
                                    message += "one or more values not found in table {table_name}".format(table_name=table_name)
                                    Log.error(message)
                                    result = False
                    except Exception as ex:
                        result = False
                        Log.error("{} {}".format(ex.message, ex.args))
        return result

    def validate(self):
        result = True
        if not self.check_column_names():
            result = False
        if not self.check_allowed_values():
            result = False
        if not self.check_db_values():
            result = False

        return result


if __name__ == "__main__":
    LoggerInitializer.init("LionJP")
    Config.init()
    project_name = "psapac-sand2"
    a = KPITemplateValidater(project_name)
    if a.validate():
        Log.info("Template Validation check - Completed")
    else:
        Log.warning("Template Validation check - Failed")

