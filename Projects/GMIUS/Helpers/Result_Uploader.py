import os
import pandas as pd
from Projects.GMIUS.Utils.Const import Const
from Trax.Cloud.Services.Connector.Keys import DbUsers
from Trax.Data.Projects.Connector import ProjectConnector




class ResultUploader():
    PS_TYPE = 'PS scores'
    def __init__(self, project, template_path):
        self.template_results = self.load_template_results(template_path)
        self.rds_conn = ProjectConnector(project, DbUsers.CalcAdmin)
        self.cur = self.rds_conn.db.cursor()
        self.sql_results = self.result_values_query()
        self.sql_types = self.result_types_query()
        self.ps_type_pk = None
        self.ps_result_pk = max(self.sql_results['pk'])

        self.update_db()

    def update_db(self):
        missing = self.template_results - set(self.sql_results['value'])
        if len(missing) > 1:
            if self.PS_TYPE not in self.sql_types['name'].values:
                self.insert_into_types()
            else:
                self.ps_type_pk = self.sql_types.set_index('name')['pk'].to_dict()[self.PS_TYPE]

            for result in missing:
                self.ps_result_pk += 1
                try:
                    self.insert_into_values(result)
                except:
                    pass
            self.rds_conn.db.commit()

    def load_template_results(self, template_path):
        df = pd.read_excel(template_path, Const.RESULT)
        df = df[df['Entity'] == 'Y']
        data = set(sum([[item.strip() for item in row['Results Value'].split(row['Delimiter'])]
                        for i, row in df.iterrows()], []))
        return data

    def load_sql_results(self):
        return set(pd.read_sql_query(self.result_values_query(), self.rds_conn.db)['value'])

    def result_values_query(self):
        return pd.read_sql_query('''
        SELECT * FROM static.kpi_result_value;         
        ''', self.rds_conn.db)

    def result_types_query(self):
        return pd.read_sql_query('''
        SELECT * FROM static.kpi_result_type;         
        ''', self.rds_conn.db)

    def insert_into_types(self):
        self.ps_type_pk = max(self.sql_types['pk']) + 1
        query = "Insert Into static.kpi_result_type Values ({}, '{}', null)".format(self.ps_type_pk, self.PS_TYPE)
        self.cur.execute(query)

    def insert_into_values(self, result):
        query = '''
        Insert Into static.kpi_result_value 
        Values ({}, '{}', {})
        '''.format(self.ps_result_pk, result, self.ps_type_pk)
        self.cur.execute(query)



