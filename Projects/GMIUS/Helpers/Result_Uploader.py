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
        self.result_value_attribs = self.get_table_attributes('static.kpi_result_value')
        self.sql_results = self.result_values_query()
        self.sql_types = self.result_types_query()
        self.ps_type_pk = None
        self.ps_result_pk = max(self.sql_results['pk'])

        self.update_db()

    def update_db(self):
        max_len = self.result_value_attribs.loc[self.result_value_attribs['Field'] == 'value', 'Type'].values[0]\
                                                                                                      .split('(')[1]\
                                                                                                      .split(')')[0]
        missing = set(self.template_results) - set(self.sql_results['value'])
        missing = list(self.template_results)
        for val in self.sql_results['value']:
            try:
                missing.remove(val)
            except:
                pass
        len_errors = []
        if len(missing) > 0:
            if self.PS_TYPE not in self.sql_types['name'].values:
                self.insert_into_types()
            else:
                self.ps_type_pk = self.sql_types.set_index('name')['pk'].to_dict()[self.PS_TYPE]

            for result in missing:
                if len(result) > int(max_len):
                    len_errors.append('    "{}": Exceeds {} characters'.format(result, max_len))
                    continue

                self.ps_result_pk += 1
                self.insert_into_values(result)

            try:
                self.rds_conn.db.commit()
            except Exception as e:
                print('Results Failed to Upload due to: '
                      '    "{}"'.format(e))

            if len_errors:
                print('Below Results not added')
                for len_error in len_errors:
                    print(len_error)
            else:
                print('Results sucessfuly loaded')


    def load_template_results(self, template_path):
        df = pd.read_excel(template_path, Const.RESULT)
        df = df[df['Entity'] == 'Y']
        data = sum([[item.strip() for item in row['Results Value'].split(row['Delimiter'])]
                        for i, row in df.iterrows()], [])
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

    def get_table_attributes(self, table):
        return pd.read_sql_query('Show Columns FROM {};'.format(table), self.rds_conn.db)




