
import pandas as pd
from Trax.Cloud.Services.Connector.Keys import DbUsers
from Trax.Data.Projects.Connector import ProjectConnector




class AtomicSpeculator():
    PS_TYPE = 'PS scores'
    KPI = 'static.kpi_level_2'
    ENTITY_TABLE = 'static.kpi_entity_type'
    FAMILY_TABLE = 'static.kpi_family'
    CALC_STAGE_TABLE = 'static.kpi_calculation_stage'

    def __init__(self, project, path):
        self.rds_conn = ProjectConnector(project, DbUsers.CalcAdmin)
        self.cur = self.rds_conn.db.cursor()

        self.kpi2_table = self.read_table(self.KPI)
        self.kpi2_set = set(self.kpi2_table['type'])
        self.kpi_pk_set = set(self.kpi2_table['pk'])

        self.entity_table = self.read_table(self.ENTITY_TABLE)
        self.family_table = self.read_table(self.FAMILY_TABLE)
        self.calc_stage_table = self.read_table(self.CALC_STAGE_TABLE)

        self.entity_set = set(self.entity_table['name'])
        self.family_set = {x.upper() for x in self.family_table['name']}
        self.calc_stage_set_pk = set(self.calc_stage_table['pk'])

        self.family_dict = self.family_table.set_index('name')['pk'].to_dict()
        self.entity_dict = self.entity_table.set_index('name')['pk'].to_dict()

        self.template = pd.read_excel(path, sheet_name='KPIs')



    def run(self):
        # pk = 30000
        pk = max(self.kpi2_table['pk']) + 1
        for i, row in self.template.iterrows():
            kpi_base = row['KPI Name']
            for i, lvl in enumerate([x.strip() for x in row['KPI Ganularity'].split(',')][::-1]):
                i += 1
                kpi = '{} - {}'.format(kpi_base, lvl)
                num = self.entity_dict[row['Numerator {}'.format(i)]]
                den = self.entity_dict[row['Denominator {}'.format(i)]]
                family = self.family_dict[row['KPI Type']]
                result = 2 if 'pass' in row['KPI Display Format'].lower() else 'Null'

                if kpi in ['Innovation Distribution - Session', 'Display by Location - Session']:
                    kpi = kpi.split('-')[0].strip() #  the exceptions to the rules :(
                self.update_kpi2(kpi, pk, family, num, den, result, 1, 0)

                if kpi not in self.kpi2_set:
                    self.insert_into_kpi2(kpi, pk, family, num, den, result, 1, 0)
                    pk += 1
        for fam in set(self.template['KPI Type']):
            if fam not in self.kpi2_set:
                self.insert_into_kpi2(fam, pk, family, 3, 5, 'Null', 1, 0)
                pk += 1

        self.rds_conn.db.commit()


    def read_table(self, table):
        return pd.read_sql_query('''
        SELECT * FROM {};         
        '''.format(table), self.rds_conn.db)

    def update_kpi2(self, kpi, pk, family_fk, num_fk, den_fk, result_fk, session, scene):
        query = '''
        update static.kpi_level_2
        set numerator_type_fk = {},
            denominator_type_fk = {}
        where type = "{}"
        '''.format(num_fk, den_fk, kpi)
        self.cur.execute(query)


    def insert_into_kpi2(self, kpi, pk, family_fk, num_fk, den_fk, result_fk, session, scene):
        query = '''
        INSERT INTO static.kpi_level_2
        (pk, type, client_name, kpi_family_fk, version, numerator_type_fk, denominator_type_fk, kpi_result_type_fk,
        valid_from, valid_until, initiated_by, kpi_calculation_stage_fk, session_relevance, scene_relevance)
        VALUES      
        ('{}', "{}", "{}", '{}', '1.0.0', '{}', '{}', {}, '1990-01-01', '2150-10-15', 'samk', '3', '{}', '{}')

        '''.format(pk, kpi, kpi, family_fk, num_fk, den_fk, result_fk, session, scene)
        print(query)
        self.cur.execute(query)

    def get_table_attributes(self, table):
        return pd.read_sql_query('Show Columns FROM {};'.format(table), self.rds_conn.db)