import os
import pandas as pd
from collections import defaultdict
from Projects.DELMONTEUS.Utils.Const import Const
from Trax.Cloud.Services.Connector.Keys import DbUsers
from Trax.Data.Projects.Connector import ProjectConnector


class AtomicFarse():
    PS_TYPE = 'PS scores'
    KPI = 'static.kpi_level_2'
    ENTITY_TABLE = 'static.kpi_entity_type'
    FAMILY_TABLE = 'static.kpi_family'
    CALC_STAGE_TABLE = 'static.kpi_calculation_stage'

    MIN = 3000
    MAX = 3200

    def __init__(self, project, template_path):
        self.template = pd.read_excel(template_path, sheetname=None)
        self.indexed_template = {key: df.set_index(Const.KPI_NAME) for key, df in self.template.items()
                                 if Const.KPI_NAME in df.columns}
        self.rds_conn = ProjectConnector(project, DbUsers.CalcAdmin)
        self.cur = self.rds_conn.db.cursor()

        self.kpi2_attribs = self.get_table_attributes(self.KPI)
        self.kpi2_table = self.read_table(self.KPI)
        self.kpi2_table = self.read_table(self.KPI)
        self.kpi2_set = set(self.kpi2_table['type'])
        self.kpi_pk_set = set(self.kpi2_table['pk'])

        self.family_attribs = self.get_table_attributes(self.FAMILY_TABLE)

        self.entity_table = self.read_table(self.ENTITY_TABLE)
        self.family_table = self.read_table(self.FAMILY_TABLE)
        self.calc_stage_table = self.read_table(self.CALC_STAGE_TABLE)

        self.entity_set = set(self.entity_table['name'])
        self.family_set = {x.upper() for x in self.family_table['name']}
        self.calc_stage_set_pk = set(self.calc_stage_table['pk'])

        self.family_dict = self.family_table.set_index('name')['pk'].to_dict()
        self.entity_dict = self.entity_table.set_index('name')['pk'].to_dict()

        self.pk_max_family = max(self.family_table['pk'])

        self.errors = []

        self.main()

    def main(self):
        self.update_family()

        max_len = self.get_max_len(self.kpi2_attribs, field_name='type')
        kpis = {x.strip() for x in self.template[Const.KPIS][Const.KPI_NAME].unique() if x}
        missing = list(kpis - self.kpi2_set)
        pk = self.special_pk(self.kpi2_table)
        # pk = 3000

        self.generic_update(missing, max_len, self.insert_into_kpi2, 'kpi level 2', tracker_set=self.kpi2_set, pk=pk,
                            gen_kwargs=1)

        if self.errors:
            print('Below Results not added')
            for error in self.errors:
                print(error)
        else:
            print('Results sucessfuly loaded')

    def update_family(self):
        max_len = self.get_max_len(self.family_attribs)
        families = {x.upper() for x in self.template[Const.KPIS][Const.TYPE].unique()}
        missing = list(families - self.family_set)

        self.generic_update(missing, max_len, self.insert_into_kpi_family, 'KPI Family', tracker_dict=self.family_dict,
                            tracker_set=self.family_set, pk=self.pk_max_family)

    def special_pk(self, table):
        range_pks = [i for i in table['pk'] if self.MIN <= i < self.MAX]
        return max(range_pks)

    def generic_update(self, iterable, max_len, update_function, group, tracker_dict={}, tracker_set=set(), pk=0,
                       gen_kwargs=0):
        for item in iterable:
            if len(item) <= max_len:
                pk += 1
                if gen_kwargs:
                    kwargs = self.get_aux(item)
                    update_function(item, pk, **kwargs)
                else:
                    update_function(item, pk)
                tracker_dict[item] = pk
            else:
                self.errors.append('    {} "{}" not added: Exceeds {} characters'
                                   .format(group, item, max_len))

            try:
                self.rds_conn.db.commit()
                tracker_set.add(item)
            except Exception as e:
                print('{} "{}" Failed to Upload due to: '
                      '    "{}"'.format(group, item, e))

    def get_aux(self, item):
        print(item)
        main_line = self.indexed_template[Const.KPIS].loc[item]
        session_lvl = 'Y'
        family = self.read_cell_from_line(main_line, Const.TYPE)[0]
        kpi_line = self.indexed_template[family].loc[item]
        # num_check = 'numerator' if [x for x in kpi_line.index if 'numerator' in x.lower()] else ''
        # den_check = [x for x in kpi_line.index if 'denominator' in x.lower()]

        # num = self.get_kpi_line_params(kpi_line, name=num_check)[-1][-1]
        # den = self.get_kpi_line_params(kpi_line, name='denominator')[-1][-1] if den_check else None
        use_result = self.read_cell_from_line(
            kpi_line, Const.RESULT) if Const.RESULT in kpi_line.index else None

        is_session = 1 if session_lvl else 0
        is_scene = 0 if session_lvl else 1
        family_fk = self.family_dict[family.upper()]
        result_fk = 2 if use_result else 'null'
        num_fk = 999  # self.entity_dict[num]
        den_fk = 999  # self.entity_dict[num] if den else 'null'

        return {'family_fk': family_fk, 'num_fk': num_fk, 'den_fk': den_fk, 'result_fk': result_fk,
                'session': is_session, 'scene': is_scene}

    def get_kpi_line_params(self, kpi_orig, name=''):
        kpi_line = kpi_orig.copy()
        if name:
            name = name.lower() + ' '
        filters = defaultdict(list)
        attribs = [x.lower() for x in kpi_line.index]
        kpi_line.index = attribs
        c = 1
        while 1:
            if '{}param {}'.format(name, c) in attribs and kpi_line['{}param {}'.format(name, c)]:
                filters[kpi_line['{}param {}'.format(
                    name, c)]] += self.splitter(kpi_line['{}value {}'.format(name, c)])
            else:
                if c > 3:  # just in case someone inexplicably chose a nonlinear numbering format.
                    break
            c += 1
        return filters

    @staticmethod
    def splitter(text_str, delimiter=','):
        ret = [text_str]
        if hasattr(text_str, 'split'):
            ret = text_str.split(delimiter)
        return ret

    @staticmethod
    def read_cell_from_line(line, col):
        try:
            val = line[col] if not pd.isnull(line[col]) else []
        except:
            val = []
        if val:
            if hasattr(val, 'split'):
                if ', ' in val:
                    val = val.split(', ')
                elif ',' in val:
                    val = val.split(',')
            if not isinstance(val, list):
                val = [val]

        return val

    def get_max_len(self, attribs, field_name='name'):
        return int(attribs.loc[attribs['Field'] == field_name, 'Type'].values[0].split('(')[1].split(')')[0])

    def read_table(self, table):
        return pd.read_sql_query('''
        SELECT * FROM {};         
        '''.format(table), self.rds_conn.db)

    def insert_into_kpi_family(self, family, pk):
        query = """
        Insert Into static.kpi_family
        Values ({}, '{}', 3)
        """.format(pk, family)
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
