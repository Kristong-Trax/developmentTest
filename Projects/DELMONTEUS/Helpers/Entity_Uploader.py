import pandas as pd
from collections import defaultdict
from Trax.Cloud.Services.Connector.Keys import DbUsers
from Trax.Data.Projects.Connector import ProjectConnector




class EntityUploader():
    PS_TYPE = 'PS scores'
    ENTITY_TYPE_TABLE = 'static.kpi_entity_type'
    CUSTOM_ENTITY_TABLE = 'static.custom_entity'

    def __init__(self, project, template_path):
        self.entity_pairs = self.parse_all(template_path)
        self.rds_conn = ProjectConnector(project, DbUsers.CalcAdmin)
        self.cur = self.rds_conn.db.cursor()

        self.custom_entity_attribs = self.get_table_attributes(self.CUSTOM_ENTITY_TABLE)
        self.entity_type_attribs = self.get_table_attributes(self.ENTITY_TYPE_TABLE)

        self.custom_entity_table = self.read_table(self.CUSTOM_ENTITY_TABLE)
        self.entity_type_table = self.read_table(self.ENTITY_TYPE_TABLE)

        self.custom_entity_set = set(self.custom_entity_table['name'])
        self.entity_type_set = set(self.entity_type_table['name'])

        self.custom_entity_pk = max([int(i) for i in self.custom_entity_table['pk']])
        self.entity_type_pk = max([int(i) for i in self.entity_type_table['pk'] if int(i) < 999])

        self.entity_type_dict = self.entity_type_table.set_index('name')['pk'].to_dict()

        self.update_db()

    def update_db(self):
        max_custom_entity_len = self.get_max_len(self.custom_entity_attribs)
        max_entity_type_len = self.get_max_len(self.entity_type_attribs)

        len_errors = []
        for entity_type, entity in self.entity_pairs:
            entity_type = entity_type.lower()
            if entity_type and entity_type not in self.entity_type_set:
                if len(entity_type) <= max_entity_type_len:
                    self.entity_type_pk += 1
                    self.insert_into_entity_types(entity_type)
                    self.entity_type_dict[entity_type] = self.entity_type_pk
                else:
                    len_errors.append('    Entity Type "{}" and Entity {} not added: {} Exceeds {} characters'\
                                      .format(entity_type, entity, entity_type, max_entity_type_len))
                    continue
            entity_type_fk = self.entity_type_dict[entity_type]
            if entity and entity not in self.custom_entity_set:
                if len(entity) <= max_custom_entity_len:
                    bc = 0
                    while 1:
                        self.custom_entity_pk += 1
                        try:
                            self.insert_into_custom_entity(entity, entity_type_fk)
                            break
                        except self.rds_conn.db.IntegrityError:
                            print('pk taken')
                            bc += 1
                        except Exception as e:
                            print(e)
                            break
                        if bc > 50:
                            break

                else:
                    len_errors.append('    Entity {} not added: Exceeds {} characters'\
                                      .format(entity, max_custom_entity_len))

            try:
                self.rds_conn.db.commit()
                self.entity_type_set.add(entity_type)
                self.custom_entity_set.add(entity)
            except Exception as e:
                print('{} Entities Failed to Upload due to: '
                      '    "{}"'.format(entity, e))

        if len_errors:
            print('Below Results not added')
            for len_error in len_errors:
                print(len_error)
        else:
            print('Results sucessfuly loaded')

    def parse_all(self, template_path):
        values = []
        for sheet, df in pd.read_excel(template_path, sheetname=None).items():
            if sheet == 'Result' or 'Map' in sheet:
                continue
            for i, line in df.iterrows():
                params = self.get_kpi_line_params(line)
                values += [(entity_type, entity) for entity_type, entity in params.items()]
        return set(sum([[(t, e) for e in es if not pd.isnull(e)] for t, es in values if not pd.isnull(t)], []))

    # def get_kpi_line_params(self, kpi_orig):
    #     print(kpi_orig)
    #     kpi_line = kpi_orig.copy()
    #     filters = defaultdict(list)
    #     attribs = [x.lower() if x.count(' ') < 2 else ' '.join(x.split(' ')[1:]).lower() for x in kpi_line.index]
    #     kpi_line.index = attribs
    #     c = 1
    #     while 1:
    #         if 'param {}'.format(c) in attribs and kpi_line['param {}'.format(c)]:
    #             filters[kpi_line['param {}'.format(c)]] += self.splitter(
    #                 kpi_line['value {}'.format(c)])
    #         else:
    #             if c > 3:  # just in case someone inexplicably chose a nonlinear numbering format.
    #                 break
    #         c += 1
    #     return filters

    def get_kpi_line_params(self, kpi_orig):
        kpi_line = kpi_orig.copy()
        filters = defaultdict(list)
        attribs = [x.lower() for x in kpi_line.index]
        kpi_line.index = attribs
        rel_attribs = [x for x in attribs if 'param' in x]
        for attrib in rel_attribs:
            filters[kpi_line[attrib]] = self.splitter(kpi_line[attrib.replace('param', 'value')])
        return filters

    @staticmethod
    def splitter(text_str, delimiter=','):
        ret = [text_str]
        if hasattr(text_str, 'split'):
            ret = text_str.split(delimiter)
        return ret

    def get_max_len(self, attribs):
        return int(attribs.loc[attribs['Field'] == 'name', 'Type'].values[0].split('(')[1].split(')')[0])

    def read_table(self, table):
        return pd.read_sql_query('''
        SELECT * FROM {};         
        '''.format(table), self.rds_conn.db)

    def insert_into_entity_types(self, entity_type):
        query = """
        Insert Into static.kpi_entity_type 
        Values ({}, '{}', 'static_new.products', 'labels', null, null)
        """.format(self.entity_type_pk, entity_type)
        self.cur.execute(query)

    def insert_into_custom_entity(self, entity, fk):
        q = '"' if "'" in entity else "'"
        query = '''
        Insert Into static.custom_entity 
        Values ({}, {}{}{}, {}, null)
        '''.format(self.custom_entity_pk, q, entity, q, fk)
        self.cur.execute(query)

    def get_table_attributes(self, table):
        return pd.read_sql_query('Show Columns FROM {};'.format(table), self.rds_conn.db)




