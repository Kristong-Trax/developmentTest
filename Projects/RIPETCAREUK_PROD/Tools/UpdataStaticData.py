from Trax.Cloud.Services.Connector.Keys import DbUsers
from Trax.Data.Projects.ProjectConnector import AwsProjectConnector
import pandas as pd
from Trax.Data.Utils.MySQLservices import get_table_insertion_query as insert
from Trax.Utils.Conf.Configuration import Config
# from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Trax.Utils.Logging.Logger import Log

from Projects.RIPETCAREUK_PROD.Tools.CreateHierarchy import CreateMarsUkKpiHierarchy
from Projects.RIPETCAREUK_PROD.Utils.Utils import get_kpi_static_data, get_kpi_set_static_data, get_atomic_kpi_static_data

__author__ = 'Dudi S'


class UpdateStaticData(object):

    def __init__(self, project_name):
        self.kpi_level_hierarchy_creator = CreateMarsUkKpiHierarchy()
        self.project_name = project_name
        self.rds_conn = AwsProjectConnector(project_name, DbUsers.CalculationEng)
        self.cur = self.rds_conn.db.cursor()
        self.current_kpi_set = get_kpi_set_static_data(self.rds_conn.db)
        self.current_kpi = get_kpi_static_data(self.rds_conn.db)
        self.current_atomic_kpi = get_atomic_kpi_static_data(self.rds_conn.db)
        self.queries = []

    def update_static_data(self):

        self.kpi_level_hierarchy_creator.create_hierarchy()
        self._insert_level_1_kpis()
        self._insert_level_2_kpis()
        self._insert_level_3_kpis()
        self.rds_conn.db.commit()

    def _insert_level_1_kpis(self):
        kpi_sets = self.kpi_level_hierarchy_creator.kpi_level_1_hierarchy
        for set_ind, set_data in kpi_sets.iterrows():
            set_name = set_data['set_name']
            if set_name not in self.current_kpi_set['kpi_set_name'].tolist():
                self._write_level_1_to_static(set_data)

    def _insert_level_2_kpis(self):
        kpis = self.kpi_level_hierarchy_creator.kpi_level_2_hierarchy
        for kpi_ind, kpi_data in kpis.iterrows():
            kpi_name = kpi_data['kpi_name']
            set_name = kpi_data['set_name']
            if kpi_name not in self.current_kpi[self.current_kpi['kpi_set_name'] == set_name]['kpi_name'].tolist():
                self._write_level_2_to_static(kpi_data)

    def _insert_level_3_kpis(self):
        kpis = self.kpi_level_hierarchy_creator.kpi_level_3_hierarchy
        for kpi_ind, kpi_data in kpis.iterrows():
            atomic_kpi_name = kpi_data['atomic_name']
            kpi_name = kpi_data['kpi_name']
            set_name = kpi_data['set_name']
            if atomic_kpi_name not in self.current_atomic_kpi[((self.current_atomic_kpi['kpi_set_name'] == set_name) &
                                                               (self.current_atomic_kpi['kpi_name'] == kpi_name))]['atomic_kpi_name'].tolist():
                self._write_level_3_to_static(kpi_data)

    def _write_level_1_to_static(self, level_1_record):
        attributes = {'name': [level_1_record['set_name']]}
        query = insert(attributes, 'static.kpi_set')
        self.cur.execute(query)
        new_set_fk = self.cur.lastrowid
        record_to_add_to_static = pd.DataFrame.from_records([{
            'kpi_set_name': level_1_record['set_name'],
            'kpi_set_fk': new_set_fk
        }])
        self.current_kpi_set = self.current_kpi_set.append(record_to_add_to_static, ignore_index=True)

    def _write_level_2_to_static(self, level_2_record):
        set_name = level_2_record['set_name']
        kpi_name = level_2_record['kpi_name']
        set_data = self.current_kpi_set[self.current_kpi_set['kpi_set_name'] == set_name]
        set_fk = set_data.iloc[0]['kpi_set_fk']
        attributes = {
            'display_text': [kpi_name],
            'kpi_set_fk': [set_fk]
        }
        query = insert(attributes, 'static.kpi')
        self.cur.execute(query)
        new_kpi_fk = self.cur.lastrowid
        record_to_add_to_static = pd.DataFrame.from_records([{
            'kpi_set_name': set_name,
            'kpi_name': kpi_name,
            'kpi_fk': new_kpi_fk,
            'kpi_set_fk': set_fk
        }])
        self.current_kpi = self.current_kpi.append(record_to_add_to_static, ignore_index=True)

    def _write_level_3_to_static(self, level_2_record):
        set_name = level_2_record['set_name']
        kpi_name = level_2_record['kpi_name']
        atomic_kpi_name = level_2_record['atomic_name']
        kpi_fk_cond = ((self.current_kpi['kpi_set_name'] == set_name) & (self.current_kpi['kpi_name'] == kpi_name))
        kpi_fk = self.current_kpi.loc[kpi_fk_cond, 'kpi_fk'].iloc[0]

        attributes = {
            'name': [atomic_kpi_name],
            'description': [atomic_kpi_name],
            'display_text': [atomic_kpi_name],
            'kpi_fk': [kpi_fk]
        }
        query = insert(attributes, 'static.atomic_kpi')
        self.cur.execute(query)

# if __name__ == '__main__':
#     Config.init()
#     LoggerInitializer.init('TREX')
#     project = 'ripetcareuk-prod'
#     update = UpdateStaticData(project)
#     update.update_static_data()