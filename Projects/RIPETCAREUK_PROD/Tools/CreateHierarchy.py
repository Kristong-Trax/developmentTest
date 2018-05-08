import pandas as pd
from Projects.RIPETCAREUK_PROD.Utils.Const import PERFECT_STORE
from Projects.RIPETCAREUK_PROD.Utils.ParseTemplates import ParseMarsUkTemplates, KPIConsts

__author__ = 'Dudi S'


class CreateMarsUkKpiHierarchy(object):

    def __init__(self):
        self.kpi_level_1_hierarchy = pd.DataFrame(columns=['set_name'])
        self.kpi_level_2_hierarchy = pd.DataFrame(columns=['set_name', 'kpi_name'])
        self.kpi_level_3_hierarchy = pd.DataFrame(columns=['set_name', 'kpi_name', 'atomic_name'])

    def create_hierarchy(self):

        template = ParseMarsUkTemplates()
        templates_data = template.parse_templates()

        set_name = PERFECT_STORE
        self._add_kpi_level_1_to_hierarchy(set_name)

        perfect_score_pillars = self._get_kpis_by_group(templates_data, set_name)

        for pillar_ind, pillar_data in perfect_score_pillars.iterrows():
            pillar_name = pillar_data[KPIConsts.KPI_NAME]
            self._add_kpi_level_1_to_hierarchy(set_name=pillar_name)

            pillar_kpis = self._get_kpis_by_group(templates_data, pillar_name)
            for kpi_ind, kpi_data in pillar_kpis.iterrows():

                kpi_name = kpi_data[KPIConsts.KPI_NAME]
                self._add_kpi_level_2_to_hierarchy(set_name=pillar_name, kpi_name=kpi_name)
                target = kpi_data[KPIConsts.TARGET]
                target_sheet_name = target.replace(KPIConsts.TARGET_PREFIX, '')
                target_sheet_data = templates_data[target_sheet_name]
                kpi_target_template_data = self._filter_template_data_by_kpi_name_(target_sheet_data, kpi_name)

                for atomic_kpi_ind, atomic_kpi_data in kpi_target_template_data.iterrows():
                    atomic_name = atomic_kpi_data['Atomic Kpi NAME']
                    self._add_kpi_level_3_to_hierarchy(set_name=pillar_name, kpi_name=kpi_name, atomic_name=atomic_name)

        self.kpi_level_1_hierarchy = self.kpi_level_1_hierarchy.drop_duplicates(['set_name']).reset_index(drop=True)
        self.kpi_level_2_hierarchy = self.kpi_level_2_hierarchy.drop_duplicates(['set_name', 'kpi_name']).reset_index(drop=True)
        self.kpi_level_3_hierarchy = self.kpi_level_3_hierarchy.drop_duplicates(['set_name', 'kpi_name', 'atomic_name']).reset_index(drop=True)

    @staticmethod
    def _filter_template_data_by_kpi_name_(target_template_data, kpi_name):
        cond = (
            (target_template_data['KPI Name'] == kpi_name)
        )
        return target_template_data.loc[cond, :]

    @staticmethod
    def _get_kpis_by_group(templates_data, set_name):
        kpi_template = templates_data[KPIConsts.SHEET_NAME]
        perfect_score_record = kpi_template[kpi_template[KPIConsts.KPI_NAME] == set_name]
        perfect_score_tested_kpi_group = perfect_score_record.iloc[0][KPIConsts.TESTED_KPI_GROUP]
        perfect_score_pillars = kpi_template[kpi_template[KPIConsts.KPI_GROUP] == perfect_score_tested_kpi_group]
        return perfect_score_pillars

    def _add_kpi_level_1_to_hierarchy(self, set_name):
        self.kpi_level_1_hierarchy = self.kpi_level_1_hierarchy.append(pd.DataFrame(columns=['set_name'],
                                                                                    data=[set_name]),
                                                                       ignore_index=True)

    def _add_kpi_level_2_to_hierarchy(self, set_name, kpi_name):
        self. kpi_level_2_hierarchy = self.kpi_level_2_hierarchy.append(pd.DataFrame(columns=['set_name', 'kpi_name'],
                                                                                     data=[(set_name, kpi_name)]),
                                                                        ignore_index=True)

    def _add_kpi_level_3_to_hierarchy(self, set_name, kpi_name, atomic_name):
        self.kpi_level_3_hierarchy = self.kpi_level_3_hierarchy.append(pd.DataFrame(columns=['set_name',
                                                                                             'kpi_name',
                                                                                             'atomic_name'],
                                                                                    data=[(set_name, kpi_name, atomic_name)]),
                                                                       ignore_index=True)
