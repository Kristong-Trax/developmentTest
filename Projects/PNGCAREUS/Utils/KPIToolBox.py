import os

import pandas as pd
from datetime import datetime

from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Utils.Conf.Keys import DbUsers
from Trax.Data.Projects.Connector import ProjectConnector
from Trax.Utils.Logging.Logger import Log
from Trax.Data.Utils.MySQLservices import get_table_insertion_query as insert
from Projects.PNGCAREUS.Utils.ParseTemplates import parse_template
from Projects.PNGCAREUS.Utils.Fetcher import PNGCAREUSPNGCAREUSQueries
from Projects.PNGCAREUS.Utils.GeneralToolBox import PNGCAREUSPNGCAREUSGENERALToolBox

__author__ = 'Ortal'

BINARY = 'binary'
NUMBER = 'number'
KPI_RESULT = 'report.kpi_results'
KPK_RESULT = 'report.kpk_results'
KPS_RESULT = 'report.kps_results'
NUMERATOR = 'Numerator'
DENOMINATOR = 'Denominator'
ENTITY = 'Entity'

TEMPLATE_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', 'Data', 'Template.xlsx')


def log_runtime(description, log_start=False):
    def decorator(func):
        def wrapper(*args, **kwargs):
            calc_start_time = datetime.utcnow()
            if log_start:
                Log.info('{} started at {}'.format(description, calc_start_time))
            result = func(*args, **kwargs)
            calc_end_time = datetime.utcnow()
            Log.info('{} took {}'.format(description, calc_end_time - calc_start_time))
            return result

        return wrapper

    return decorator


class PNGCAREUSPNGCAREUSToolBox:
    LEVEL1 = 1
    LEVEL2 = 2
    LEVEL3 = 3

    def __init__(self, data_provider, output):
        self.output = output
        self.data_provider = data_provider
        self.project_name = self.data_provider.project_name
        self.session_uid = self.data_provider.session_uid
        self.products = self.data_provider[Data.PRODUCTS]
        self.all_products = self.data_provider[Data.ALL_PRODUCTS]
        self.match_product_in_scene = self.data_provider[Data.MATCHES]
        self.visit_date = self.data_provider[Data.VISIT_DATE]
        self.session_info = self.data_provider[Data.SESSION_INFO]
        self.scene_info = self.data_provider[Data.SCENES_INFO]
        self.store_id = self.data_provider[Data.STORE_FK]
        self.scif = self.data_provider[Data.SCENE_ITEM_FACTS]
        self.rds_conn = ProjectConnector(self.project_name, DbUsers.CalculationEng)
        self.tools = PNGCAREUSPNGCAREUSGENERALToolBox(self.data_provider, self.output, rds_conn=self.rds_conn)
        self.kpi_static_data = self.get_kpi_static_data()
        self.kpi_results_queries = []
        self.custom_templates = {}

    def get_kpi_static_data(self):
        """
        This function extracts the static KPI data and saves it into one global data frame.
        The data is taken from static.kpi / static.atomic_kpi / static.kpi_set.
        """
        query = PNGCAREUSPNGCAREUSQueries.get_all_kpi_data()
        kpi_static_data = pd.read_sql_query(query, self.rds_conn.db)
        return kpi_static_data

    def get_custom_template(self, template_path, name):
        if name not in self.custom_templates.keys():
            template = parse_template(template_path, sheet_name=name)
            if template.empty:
                template = parse_template(template_path, name, 2)
            self.custom_templates[name] = template
        return self.custom_templates[name]

    def main_calculation(self, kpi_set_fk, *args, **kwargs):
        """
        This function calculates the KPI results.
        """
        template_data = parse_template(TEMPLATE_PATH, 'KPIs')
        kpi_set_name = self.kpi_static_data[(self.kpi_static_data['kpi_set_fk'] == kpi_set_fk)]['kpi_set_name'][
            kpi_set_fk - 1]
        template_kps = template_data.loc[template_data['Set Name'] == kpi_set_name]
        for kpi_name in template_kps['KPI Name'].unique().tolist():
            kpi_data = template_kps.loc[template_kps['KPI Name'] == kpi_name]
            scene_type = [s for s in kpi_data['Scene Types'].values[0].encode().split(',')]
            kpi_type = kpi_data['KPI Type'].values
            if kpi_type in 'block together':
                self.calculate_block_together(kpi_set_fk, kpi_name, 'block together', scene_type)
            elif kpi_type == 'availability':
                self.calculate_availability(kpi_set_fk, kpi_name, 'availability', scene_type)
            elif kpi_type == 'count_of_scenes':
                self.calculate_count_of_scenes(kpi_set_fk, kpi_name, 'count_of_scenes')
            elif kpi_type == 'sequence blocks':
                self.calculate_blocks_in_sequence(kpi_set_fk, kpi_name, 'sequence blocks', scene_type)
            elif kpi_type == 'anchor':
                self.calculate_anchor(kpi_set_fk, kpi_name, 'anchor', scene_type)
            elif kpi_type in ['sos', 'sos per scene_type']:
                self.calculate_facings_sos(kpi_set_fk, kpi_name, 'sos', scene_type)
            elif kpi_type == 'block + availability':
                self.calculate_block_and_availability(kpi_set_fk, kpi_name, 'block + availability', scene_type)
            elif kpi_type == 'eye level':
                self.calculate_eye_level(kpi_set_fk, kpi_name, 'eye level', scene_type)
            elif kpi_type == 'relative position':
                self.calculate_relative_position_sets(kpi_set_fk, kpi_name, 'relative position', scene_type)
            elif kpi_type == 'count share of display':
                self.calculate_count_of_display_entity(kpi_set_fk, kpi_name, 'count share of display', scene_type)
            elif kpi_type == 'sequence':
                self.calculate_sequence_sets(kpi_set_fk, kpi_name, 'sequence', scene_type)
            elif kpi_type == 'special sequence':
                related_results = self.calculate_special_sequence(kpi_set_fk, kpi_name, 'special sequence', scene_type)
            elif kpi_type == 'related kpi':
                self.calculate_related_kpi(kpi_set_fk, kpi_name, related_results)
        return

    def calculate_availability(self, kpi_set_fk, kpi_name, tab, scene_type):
        if any(i in self.scif['template_name'].unique().tolist() for i in scene_type):
            template = self.get_custom_template(TEMPLATE_PATH, tab)
            kpi_template = template.loc[template['kpi Name'] == kpi_name]
            if kpi_template.empty:
                return None
            kpi_template = kpi_template.iloc[0]
            filters = {kpi_template['Param 1']: [s for s in kpi_template['Value 1'].split(',')],
                       'template_name': scene_type}
            result = self.tools.calculate_availability(**filters)
            if kpi_template['score'] == BINARY:
                score = 1 if result >= 1 else 0  # target is 1 facing
                self.write_to_db_result(kpi_set_fk, kpi_name=kpi_name, level=self.LEVEL3, result=result, score=score)
            elif kpi_template['score'] == NUMBER:
                score = result

                self.write_to_db_result(kpi_set_fk, kpi_name=kpi_name, level=self.LEVEL3, result=result, score=score)

    def calculate_related_kpi(self, kpi_set_fk, kpi_name, related_results):
        if related_results > 0:
            self.write_to_db_result(kpi_set_fk, kpi_name=kpi_name, level=self.LEVEL3, result=1, score=1)
        else:
            self.write_to_db_result(kpi_set_fk, kpi_name=kpi_name, level=self.LEVEL3, result=0, score=0)

    def calculate_count_of_scenes(self, kpi_set_fk, kpi_name, tab):
        template = self.get_custom_template(TEMPLATE_PATH, tab)
        kpi_template = template.loc[template['kpi Name'] == kpi_name]
        if kpi_template.empty:
            return None
        kpi_template = kpi_template.iloc[0]
        scene_type = [s for s in kpi_template['template_name'].split(',')]
        filters = {'template_name': scene_type}
        result = self.tools.calculate_number_of_scenes(**filters)
        score = result
        self.write_to_db_result(kpi_set_fk, kpi_name=kpi_name, level=self.LEVEL3, result=score, score=score)

    def calculate_anchor(self, kpi_set_fk, kpi_name, tab, scene_type):
        if any(i in self.scif['template_name'].unique().tolist() for i in scene_type):
            template = self.get_custom_template(TEMPLATE_PATH, tab)
            kpi_template = template.loc[template['kpi Name'] == kpi_name]
            if kpi_template.empty:
                return None
            kpi_template = kpi_template.iloc[0]
            filters = {'template_name': scene_type, kpi_template['param']: kpi_template['value']}
            category = kpi_template['category']
            position = kpi_template['lead']
            result = self.tools.calculate_products_on_edge(position=position, category=category, **filters)
            score = 1 if result[0] >= 1 else 0
            self.write_to_db_result(kpi_set_fk, kpi_name=kpi_name, level=self.LEVEL3, result=score, score=score)

    def calculate_eye_level(self, kpi_set_fk, kpi_name, tab, scene_type):
        if any(i in self.scif['template_name'].unique().tolist() for i in scene_type):
            template = self.get_custom_template(TEMPLATE_PATH, tab)
            kpi_template = template.loc[template['kpi Name'] == kpi_name]
            kpi_template = kpi_template.iloc[0]
            if kpi_template.empty:
                return None
            filters = {'template_name': scene_type, kpi_template['param']: kpi_template['value']}
            shelf_target = [int(str(s)) for s in kpi_template['shelf eye'].split(',')]
            result = self.tools.shelf_level_assortment(shelf_target, **filters)
            score = 1 if result >= 1 else 0
            self.write_to_db_result(kpi_set_fk, kpi_name=kpi_name, level=self.LEVEL3, result=score, score=score)

    def calculate_count_of_display_entity(self, kpi_set_fk, kpi_name, tab, scene_type):
        template = self.get_custom_template(TEMPLATE_PATH, tab)
        kpi_template = template.loc[template['kpi Name'] == kpi_name]
        kpi_template = kpi_template.iloc[0]
        if kpi_template.empty:
            return None
        scif_scenes = self.scif.loc[self.scif['template_name'].isin(scene_type)]
        scenes = scif_scenes['scene_id'].unique().tolist()
        count_displays = 0
        if scenes is not None:
            for scene in scenes:
                filters = {'scene_fk': scene}
                denominator = self.tools.calculate_availability(**filters)
                filters[kpi_template['Entity Type Numerator']] = [s for s in
                                                                     kpi_template['Numerator'].split(', ')]
                nominator = self.tools.calculate_availability(**filters)
                if denominator != 0:
                    result = str(float(nominator) / denominator)
                    result = float(result)
                    result = round(result, 3)
                    result *= 100
                else:
                    continue
                if result >= float(kpi_template['target']):
                    count_displays = +1
        score = count_displays
        self.write_to_db_result(kpi_set_fk, kpi_name=kpi_name, level=self.LEVEL3, result=score, score=score)

    def calculate_block_together(self, kpi_set_fk, kpi_name, tab, scene_type):
        template = self.get_custom_template(TEMPLATE_PATH, tab)
        kpi_template = template.loc[template['kpi Name'] == kpi_name]
        kpi_template = kpi_template.iloc[0]
        if kpi_template.empty:
            return None
        filters = {kpi_template['block param']: [block for block in kpi_template['block value'].split(',')]}
        include_empty = False
        scif_scenes = self.scif.loc[self.scif['template_name'].isin(scene_type)]
        scenes = scif_scenes['scene_id'].unique().tolist()
        score = 0
        if scenes is not None:
            for scene in scenes:
                filters['scene_id'] = scene
                res = self.tools.calculate_block_together(include_empty=include_empty,minimum_block_ratio=0.75,vertical=True,
                                                          horizontal=True,orphan=True, **filters)
                if res:
                    score = 1
                    self.write_to_db_result(kpi_set_fk, kpi_name=kpi_name, level=self.LEVEL3, result=score, score=score)
                    return
        else:
            score = 0
        self.write_to_db_result(kpi_set_fk, kpi_name=kpi_name, level=self.LEVEL3, result=score, score=score)

    def calculate_facings_sos(self, kpi_set_fk, kpi_name, tab, scene_type):
        """
        This function calculates simple facing Share of Shelf typed Atomic KPI, and writes it to the DB.
        """
        template = self.get_custom_template(TEMPLATE_PATH, tab)
        kpi_data = template.loc[template['kpi Name'] == kpi_name]
        if kpi_data.empty:
            return None
        kpi_data = kpi_data.iloc[0]
        if kpi_data['for_each']:
            for scene in scene_type:
                result = self.calculate_sos(kpi_data, scene)
                result = round(result, 2)
                self.write_to_db_result(kpi_set_fk, kpi_name=kpi_name + ' ' + scene.replace("'", "\\'"), level=self.LEVEL3,
                                        result=result,
                                        score=result)
        else:
            result = self.calculate_sos(kpi_data, scene_type)
            self.write_to_db_result(kpi_set_fk, kpi_name=kpi_name, level=self.LEVEL3, result=result,
                                    score=result)

    def calculate_sos(self, kpi_data, scene):
        numerator_filters = {
            kpi_data['Entity Type Numerator 1']: [s for s in kpi_data['Numerator 1'].split(',')], 'template_name':scene}
        if kpi_data['Numerator 2'] is not None:
            if kpi_data['Numerator 2'] != '':
                numerator_filters[kpi_data['Entity Type Numerator 2']] = kpi_data['Numerator 2']
        if kpi_data['Numerator 1'] == '1':
            numerator_filters[kpi_data['Entity Type Numerator 1']] = 1
        numerator_result = self.tools.calculate_availability(**numerator_filters)
        denominator_filters = {}
        if kpi_data['Entity Type Denominator'] is not None:
            if  kpi_data['Entity Type Denominator'] != '':
                denominator_filters = {
                    kpi_data['Entity Type Denominator']: [s for s in kpi_data['Denominator'].split(', ')]}
            denominator_filters['template_name'] = scene
            denominator_result = self.tools.calculate_availability(**denominator_filters)
        else:
            denominator_result = int(sum(self.scif['facings']))
        result = 0 if denominator_result == 0 else numerator_result / float(denominator_result)
        return result

    def calculate_blocks_in_sequence(self, kpi_set_fk, kpi_name, tab, scene_types):
        """
        This function calculates Atomics of type 'Blocks in sequence' - It checks whether 2 sets of parameters are
        positioned in a block, and then checks whether the two blocks are in a sequence.
        Later it saves the results to the DB.
        """
        template = self.get_custom_template(TEMPLATE_PATH, tab)
        kpi_data = template.loc[template['kpi Name'] == kpi_name]
        score = 0
        if kpi_data.empty:
            return None
        kpi_data = kpi_data.iloc[0]
        if scene_types:
            relevant_scenes = self.scif[self.scif['template_name'].isin(scene_types)]['scene_id'].unique().tolist()
            for scene in relevant_scenes:
                group1_filters = {kpi_data['Group 1 param']: kpi_data['Group 1 value'], 'scene_id': scene}
                if kpi_data['Group 1 param 2'] is not None:
                    if kpi_data['Group 1 value 2'] != '':
                        group1_filters[kpi_data['Group 1 param 2']] = [s for s in kpi_data['Group 1 value 2'].split(',')]
                group2_filters = {kpi_data['Group 2 param']: [s for s in kpi_data['Group 2 value'].split(', ')],
                                  'scene_id': scene}
                result1 = self.tools.calculate_block_together(scene_fk=scene, minimum_block_ratio=0.01,
                                                              **group1_filters)
                result2 = self.tools.calculate_block_together(scene_fk=scene, minimum_block_ratio=0.01,
                                                              **group2_filters)
                if result1 and result2:
                    direction_data = {'top': 0,
                                      'bottom': 0,
                                      'left': self._get_direction_for_relative_position('Y'),
                                      'right': self._get_direction_for_relative_position('Y')}
                    general_filters = {'scene_id': scene}
                    group1_filters.pop('scene_id')
                    group2_filters.pop('scene_id')
                    result = self.tools.calculate_relative_position(group1_filters, group2_filters, direction_data,
                                                                    **general_filters)
                    score += 1 if result else 0
                    if result:
                        if kpi_data['Group 3 param']:
                            group3_filters = {kpi_data['Group 3 param']: kpi_data['Group 3 value'], 'scene_id': scene}
                            result3 = self.tools.calculate_block_together(minimum_block_ratio=0.01, **group3_filters)
                            if result3:
                                group3_filters.pop('scene_id')
                                result4 = self.tools.calculate_relative_position(group2_filters, group3_filters,
                                                                                 direction_data, **general_filters)
                                score = 1 if result4 else 0
                                self.write_to_db_result(kpi_set_fk, kpi_name=kpi_name, level=self.LEVEL3, result=score,
                                                        score=score)

                                return

            final_score = 1 if score >= 1 else 0
            self.write_to_db_result(kpi_set_fk, kpi_name=kpi_name, level=self.LEVEL3, result=final_score,
                                    score=final_score)

    def calculate_sequence_sets(self, kpi_set_fk, kpi_name, tab, scene_types):
        """
        This function calculates every relative-position-typed KPI from the relevant sets, and returns the set final score.
        """
        template = self.get_custom_template(TEMPLATE_PATH, tab)
        kpi_data = template.loc[template['kpi Name'] == kpi_name]
        score = 0
        if kpi_data.empty:
            return None
        kpi_data = kpi_data.iloc[0]
        if scene_types:
            filters = (kpi_data['Group 1 param'], [kpi_data['Group 1 value'],kpi_data['Group 2 value'],
                                                   kpi_data['Group 3 value']])
            direction_data = [s for s in kpi_data['direction'].split(', ')]
            relevant_scenes = self.scif[self.scif['template_name'].isin(scene_types)]['scene_id'].unique().tolist()
            for scene in relevant_scenes:
                for direction in direction_data:
                    general_filters = {'scene_id': scene}
                    result = self.tools.calculate_product_sequence(sequence_filters=filters,
                                                                 direction=direction, **general_filters)
                    score += 1 if result else 0
        scores = 1 if score > 0 else 0
        self.write_to_db_result(kpi_set_fk, kpi_name=kpi_name, level=self.LEVEL3, result=scores, score=scores)


    def calculate_relative_position_sets(self, kpi_set_fk, kpi_name, tab, scene_types):
        """
        This function calculates every relative-position-typed KPI from the relevant sets, and returns the set final score.
        """
        template = self.get_custom_template(TEMPLATE_PATH, tab)
        kpi_data = template.loc[template['kpi Name'] == kpi_name]
        score = 0
        if kpi_data.empty:
            return None
        kpi_data = kpi_data.iloc[0]
        if scene_types:
            tested_filters = {kpi_data['Group 1 param']: kpi_data['Group 1 value']}
            anchor_filters = {kpi_data['Group 2 param']: kpi_data['Group 2 value']}
            direction_data = {'top': 0,
                              'bottom': 0,
                              'left': self._get_direction_for_relative_position(kpi_data['left']),
                              'right': self._get_direction_for_relative_position(kpi_data['right'])}
            relevant_scenes = self.scif[self.scif['template_name'].isin(scene_types)]['scene_id'].unique().tolist()
            for scene in relevant_scenes:
                general_filters = {'scene_id': scene}
                result = self.tools.calculate_relative_position(tested_filters, anchor_filters, direction_data,
                                                                **general_filters)
                score += 1 if result else 0
        scores = 1 if score > 0 else 0
        self.write_to_db_result(kpi_set_fk, kpi_name=kpi_name, level=self.LEVEL3, result=scores, score=scores)

    def calculate_block_and_availability(self, kpi_set_fk, kpi_name, tab, scene_types):
        """
        This function calculates every relative-position-typed KPI from the relevant sets, and returns the set final score.
        """
        template = self.get_custom_template(TEMPLATE_PATH, tab)
        kpi_data = template.loc[template['kpi Name'] == kpi_name]
        score = 0
        if kpi_data.empty:
            return None
        kpi_data = kpi_data.iloc[0]
        if scene_types:
            relevant_scenes = self.scif[self.scif['template_name'].isin(scene_types)]['scene_id'].unique().tolist()
            conditions = {kpi_data['Block param']: (0, [s for s in kpi_data['Block value'].split(', ')], 2),
                          'shelf_number': (0, None, 3),
                          kpi_data['availability param']: (
                          0, [s for s in kpi_data['availability value'].split(', ')], 1)}
            for scene in relevant_scenes:
                general_filters = {'scene_id': scene}
                result = self.tools.calculate_existence_of_blocks(conditions, **general_filters)
                score += 1 if result else 0
        scores = 1 if score > 0 else 0
        self.write_to_db_result(kpi_set_fk, kpi_name=kpi_name, level=self.LEVEL3, result=scores, score=scores)

    def calculate_special_sequence(self, kpi_set_fk, kpi_name, tab, scene_types):
        """
        This function calculates every relative-position-typed KPI from the relevant sets, and returns the set final score.
        """
        template = self.get_custom_template(TEMPLATE_PATH, tab)
        kpi_data = template.loc[template['kpi Name'] == kpi_name]
        score = 0
        if kpi_data.empty:
            return None
        kpi_data = kpi_data.iloc[0]
        if scene_types:
            general_filters = {'template_name': scene_types, kpi_data['Group 1 param']: kpi_data['Group 1 value']}
            dict_result = self.tools.calculate_flexible_blocks(**general_filters)
            result = sum(dict_result.values())
            score += result if result > 0 else 0
        self.write_to_db_result(kpi_set_fk, kpi_name=kpi_name, level=self.LEVEL3, result=score, score=score)


    def calculate_group_in_block(self, kpi_set_fk, kpi_name, tab, scene_types):
        """
        This function calculates every relative-position-typed KPI from the relevant sets, and returns the set final score.
        """
        template = self.get_custom_template(TEMPLATE_PATH, tab)
        kpi_data = template.loc[template['kpi Name'] == kpi_name]
        score = 0
        if kpi_data.empty:
            return None
        kpi_data = kpi_data.iloc[0]
        if scene_types:
            general_filters = {'template_name': scene_types, kpi_data['Group 1 param']: [kpi_data['Group 1 value'],
                                                                                         kpi_data['Group 2 value']]}
            block_filters = {kpi_data['Group 2 param']: kpi_data['Group 2 value']}
            group_filters = {kpi_data['Group 2 param']: kpi_data['Group 2 value']}
            dict_result = self.tools.calculate_block_together( allowed_products_filters=None,
                                 minimum_block_ratio=1, result_by_scene=False, vertical=False, horizontal=False,
                                 orphan=False, group=False)
            result = sum(dict_result.values())
            score += result if result > 0 else 0
        self.write_to_db_result(kpi_set_fk, kpi_name=kpi_name, level=self.LEVEL3, result=score, score=score)

    def _get_direction_for_relative_position(self, value):
        """
        This function converts direction data from the template (as string) to a number.
        """
        return_value = 0
        if value == 'Y':
            return_value = 1000
        return return_value

    def write_to_db_result(self, kpi_set_fk, result, level, score=None, threshold=None, kpi_name=None):
        """
        This function the result data frame of every KPI (atomic KPI/KPI/KPI set),
        and appends the insert SQL query into the queries' list, later to be written to the DB.
        """
        attributes = self.create_attributes_dict(kpi_set_fk, result=result, level=level, score=score,
                                                 threshold=threshold,
                                                 kpi_name=kpi_name)
        if level == self.LEVEL1:
            table = KPS_RESULT
        elif level == self.LEVEL3:
            table = KPI_RESULT
        else:
            return
        query = insert(attributes, table)
        self.kpi_results_queries.append(query)

    def create_attributes_dict(self, kpi_set_fk, result, level, score=None, threshold=None, kpi_name=None):
        """
        This function creates a data frame with all attributes needed for saving in KPI results tables.

        """
        if level == self.LEVEL1:
            kpi_set_name = \
            self.kpi_static_data[self.kpi_static_data['kpi_set_fk'] == kpi_set_fk]['kpi_set_name'].values[0]
            score_type = ''
            # attributes = pd.DataFrame([(kpi_set_name, self.session_uid, self.store_id, self.visit_date.isoformat(),
            #                             format(result, '.2f'), score_type, fk)],
            #                           columns=['kps_name', 'session_uid', 'store_fk', 'visit_date', 'score_1',
            #                                    'score_2', 'kpi_set_fk'])
            attributes = pd.DataFrame([(kpi_set_name, self.session_uid, self.store_id, self.visit_date.isoformat(),
                                        result, score_type, kpi_set_fk,)],
                                      columns=['kps_name', 'session_uid', 'store_fk', 'visit_date', 'score_1',
                                               'kpi_set_fk'])

        elif level == self.LEVEL3:
            kpi_set_name = \
            self.kpi_static_data[self.kpi_static_data['kpi_set_fk'] == kpi_set_fk]['kpi_set_name'].values[0]
            attributes = pd.DataFrame([(kpi_name, self.session_uid, kpi_set_name, self.store_id,
                                        self.visit_date.isoformat(), datetime.utcnow().isoformat(),
                                        result, kpi_set_fk, kpi_set_fk, threshold, score)],
                                      columns=['display_text', 'session_uid', 'kps_name', 'store_fk',
                                               'visit_date',
                                               'calculation_time', 'result', 'kpi_fk', 'atomic_kpi_fk',
                                               'threshold',
                                               'score'])
        else:
            attributes = pd.DataFrame()
        return attributes.to_dict()

    @log_runtime('Saving to DB')
    def commit_results_data(self):
        """
        This function writes all KPI results to the DB, and commits the changes.
        """
        self.rds_conn = ProjectConnector(self.project_name, DbUsers.CalculationEng)
        cur = self.rds_conn.db.cursor()
        delete_queries = PNGCAREUSPNGCAREUSQueries.get_delete_session_results_query(self.session_uid)
        for query in delete_queries:
            cur.execute(query)
        for query in self.kpi_results_queries:
            cur.execute(query)
        self.rds_conn.db.commit()

    @staticmethod
    def merge_insert_queries(insert_queries):
        query_groups = {}
        for query in insert_queries:
            static_data, inserted_data = query.split('VALUES ')
            if static_data not in query_groups:
                query_groups[static_data] = []
            query_groups[static_data].append(inserted_data)
        merged_queries = []
        for group in query_groups:
            merged_queries.append('{0} VALUES {1}'.format(group, ','.join(query_groups[group])))
        return merged_queries
