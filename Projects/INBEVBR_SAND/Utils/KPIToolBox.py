import os
import pandas as pd
from datetime import datetime

from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
from Trax.Data.Projects.Connector import ProjectConnector
from Trax.Utils.Conf.Keys import DbUsers

from Trax.Utils.Logging.Logger import Log
from Trax.Data.Utils.MySQLservices import get_table_insertion_query as insert
from Projects.INBEVBR_SAND.Utils.ParseTemplates import parse_template
from Projects.INBEVBR_SAND.Utils.Fetcher import INBEVBR_SANDQueries
from Projects.INBEVBR_SAND.Utils.GeneralToolBox import INBEVBR_SANDGENERALToolBox




__author__ = 'Yasmin'

KPI_RESULT = 'report.kpi_results'
KPK_RESULT = 'report.kpk_results'
KPS_RESULT = 'report.kps_results'
GAME_PLAN_TAB = 'Game Plan'
COUNT_OF_SCENES = 'Count of Scenes'
AVAILABILITY = 'Availability'
AVAILABILITY_TAB = 'Availability'
FACING_SOS_TAB = 'SOS Targets'
ASSORTMENT_TAB = 'Assortment'
BRAND_BLOCK_TAB = 'Brand Block'
SHELF_SEQUENCE_TAB = 'Shelf Sequence'
EXTRA_POINT_TAB = 'Extra Point'
# os.path.dirname(os.path.realpath(__file__))
TEMPLATE_PATH = os.path.join('/home/Yasmin/dev/trax_ace_factory/Projects/INBEVBR_SAND/', 'Data',
                             'inbevBR setupfile.xlsx')
NEW_TEMPLATE_DATE = '2016-08-13'


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


class INBEVBR_SANDToolBox:
    LEVEL1 = 1
    LEVEL2 = 2
    LEVEL3 = 3

    def __init__(self, data_provider, output):
        self.k_engine = BaseCalculationsScript(data_provider, output)
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
        self.store_data = self.data_provider[Data.STORE_INFO]
        self.rds_conn = ProjectConnector(self.project_name, DbUsers.CalculationEng)
        self.tools = INBEVBR_SANDGENERALToolBox(self.data_provider, self.output, rds_conn=self.rds_conn,
                                                geometric_kpi_flag=True)
        self.kpi_static_data = self.get_kpi_static_data()
        self.survey_response = self.data_provider[Data.SURVEY_RESPONSES]
        self.displays = self.get_match_display()
        self.kpi_results_queries = []
        self.template_path = self.get_template_path()
        self.custom_templates = {}
        self.store_type = self.store_data['store_type'].values[0]
        self.group = self.get_group()
        self.state = self.get_state()

        self.filters = {'Manufacturer': 'manufacturer_name', 'Category': 'category', 'Sub_category': 'sub_category',
                        'Sub Category':'sub_category', 'Sub Brand': 'sub_brand_name', 'att1': 'att1',
                        'Brand': 'brand_name', 'Att1':'att1', 'SKU':'product_name','Product_EAN':'product_ean_code',
                        'Template Name': 'template_name', 'Template Group':'template_group', 'Size':'size'}

    def get_template_path(self):
        visit_date = self.session_info['visit_date'].iloc[0]
        template_date = datetime.strptime(NEW_TEMPLATE_DATE,'%Y-%m-%d').date()
        filename = 'Setupfile_current.xlsx' if visit_date >= template_date else 'Setupfile_previous.xlsx'

        return os.path.join('/home/Yasmin/dev/trax_ace_factory/Projects/INBEVBR_SAND/', 'Data',
                            filename)

    def get_custom_template(self, name, lower_headers_row_index=0, upper_headers_row_index=None,
                            data_content_column_index=None, output_column_name_separator=None, converters=None):
        if name not in self.custom_templates.keys():
            template = parse_template(self.template_path, name, lower_headers_row_index, upper_headers_row_index,
                                      data_content_column_index=data_content_column_index,
                                      output_column_name_separator=output_column_name_separator, converters=converters)
            if template.empty:
                template = parse_template(self.template_path, name, 2)
            self.custom_templates[name] = template
        return self.custom_templates[name]

    def get_kpi_static_data(self):
        """
        This function extracts the static KPI data and saves it into one global data frame.
        The data is taken from static.kpi / static.atomic_kpi / static.kpi_set.
        """
        query = INBEVBR_SANDQueries.get_all_kpi_data()
        kpi_static_data = pd.read_sql_query(query, self.rds_conn.db)
        return kpi_static_data

    def main_calculation(self, *args, **kwargs):
        """
        This function calculates the KPI results.
        """
        kpi_template = self.get_custom_template('KPIs')
        kpi_set_fk = kwargs['kpi_set_fk']
        set_name = self.kpi_static_data[self.kpi_static_data['kpi_set_fk'] == kpi_set_fk]['kpi_set_name'].iloc[0]
        kpi_template = kpi_template.loc[kpi_template['KPI Group'] == set_name]
        set_score = 0
        for index, row in kpi_template.iterrows():
            score = 0
            score2 = None
            kpi_target = row['Target']
            relevant_tab = row['Tab']
            kpi_name = row['KPI Display Name']
            if kpi_target and 'Target' in row.keys().tolist():
                if not row['Target'].replace(' ', '').isalpha():
                    kpi_target = float(row['Target'])

            if row['WEIGHT']:
                kpi_weight = float(row['WEIGHT'])
            else:
                kpi_weight = 1
            kpi_data = self.kpi_static_data[self.kpi_static_data['kpi_name'] == kpi_name]
            if not kpi_data.empty:
                kpi_data = kpi_data.loc[kpi_data['kpi_set_fk'] == kpi_set_fk].iloc[0]
                kpi_fk = kpi_data['kpi_fk']

            if row['Tab'] == GAME_PLAN_TAB:
                score = self.calculate_game_plan(kpi_name, kpi_set_fk, kpi_set_name=set_name)

            elif row['Tab'] == EXTRA_POINT_TAB:
                score = self.extra_points(kpi_name, kpi_set_fk)

            elif row['KPI Type'] == 'Facing SOS':
                score, score2 = self.calculate_facingSOS(kpi_name, kpi_set_fk, relevant_tab)

            elif row['KPI Type'] in ('Percentage of passed KPI\'s', 'Number of passed KPI\'s') :
                score, score2 = self.calculate_passed_kpis(kpi_name, kpi_set_fk, relevant_tab, kpi_target)

            elif row['KPI Type'] == 'Block Together in Sequence':
                score, score2 = self.calculate_shelf_sequence(kpi_name, kpi_set_fk)

            elif row['KPI Type'] == 'Availability':
                score, score2 = self.calculate_availability(kpi_name, kpi_set_fk, relevant_tab, kpi_target)

            elif row['KPI Type'] == 'Survey Question':
                question = row['Survey_Question_id'].encode('utf-8')
                if question in self.survey_response['question_text'].tolist():
                    score, result = self.tools.check_survey_answer(question, kpi_target)
                    kpi_score = 1 if score else 0

                    atomic_fk = kpi_data['atomic_kpi_fk']
                    self.write_to_db_result(atomic_fk, result, self.LEVEL3, score=kpi_score,
                                            threshold=kpi_target)
                else:
                    score = None

            elif row['KPI Type'] == 'Display Count':
                display_names = self.displays.loc[self.displays['display_type'] == row['Display Type Name']]
                result = len(display_names)
                kpi_score = 1 if score > kpi_target else 0
                atomic_fk = kpi_data['atomic_kpi_fk']
                self.write_to_db_result(atomic_fk, result, self.LEVEL3, score=kpi_score,
                                        threshold=kpi_target)
            if score is not None:
                kpi_score = score * int(kpi_weight) if isinstance(score, (float, int)) else score[0] * int(kpi_weight)
                set_score += kpi_score
                if kpi_fk < 400:
                    print
                if score2:
                    self.write_to_db_result(kpi_fk, None, self.LEVEL2,  score2=float(score2), score=kpi_score,
                                            threshold=kpi_target)
                else:
                    self.write_to_db_result(kpi_fk, None, self.LEVEL2, score=kpi_score, threshold=kpi_target)

        return set_score

    def calculate_facingSOS(self, kpi_name, kpi_set_fk, tab):
        if tab == FACING_SOS_TAB:
            sos_target_template = self.get_custom_template(tab)
            sos_target_template = sos_target_template.loc[(sos_target_template['KPI Display Name'] == kpi_name)]
            for index, row in sos_target_template.iterrows():
                store = 'store type: {}'.format(self.store_type)
                if store in sos_target_template.columns.tolist() and row[store]:
                    sos_filters = {}
                    general_filters = {}
                    atomic_target = float(row[store])
                    relevant_scenes = self.find_scenes({'template_group': row['Template Group']})
                    if relevant_scenes:
                        general_filters['scene_id'] = relevant_scenes
                        sos_filters['scene_id'] = relevant_scenes

                    numerator_filter = row['Entity Type Numerator'].split(',')

                    if len(numerator_filter) == 1:
                        sos_filters[self.filters[numerator_filter[0]]] = row['Numerator']
                    else:
                        for i in xrange(len(numerator_filter)):
                            sos_filters[self.filters[numerator_filter[i]]] = (row['Numerator'].split(','))[i]

                    denominator_filter = row['Entity Type Denominator'].split(',')
                    if denominator_filter:
                        for i in xrange(len(denominator_filter)):
                            general_filters[self.filters[denominator_filter[i]]] = (row['Denominator'].split(','))[i]

                    score = self.tools.calculate_share_of_shelf(sos_filters=sos_filters, **general_filters)
                    atomic_score = 1 if score >= atomic_target else 0

                    atomic_fk = self.kpi_static_data.loc[(self.kpi_static_data['atomic_kpi_name'] == kpi_name)
                        & (self.kpi_static_data['kpi_set_fk'] == kpi_set_fk)]['atomic_kpi_fk'].iloc[0]
                    self.write_to_db_result(atomic_fk, score, self.LEVEL3, score=atomic_score,
                                            threshold=atomic_target)
                    return atomic_score, score
            return 0, 0

    def calculate_availability(self, kpi_name, kpi_set_fk, tab, kpi_target):
        atomic_scores = pd.DataFrame(columns=['atomic', 'score'])
        if tab == AVAILABILITY_TAB:
            availability_template = self.get_custom_template(tab)
            availability_template = availability_template.loc[(availability_template['KPI Display Name'] == kpi_name)]
            availability_template = availability_template.loc[(availability_template['KPI Display Name'] == kpi_name)]
            num_of_atomics = len(availability_template)
            for index, row in availability_template.iterrows():
                if not row['State'] or (row['State'] and self.state in row['State'].split(',')):
                    atomic_name = row['Atomic KPI Name']
                    filters = self.get_filters(row, ['Product_EAN', 'Brand', 'Size'])
                    score = self.tools.calculate_availability(**filters)
                    atomic_score = 1 if score >= 1 else 0
                    atomic_scores = atomic_scores.append(
                        {'atomic': atomic_name, 'score': atomic_score}, ignore_index=True)
                    atomic_fk = self.kpi_static_data.loc[(self.kpi_static_data['atomic_kpi_name'] == atomic_name)
                         & (self.kpi_static_data['kpi_set_fk'] == kpi_set_fk)]['atomic_kpi_fk'].iloc[0]
                    self.write_to_db_result(atomic_fk, score, self.LEVEL3, score=atomic_score, threshold=1)

            atomics_sum = atomic_scores['score'].sum(axis=0)
            score = (float(atomics_sum) / num_of_atomics) if num_of_atomics else 0
            total_score = 1 if score >= float(kpi_target) else 0
            return total_score, (float(atomics_sum) / num_of_atomics)

        elif tab == ASSORTMENT_TAB:
            assortment_template = self.get_custom_template(tab, 2, 1, 1, ':')
            assortment_template = assortment_template.loc[(assortment_template['KPI Display Name'] == kpi_name)]
            store = 'Store type: {}:{}'.format(self.store_type, self.group)
            if store in assortment_template.columns.tolist():
                assortment_template = assortment_template.loc[assortment_template[store].apply(bool)]
                for index, row in assortment_template.iterrows():
                            atomic_name = row['Atomic KPI Name']
                            atomic_target = float(row[store])
                            filters = {'brand_name': row['Brand Name']}
                            params = row['Parameter'].split(',') if row['Parameter'] else []
                            values = row['Value'].split(',') if row['Value'] else []

                            if len(params) == 1:
                                filters[self.filters[params[0]]] = values
                            else:
                                for i in xrange(len(params)):
                                    filters[self.filters[params[i]]] = values[i]

                            score = self.tools.calculate_availability(**filters)
                            atomic_score = 1 if score >= atomic_target else 0
                            atomic_scores = atomic_scores.append(
                                {'atomic': atomic_name, 'score': atomic_score}, ignore_index=True)
                            atomic_fk = self.kpi_static_data.loc[(self.kpi_static_data['atomic_kpi_name'] == atomic_name)
                                    & (self.kpi_static_data['kpi_set_fk'] == kpi_set_fk)]['atomic_kpi_fk'].iloc[0]
                            self.write_to_db_result(atomic_fk, score, self.LEVEL3, score=atomic_score,
                                                    threshold=atomic_target)

            atomics_sum = atomic_scores['score'].sum(axis=0)
            total_score = 1 if atomics_sum >= kpi_target else 0
            return total_score, atomics_sum

    def calculate_passed_kpis(self, kpi_name, kpi_set_fk, tab, kpi_target):  # assortment, brand block
        atomic_scores = pd.DataFrame(columns=['atomic', 'score'])
        if tab == ASSORTMENT_TAB:
            assortment_template = self.get_custom_template(tab, 2, 1, 1, ':')
            assortment_template = assortment_template.loc[(assortment_template['KPI Display Name'] == kpi_name)]
            store = 'Store type: {}:{}'.format(self.store_type, self.group)
            num_of_atomics = 0
            if store in assortment_template.columns.tolist():
                assortment_template = assortment_template.loc[assortment_template[store].apply(bool)]
                num_of_atomics = len(assortment_template)
                for index, row in assortment_template.iterrows():
                    filters = {'brand_name': row['Brand Name'].split(',')}
                    if row['KPI Type'] == 'Count of Unique SKU':
                        atomic_name = row['Atomic KPI Name']
                        atomic_target = float(row[store])
                        params = row['Parameter'].split(',') if row['Parameter'] else []
                        values = row['Value'].split(',') if row['Value'] else []
                        if len(params) == 1:
                            filters[self.filters[params[0]]] = values
                        else:
                            for i in xrange(len(params)):
                                filters[self.filters[params[i]]] = values[i]

                        score = self.tools.calculate_assortment(**filters)
                        atomic_score = 1 * atomic_target if score >= float(atomic_target) else 0
                        atomic_scores = atomic_scores.append(
                            {'atomic': atomic_name, 'score': atomic_score}, ignore_index=True)
                        atomic_fk = \
                        self.kpi_static_data.loc[(self.kpi_static_data['atomic_kpi_name'] == atomic_name)
                            & (self.kpi_static_data['kpi_set_fk'] == kpi_set_fk)]['atomic_kpi_fk'].iloc[0]
                        self.write_to_db_result(atomic_fk, score, self.LEVEL3, score=atomic_score,
                                                threshold=atomic_target)
            atomics_sum = atomic_scores['score'].sum(axis=0)
            score = (float(atomics_sum) / num_of_atomics) if num_of_atomics else 0
            total_score = 1 if score >= kpi_target else 0
            return total_score, (float(atomics_sum) / num_of_atomics)

        elif tab == BRAND_BLOCK_TAB:
            brand_block_template = self.get_custom_template(tab)
            brand_block_template = brand_block_template.loc[(brand_block_template['KPI Display Name'] == kpi_name)]
            for index, row in brand_block_template.iterrows():
                filters = {}
                if row['Template Group']:
                    filters['template_group'] = row['Template Group']
                atomic_name = row['Atomic KPI Name']
                filters[self.filters[row['Block Entity']]] = row[self.group]
                ratio = float(row['% of blocking'])
                is_blocked = self.tools.calculate_block_together(
                    allowed_products_filters=None, include_empty=True,
                    minimum_block_ratio=ratio, result_by_scene=False, **filters)
                if isinstance(is_blocked, tuple):
                    is_blocked = is_blocked[0]
                if not is_blocked:
                    is_blocked = 0
                atomic_score = 1 if is_blocked >= ratio else 0
                atomic_scores = atomic_scores.append(
                    {'atomic': atomic_name, 'score': atomic_score}, ignore_index=True)
                atomic_fk = self.kpi_static_data.loc[(self.kpi_static_data['atomic_kpi_name'] == atomic_name)
                    & (self.kpi_static_data['kpi_set_fk'] == kpi_set_fk)]['atomic_kpi_fk'].iloc[0]
                self.write_to_db_result(atomic_fk, is_blocked, self.LEVEL3, score=atomic_score, threshold=ratio)

            atomics_sum = atomic_scores['score'].sum(axis=0)
            total_score = 1 if atomics_sum >= kpi_target else 0
            return total_score, atomics_sum

    def calculate_shelf_sequence(self, kpi_name, kpi_set_fk):
        atomic_scores = pd.DataFrame(columns=['atomic', 'score'])
        shelf_sequence_template = self.get_custom_template(SHELF_SEQUENCE_TAB)
        kpi_data = shelf_sequence_template.loc[(shelf_sequence_template['KPI Display Name'] == kpi_name)]
        total_score = 0
        atomics_sum = 0
        for index, row in kpi_data.iterrows():
            atomic_name = row['Atomic KPI Name']
            atomic_fk = self.kpi_static_data.loc[(self.kpi_static_data['atomic_kpi_name'] == atomic_name)
                                & (self.kpi_static_data['kpi_set_fk'] == kpi_set_fk)]['atomic_kpi_fk'].iloc[0]
            groups_filter = {}
            min_ratio = float(row['% of Block']) if row['% of Block'] else 1
            entities = row[self.group].split(',')
            for entity in entities:
                groups_filter[entity] = {self.filters[row['Parameter']]: entity}

            relevant_scenes = self.find_scenes({'template_group': row['Template Group']})
            if relevant_scenes:
                atomic_score = 0
                for entity in groups_filter:
                    relevant_scenes = set(relevant_scenes).intersection(
                        set(self.scif[self.tools.get_filter_condition(self.scif,
                                                            **groups_filter[entity])]['scene_id'].unique().tolist()))
                block_result = {}

                relevant_scenes = list(relevant_scenes)
                if not relevant_scenes:
                    atomic_score = 0
                else:
                    # Make sure each group is blocked together by itself
                    for entity in groups_filter:
                        result = self.tools.calculate_block_together(scene_fk=relevant_scenes,
                                                                     minimum_block_ratio=min_ratio, **groups_filter[entity])
                        block_result[entity] = 1 if result and result >= min_ratio else 0

                    if 0 not in block_result.values():
                        blocks = block_result.keys()
                        filters = {self.filters[row['Parameter']]: blocks}

                        # Checks if all groups are blocked together
                        result = self.tools.calculate_block_together(scene_fk=relevant_scenes,
                                                                     minimum_block_ratio=min_ratio, **filters)
                        if result:
                            filters = groups_filter.values()
                            # checks if all group order by sequence
                            left = self.tools.calculate_product_sequence(sequence_filters=filters, direction='left',
                                                                         min_required_to_pass=1)
                            right = self.tools.calculate_product_sequence(sequence_filters=filters, direction='right',
                                                                          min_required_to_pass=1)

                            atomic_score = 1 if (left or right) else 0
                    else:
                        atomic_score = 0

                self.write_to_db_result(atomic_fk, atomic_score, self.LEVEL3, score=atomic_score, threshold=min_ratio)
                atomic_scores = atomic_scores.append(
                    {'atomic': atomic_name, 'score': atomic_score}, ignore_index=True)
                atomics_sum = atomic_scores['score'].sum(axis=0)
                total_score = max(atomic_scores['score'])
            else:
                total_score = 0
        return total_score, atomics_sum

    def extra_points(self, kpi_name, kpi_set_fk):
        extra_point_template = self.get_custom_template(EXTRA_POINT_TAB)
        extra_point_template = extra_point_template.loc[(extra_point_template['KPI Display Name'] == kpi_name)]

        for index, row in extra_point_template.iterrows():
            relevant_store_type = row['Store Type'].split(',') if row['Store Type'] else []
            relevant_groups = row['Group'].split(',') if row['Group'] else []
            if (not relevant_store_type or self.store_type in relevant_store_type) and \
                    (not relevant_groups or self.group in relevant_groups):
                ep_atomic_name = row['Atomic Kpi Name']
                ep_atomic_target = row['Target']
                result = 0
                atomic_score = 0
                if row['KPI Type'] == 'Share of Scenes':
                    relevant_atomics = extra_point_template.loc[
                        (extra_point_template['KPI Type'] == 'Count of Scenes')]
                    share_of_scene = []
                    for atomic_index, atomic_row in relevant_atomics.iterrows():
                        share_of_scene.append(self.calculate_atomic_count_of_scene(atomic_row, kpi_set_fk))
                    share = float(share_of_scene[0]) / float(share_of_scene[1] + share_of_scene[0])\
                                if float(share_of_scene[1] + share_of_scene[0]) else 0
                    if ep_atomic_target:
                        atomic_score = 1 if share >= float(ep_atomic_target) else 0
                    else:
                        atomic_score = share

                    if row['Save to DB'] == 'Y':
                        atomic_fk = self.kpi_static_data.loc[(self.kpi_static_data['kpi_set_fk'] == kpi_set_fk)
                                                         & (self.kpi_static_data['atomic_kpi_name'] == ep_atomic_name)][
                            'atomic_kpi_fk'].iloc[0]
                        self.write_to_db_result(atomic_fk, share, self.LEVEL3, score=atomic_score,
                                                threshold=ep_atomic_target)

                elif row['KPI Type'] == 'Count of passed KPI':
                    relevant_atomics = extra_point_template.loc[
                        (extra_point_template['KPI Type'] == 'Count of Scenes')]
                    counter = 0
                    for atomic_index, atomic_row in relevant_atomics.iterrows():
                        counter += self.calculate_atomic_count_of_scene(atomic_row, kpi_set_fk)
                    if ep_atomic_target:
                        atomic_score = 1 if counter >= float(ep_atomic_target) else 0
                    else:
                        atomic_score = counter
                    result = counter

                elif row['KPI Type'] == 'Count of Scenes':
                    count = self.calculate_atomic_count_of_scene(row, kpi_set_fk)
                    if ep_atomic_target:
                        atomic_score = 1 if count >= float(ep_atomic_target) else 0
                    else:
                        atomic_score = count
                    result = count

                elif row['KPI Type'] == 'Count of Facing':
                    filters = self.get_filters(row, ['Template Group', 'Template Name','Manufacturer',
                                                            'Category', 'Sub Category', 'Brand',
                                                            'Sub Brand', 'SKU', 'Att1'])
                    facing_num = self.tools.calculate_availability(**filters)
                    if ep_atomic_target:
                        atomic_score = 1 if facing_num >= float(ep_atomic_target) else 0
                    else:
                        atomic_score = facing_num
                    result = facing_num

                elif row['KPI Type'] == 'Facing SOS':
                    ep_atomic_name = row['Atomic Kpi Name']
                    ep_atomic_target = row['Target']
                    relevant_atomics = extra_point_template.loc[
                        (extra_point_template['KPI Type'] == 'Count of Facing') &
                        (extra_point_template['Atomic Kpi Name'] == ep_atomic_name)]
                    sos_filters = []
                    for atomic_index, atomic_row in relevant_atomics.iterrows():
                        sos_filters.append(self.get_filters(atomic_row, ['Template Group', 'Template Name',
                                                                         'Manufacturer', 'Category', 'Sub Category',
                                                                         'Brand', 'Sub Brand', 'SKU', 'Att1']))
                    share = self.tools.calculate_share_of_shelf(sos_filters=sos_filters[0], **sos_filters[1])
                    if ep_atomic_target:
                        atomic_score = 1 if share >= float(ep_atomic_target) else 0
                    else:
                        atomic_score = share

                return atomic_score

    def calculate_atomic_count_of_scene(self, atomic_row, kpi_set_fk):
            atomic_name = atomic_row['Atomic Kpi Name']
            atomic_target = float(atomic_row['Target']) if atomic_row['Target'] else None
            scene_to_count = []
            if atomic_row['Minimum Facing']:
                sku_target = float(atomic_row['Minimum Facing'])
            else:
                sku_target = 0
            manufacutrer_target = float(atomic_row["Minimum Threshold"])
            min_share = atomic_row['Minimum Share of each scene']
            share_target = float(min_share) if min_share else 0

            filters = self.get_filters(atomic_row, ['Template Group', 'Template Name'])
            scenes = self.find_scenes(filters)
            atomic_score = 0
            if scenes:
                for scene in scenes:
                    sku_filter = {'scene_id': scene}
                    manufacutrer_filter = self.get_filters(atomic_row, ['Manufacturer'])
                    manufacutrer_filter.update(sku_filter)
                    other_filters = self.get_filters(atomic_row, ['Category', 'Sub Category', 'Brand',
                                                                  'Sub Brand', 'SKU', 'Att1'])
                    sku_number = self.tools.calculate_availability(**sku_filter)
                    manufacturer_number = self.tools.calculate_availability(**manufacutrer_filter)
                    share_number = self.tools.calculate_availability(**other_filters)

                    if sku_number >= sku_target and float(manufacturer_number)/sku_number >= manufacutrer_target:
                        if share_target:
                            if float(share_number)/sku_number >= share_target:
                                scene_to_count.append(scene)
                        else:
                            scene_to_count.append(scene)
                if atomic_target:
                    atomic_score = 1 if len(scene_to_count) >= atomic_target else 0
                else:
                    atomic_score = len(scene_to_count)

                if 'Save to DB' in atomic_row.keys().tolist():
                    if atomic_row['Save to DB'] == 'Y':
                        atomic_fk = self.kpi_static_data.loc[(self.kpi_static_data['kpi_set_fk'] == kpi_set_fk)
                             & (self.kpi_static_data['atomic_kpi_name'] == atomic_name)]['atomic_kpi_fk'].iloc[0]
                        self.write_to_db_result(atomic_fk, len(scene_to_count), self.LEVEL3, score=atomic_score,
                                                threshold=atomic_target)

                else:
                    atomic_fk = self.kpi_static_data.loc[(self.kpi_static_data['atomic_kpi_name'] == atomic_name)
                                         & (self.kpi_static_data['kpi_set_fk'] == kpi_set_fk)]['atomic_kpi_fk'].iloc[0]
                    self.write_to_db_result(atomic_fk, len(scene_to_count), self.LEVEL3, score=atomic_score,
                                            threshold=atomic_target)
            return atomic_score

    def find_scenes(self, params):
        """
        :param params: a dictionary of filters
        :return: a list of scenes with relevant parameters.
        """
        relevant_scenes = self.tools.get_filter_condition(self.scif, **params)
        relevant_scenes = self.scif[relevant_scenes]
        return relevant_scenes['scene_id'].unique().tolist()

    def calculate_game_plan(self, kpi_name, kpi_set_fk, kpi_set_name):
        game_plan_template = self.get_custom_template(GAME_PLAN_TAB)
        game_plan_template = game_plan_template.loc[(game_plan_template['KPI Display Name'] == kpi_name) &
                                                    (game_plan_template['KPI Group'] == kpi_set_name)]
        game_plan_score = 0
        for index, row in game_plan_template.iterrows():
            relevant_store_type = [x.strip() for x in row['Store Type'].split(',')] if row['Store Type'] else []
            relevant_groups = [x.strip() for x in row['Store Type'].split(',')] if row['Group'] else []
            atomic_name = row['Atomic Kpi Name']
            target = float(row['Target']) if row['Target'] else 1

            if (not self.store_type or self.store_type in relevant_store_type) and \
                    (not relevant_groups or self.group in relevant_groups):
                count = 0
                if row['KPI Type'] == 'Count of Scenes':
                    count = self.calculate_atomic_count_of_scene(row, kpi_set_fk)
                    atomic_score = 5 if count >= target else 0
                elif row['KPI Type'] == 'Availability':
                    filters = self.get_filters(row, ['Category', 'Sub Category', 'Brand', 'Sub Brand', 'SKU', 'Att1'])
                    count = self.tools.calculate_availability(**filters)
                    atomic_score = 5 if count >= int(target) else 0
                    atomic_fk = self.kpi_static_data.loc[(self.kpi_static_data['atomic_kpi_name'] == atomic_name)
                        & (self.kpi_static_data['kpi_set_fk'] == kpi_set_fk)]['atomic_kpi_fk'].iloc[0]
                    self.write_to_db_result(atomic_fk, count, self.LEVEL3, score=atomic_score, threshold=target)
                else:
                    atomic_score = 0
                game_plan_score += atomic_score
        return game_plan_score

    def get_filters(self, row, relevant_filters):
        filters = {}
        for current_filter in relevant_filters:
            filter_value = row[current_filter]
            if filter_value and filter_value != "''":
                if len(filter_value.split(',')) > 1:
                    filter_value = filter_value.split(',')
                else:
                    if filter_value.startswith('Not '):
                        filter_value = (filter_value[4:], self.tools.EXCLUDE_FILTER)
                current_filter = self.filters[current_filter] if current_filter in self.filters else current_filter
                filters[current_filter] = filter_value
        return filters

    def get_group(self):
        group_template = self.get_custom_template('Group')
        city = self.store_data['address_city'].values[0]
        group = group_template.loc[group_template['City'] == city, 'Group'].iloc[0]
        return group

    def get_state(self):
        query = INBEVBR_SANDQueries.get_state(self.store_id)
        match_state = pd.read_sql_query(query, self.rds_conn.db)
        return match_state['state'].values[0]

    def get_match_display(self):
        """
        This function extracts the display matches data and saves it into one global data frame.
        The data is taken from probedata.match_display_in_scene.
        """
        query = INBEVBR_SANDQueries.get_match_display(self.session_uid)
        match_display = pd.read_sql_query(query, self.rds_conn.db)
        return match_display

    def write_to_db_result(self, fk, result, level, score2=None, score=None, threshold=None):
        """
        This function the result data frame of every KPI (atomic KPI/KPI/KPI set),
        and appends the insert SQL query into the queries' list, later to be written to the DB.
        """
        attributes = self.create_attributes_dict(fk, result, level, score2=score2, score=score, threshold=threshold)
        if level == self.LEVEL1:
            table = KPS_RESULT
        elif level == self.LEVEL2:
            table = KPK_RESULT
        elif level == self.LEVEL3:
            table = KPI_RESULT
        else:
            return
        query = insert(attributes, table)
        self.kpi_results_queries.append(query)

    def create_attributes_dict(self, fk, result, level, score2=None, score=None, threshold=None):
        """
        This function creates a data frame with all attributes needed for saving in KPI results tables.

        """
        float(format(result, '.2f')) if isinstance(result, float) else result
        # result = round(result, 2)
        if level == self.LEVEL1:
            kpi_set_name = self.kpi_static_data[self.kpi_static_data['kpi_set_fk'] == fk]['kpi_set_name'].values[0]
            score_type = ''
            # attributes = pd.DataFrame([(kpi_set_name, self.session_uid, self.store_id, self.visit_date.isoformat(),
            #                             format(result, '.2f'), score_type, fk)],
            #                           columns=['kps_name', 'session_uid', 'store_fk', 'visit_date', 'score_1',
            #                                    'score_2', 'kpi_set_fk'])
            attributes = pd.DataFrame([(kpi_set_name, self.session_uid, self.store_id, self.visit_date.isoformat(),
                                        result, score_type, fk)],
                                      columns=['kps_name', 'session_uid', 'store_fk', 'visit_date', 'score_1',
                                               'score_2', 'kpi_set_fk'])

        elif level == self.LEVEL2:
            kpi_name = self.kpi_static_data[self.kpi_static_data['kpi_fk'] == fk]['kpi_name'].values[0].replace("'",
                                                                                                                "\\'")
            attributes = pd.DataFrame([(self.session_uid, self.store_id, self.visit_date.isoformat(),
                                        fk, kpi_name, score, score2)],
                                      columns=['session_uid', 'store_fk', 'visit_date', 'kpi_fk', 'kpk_name', 'score',
                                               'score_2'])
        elif level == self.LEVEL3:

            data = self.kpi_static_data[self.kpi_static_data['atomic_kpi_fk'] == fk]
            atomic_kpi_name = data['atomic_kpi_name'].values[0].replace("'", "\\'")
            kpi_fk = data['kpi_fk'].values[0]
            kpi_set_name = self.kpi_static_data[self.kpi_static_data['atomic_kpi_fk'] == fk]['kpi_set_name'].values[0]
            if not score and not threshold:
                attributes = pd.DataFrame([(atomic_kpi_name, self.session_uid, kpi_set_name, self.store_id,
                                            self.visit_date.isoformat(), datetime.utcnow().isoformat(),
                                            result, kpi_fk, fk, None, 0)],
                                          columns=['display_text', 'session_uid', 'kps_name', 'store_fk', 'visit_date',
                                                   'calculation_time', 'result', 'kpi_fk', 'atomic_kpi_fk', 'threshold',
                                                   'score'])
            else:
                attributes = pd.DataFrame([(atomic_kpi_name, self.session_uid, kpi_set_name, self.store_id,
                                            self.visit_date.isoformat(), datetime.utcnow().isoformat(),
                                            result, kpi_fk, fk, threshold, score)],
                                          columns=['display_text', 'session_uid', 'kps_name', 'store_fk',
                                                   'visit_date',
                                                   'calculation_time', 'result', 'kpi_fk', 'atomic_kpi_fk',
                                                   'threshold',
                                                   'score'])
        else:
            attributes = pd.DataFrame()
        return attributes.to_dict()

    def save_level2_and_level3(self, set_name, kpi_name, result, score=0, threshold=None, level_2_only=False,
                                   level_3_only=False, level2_name_for_atomic=None):
        """
        Given KPI data and a score, this functions writes the score for both KPI level 2 and 3 in the DB.
        """
        # kpi_data = self.kpi_static_data[(self.kpi_static_data['kpi_set_name'] == set_name) &
        #                                 (self.kpi_static_data['kpi_name'] == kpi_name)]
        #
        # kpi_fk = kpi_data['kpi_fk'].values[0]
        # atomic_kpi_fk = kpi_data['atomic_kpi_fk'].values[0]
        # self.write_to_db_result(kpi_fk, result, self.LEVEL2)
        # if not result and not threshold:
        #     self.write_to_db_result(atomic_kpi_fk, result, self.LEVEL3)
        # else:
        #     self.write_to_db_result(atomic_kpi_fk, result, self.LEVEL3, score=score, threshold=threshold)

        if level_2_only:
            kpi_data = self.kpi_static_data[(self.kpi_static_data['kpi_set_name'] == set_name) &
                                            (self.kpi_static_data['kpi_name'] == kpi_name)]
            kpi_fk = kpi_data['kpi_fk'].values[0]
            self.write_to_db_result(kpi_fk, result, self.LEVEL2)
        if level_3_only:
            kpi_data = self.kpi_static_data[(self.kpi_static_data['kpi_set_name'] == set_name) &
                                            (self.kpi_static_data['kpi_name'] == level2_name_for_atomic) & (
                                                self.kpi_static_data['atomic_kpi_name'] == kpi_name)]
            # kpi_fk = kpi_data['kpi_fk'].values[0]
            atomic_kpi_fk = kpi_data['atomic_kpi_fk'].values[0]
            self.write_to_db_result(atomic_kpi_fk, result, self.LEVEL3)
        else:
            kpi_data = self.kpi_static_data[(self.kpi_static_data['kpi_set_name'] == set_name) &
                                            (self.kpi_static_data['kpi_name'] == kpi_name)]
            kpi_fk = kpi_data['kpi_fk'].values[0]
            atomic_kpi_fk = kpi_data['atomic_kpi_fk'].values[0]
            self.write_to_db_result(kpi_fk, result, self.LEVEL2)
            if not result and not threshold:
                self.write_to_db_result(atomic_kpi_fk, result, self.LEVEL3)
            else:
                self.write_to_db_result(atomic_kpi_fk, result, self.LEVEL3, score=score, threshold=threshold)

    @log_runtime('Saving to DB')
    def commit_results_data(self):
        """
        This function writes all KPI results to the DB, and commits the changes.
        """
        cur = self.rds_conn.db.cursor()
        delete_queries = INBEVBR_SANDQueries.get_delete_session_results_query(self.session_uid)
        for query in delete_queries:
            cur.execute(query)
        for query in self.kpi_results_queries:
            cur.execute(query)
        self.rds_conn.db.commit()
