import pandas as pd
from datetime import datetime, timedelta
from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Cloud.Services.Connector.Keys import DbUsers
from KPIUtils_v2.DB.PsProjectConnector import PSProjectConnector
from Trax.Utils.Logging.Logger import Log
from Trax.Data.Utils.MySQLservices import get_table_insertion_query as insert
from Trax.Algo.Calculations.Core.Shortcuts import BaseCalculationsGroup
from Projects.CCUS_SAND2.Utils.Fetcher import CCUSQueries
from Projects.CCUS_SAND2.Utils.ToolBox import ToolBox

__author__ = 'ortal'
MAX_PARAMS = 4
COUNT_OF_BUNDLE_UNIQUE_SKU = 'Count of Bundle Unique SKU'
COUNT_OF_DISPLAY_ENTITY = 'Count of Display Entity'
COUNT_OF_DISPLAY = 'Count of Display'
FACING_COUNT_ON_SHELF_LEVEL = 'Facing Count on Shelf Level'
AVAILABILITY_POSITION = 'Availability Position'
SURVEY_QUESTION = 'Survey Question'
COUNT_OF_SKUS = 'Count of SKUs'
SOS = 'SOS'
SCENE_TYPE_COUNT = 'Scene Type Count'
KPI_RESULT = 'report.kpi_results'
KPK_RESULT = 'report.kpk_results'
KPS_RESULT = 'report.kps_results'
KPI_TYPE = 'KPI_Type'
AVAILABILITY = 'Availability'
CUSTOM_ANCHOR_KPI = 'Custom Anchor KPI'


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


class CCUSToolBox:
    LEVEL1 = 1
    LEVEL2 = 2
    LEVEL3 = 3

    def __init__(self, data_provider, output):
        self.k_engine = BaseCalculationsGroup(data_provider, output)
        self.data_provider = data_provider
        self.project_name = self.data_provider.project_name
        self.session_uid = self.data_provider.session_uid
        self.products = self.data_provider[Data.PRODUCTS]
        self.all_products = self.data_provider[Data.ALL_PRODUCTS]
        self.match_product_in_scene = self.data_provider[Data.MATCHES]
        self.visit_date = self.data_provider[Data.VISIT_DATE]
        self.session_info = self.data_provider[Data.SESSION_INFO]
        self.rds_conn = PSProjectConnector(self.project_name, DbUsers.CalcAdmin)
        self.store_info = self.data_provider[Data.STORE_INFO]
        self.store_type = self.store_info['store_type'].values[0]
        self.scene_info = self.data_provider[Data.SCENES_INFO]
        self.store_id = self.data_provider[Data.STORE_FK]
        self.scif = self.data_provider[Data.SCENE_ITEM_FACTS]
        self.scif = self.scif.merge(self.data_provider[Data.STORE_INFO], how='left', left_on='store_id',
                                    right_on='store_fk')
        self.match_display_in_scene = self.get_match_display()
        self.scif = self.scif.merge(self.match_display_in_scene, how='left', left_on='scene_id',
                                    right_on='scene_fk')
        self.scif = self.scif.replace(['Display\r\n'],['Display'])
        # self.get_atts()
        self.set_templates_data = {}
        self.kpi_static_data = self.get_kpi_static_data()
        self.tools = ToolBox(self.data_provider, output,
                                      kpi_static_data=self.kpi_static_data,
                                      match_display_in_scene=self.match_display_in_scene)
        self.download_time = timedelta(0)
        self.kpi_results_queries = []
        self.session_fk = self.data_provider[Data.SESSION_INFO]['pk'].iloc[0]
        self.survey_response = self.data_provider[Data.SURVEY_RESPONSES]
        # self.get_product_att4()

    # def get_product_att4(self):
    #     """
    #     This function extracts the static KPI data and saves it into one global data frame.
    #     The data is taken from static.kpi / static.atomic_kpi / static.kpi_set.
    #     """
    #     self.rds_conn = PSProjectConnector(self.project_name, DbUsers.CalculationEng)
    #     cur = self.rds_conn.db.cursor()
    #     query = DUNKINQueries.get_product_att4()
    #     product_att4 = pd.read_sql_query(query, self.rds_conn.db)
    #     self.scif = self.scif.merge(product_att4, how='left', on='product_ean_code')


    def get_kpi_static_data(self):
        """
        This function extracts the static KPI data and saves it into one global data frame.
        The data is taken from static.kpi / static.atomic_kpi / static.kpi_set.
        """
        query = CCUSQueries.get_all_kpi_data()
        kpi_static_data = pd.read_sql_query(query, self.rds_conn.db)
        return kpi_static_data

    # def get_atts(self):
    #     """
    #     This function extracts the static KPI data and saves it into one global data frame.
    #     The data is taken from static.kpi / static.atomic_kpi / static.kpi_set.
    #     """
    #     query = CCUSQueries.get_product_atts()
    #     product_att4 = pd.read_sql_query(query, self.rds_conn.db)
    #     self.scif = self.scif.merge(product_att4, how='left', left_on='product_ean_code',
    #                                 right_on='product_ean_code')

    def get_match_display(self):
        """
        This function extracts the display matches data and saves it into one global data frame.
        The data is taken from probedata.match_display_in_scene.
        """
        query = CCUSQueries.get_match_display(self.session_uid)
        match_display = pd.read_sql_query(query, self.rds_conn.db)
        return match_display

    def main_calculation(self, set_name):
        """
        This function calculates the KPI results.
        """
        if set_name == 'Dunkin Donuts':
            if set_name not in self.set_templates_data.keys():
                calc_start_time = datetime.utcnow()
                self.set_templates_data[set_name] = self.tools.download_template(set_name)
                self.download_time += datetime.utcnow() - calc_start_time
            for param in self.set_templates_data[set_name]:
                if param.get('KPI_Type') == SOS:
                    self.calculate_share_of_shelf(param,set_name)
                    continue
                if param.get('KPI_Type') == COUNT_OF_SKUS or param.get('KPI_Type') == AVAILABILITY:
                    self.calculate_assortment_or_availability(param, set_name)
                    continue
                if param.get('KPI_Type') == COUNT_OF_DISPLAY_ENTITY:
                    self.calculate_count_of_display_entity(param,set_name)
                    continue
                if param.get('KPI_Type') == COUNT_OF_DISPLAY:
                    self.calculate_count_of_display(param,set_name)
                    continue
                if param.get('KPI_Type') == FACING_COUNT_ON_SHELF_LEVEL:
                    self.calculate_facing_count_on_shelf_level(param, set_name)
                    continue
                if param.get('KPI_Type') == AVAILABILITY_POSITION:
                    self.calculate_availability_position(param, set_name)
                    continue
                if param.get('KPI_Type') == SURVEY_QUESTION:
                    self.calculate_survey(param, set_name)
                    continue
        return

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

    def save_level1(self, set_name, score):
        kpi_data = self.kpi_static_data[self.kpi_static_data['kpi_set_name'] == set_name]
        kpi_set_fk = kpi_data['kpi_set_fk'].values[0]
        self.write_to_db_result(kpi_set_fk, score, self.LEVEL1)

    def calculate_survey(self, param, set_name):
        target = param.get('Target')
        entity = 'question_text'
        value = param.get('Value1')
        value = [value] if not isinstance(value, list) else value
        if target is not None:
            answer = self.tools.check_survey_answer(value, target)
            if answer:
                res = 1
            else:
                res = 0
        else:
            survey_data = self.survey_response[self.survey_response[entity].isin(value)]
            if survey_data.empty:
                Log.warning('Survey with {} = {} doesn\'t exist'.format(entity, value))
                return
            else:
                res = survey_data['selected_option_text'].values[0]
        self.save_level2_and_level3(set_name=set_name, kpi_name=param.get('KPI Name'), result=res,
                                    score=res, threshold=0)

    def calculate_count_bay_anchor(self, param, set_name):
        general_filters = {}
        score = 0
        store_type = param.get('Store_Type')
        if store_type is not None:
            general_filters['store_type'] = [str(g) for g in param.get('Store_Type').split(",")]
        template_group = param.get('Template_Group')
        if template_group is not None:
            general_filters['template_group'] = [str(g) for g in param.get('Template_Group').split(",")]
        general_filters[param.get('Param1')] = [str(g) for g in param.get('Value1').split(",")]
        position = param.get('Value2')
        target = param.get('target')
        res = self.tools.calculate_products_on_bay_edge(min_number_of_facings=target, min_number_of_shelves=1,
                                                        position=position, **general_filters)
        if res[0] >= 1:
            score = 1
        self.save_level2_and_level3(set_name=set_name, kpi_name=param.get('KPI Name'), result=res[0],
                                    score=score, threshold=0)
        return

    def calculate_share_of_shelf(self, param,set_name):
        sos_filter = {}
        general_filters = {}
        template_group = param.get('Template_Group')
        if template_group is not None:
            general_filters['template_group'] = [str(g) for g in param.get('Template_Group').split(",")]
            sos_filter['template_group'] = [str(g) for g in param.get('Template_Group').split(",")]
        for i in xrange(1, MAX_PARAMS + 1):
            if param.get("Param{}".format(i)):
                sos_filter[param.get("Param{}".format(i))] = [str(g) for g in param.get("Value{}".format(i)).split(",")]
        res = self.tools.calculate_share_of_shelf(sos_filters =sos_filter, **general_filters)
        res *= 100
        res = float("{0:.2f}".format(res))
        self.save_level2_and_level3(set_name=set_name, kpi_name=param.get('KPI Name'), result=res,
                                    score=res, threshold=0)
        return

    def calculate_count_of_bundle_unique_sku(self, param, set_name):
        filters = {}
        group_count = 0
        all = True
        store_type = param.get('Store_Type')
        if store_type is not None:
            filters['store_type'] = [str(g) for g in param.get('Store_Type').split(",")]
        template_group = param.get('Template_Group')
        if template_group is not None:
            filters['template_group'] = [str(g) for g in param.get('Template_Group').split(",")]
        group = [str(g) for g in param.get("Value1").split("),")]
        i=2
        for g in group:
            g = g.replace('(', '')
            g = g.replace(')', '')
            g1 = [int(p) for p in g.split(",")]
            filters['product_ean_code'] = g1
            res = self.tools.general_tools.calculate_assortment(**filters)
            target = param.get("Value{}".format(i))
            i += 1
            if res >= target:
                group_count += res
                all = True
            else:
                all = False
            filters.pop('product_ean_code')
        if all:
            scores = 1
        else:
            scores = 0
        self.save_level2_and_level3(set_name=set_name, kpi_name=param.get('KPI Name'), result=group_count,
                                    score=scores, threshold=0)

    def calculate_assortment_or_availability(self, param, set_name):
        general_filters = {}
        res = 0
        store_type = param.get('Store_Type')
        if store_type is not None:
            general_filters['store_type'] = [str(g) for g in param.get('Store_Type').split(",")]
        template_group = param.get('Template_Group')
        if template_group is not None:
            general_filters['template_group'] = [str(g) for g in param.get('Template_Group').split(",")]
        for i in xrange(1, MAX_PARAMS + 1):
            if param.get("Param{}".format(i)):
                general_filters[param.get("Param{}".format(i))] = \
                    [str(g) for g in param.get("Value{}".format(i)).split(",")]
        if param.get(KPI_TYPE) == AVAILABILITY:
            res = self.tools.calculate_availability(**general_filters)
        elif param.get(KPI_TYPE) == COUNT_OF_SKUS:
            res = self.tools.general_tools.calculate_assortment(**general_filters)
        target = param.get('Target')
        if target is not None:
            if target <= res:
                score = 1
            else:
                score = 0
            self.save_level2_and_level3(set_name=set_name, kpi_name=param.get('KPI Name'), result=res,
                                        score=score, threshold=0)
        else:
            self.save_level2_and_level3(set_name=set_name, kpi_name=param.get('KPI Name'), result=res,
                                        score=res, threshold=0)

    def calculate_count_of_scene(self, param, set_name):
        scif_scenes = self.scif.loc[self.scif['template_group'] == param.get('Template_Group')]
        if not scif_scenes.empty:
            scif_scenes = scif_scenes.loc[scif_scenes['store_type'] == param.get('Store_Type')]
            valuse = {}
            for i in xrange(1, MAX_PARAMS + 1):
                if param.get("Param{}".format(i)):
                    valuse[param.get("Param{}".format(i))] = [str(g) for g in param.get("Value{}".format(i)).split(",")]
            for value in valuse:
                scif_scenes = scif_scenes.loc[scif_scenes[value].isin(tuple(valuse.get(value)))]
            scenes = len(scif_scenes['scene_id'].unique().tolist())
            self.save_level2_and_level3(set_name=set_name, kpi_name=param.get('KPI Name'), result=scenes,
                                        score=scenes, threshold=0)

    def calculate_count_of_display_entity(self, param, set_name):
        scif_scenes = self.scif.loc[self.scif['template_group'] == param.get('Template_Group')]
        if not scif_scenes.empty:
            scif_scenes.dropna(subset=[param.get('Param1')], how='all', inplace=True)
            if not scif_scenes.empty:
                scif_scenes = scif_scenes.loc[scif_scenes[param.get('Param1')] == param.get('Value1')]
        else:
            self.save_level2_and_level3(set_name=set_name, kpi_name=param.get('KPI Name'), result=0,
                                        score=0, threshold=0)
            return
        scenes = scif_scenes['scene_id'].unique().tolist()
        count_displays = 0
        score = 0
        if scenes is not None:
            for scene in scenes:
                matchs1 = self.match_product_in_scene.loc[self.match_product_in_scene['scene_fk'] == scene]
                matchs = matchs1.merge(self.match_display_in_scene, how='left', on=['scene_fk', 'bay_number'])
                bays = matchs['bay_number'].unique().tolist()
                for bay in bays:
                    bay_data = matchs.loc[matchs['bay_number'] == bay]
                    if type(bay_data['display_type'].values) == str:
                        display_type = bay_data['display_type'].values
                    else:
                        display_type = bay_data['display_type'].values[0]
                    if display_type == param.get('Value1'):
                        filters = {}
                        filters['bay_number'] = bay
                        filters['scene_fk'] = scene
                        denominator = self.tools.calculate_availability(**filters)
                        filters[param.get('Param2')] = param.get('Value2')
                        nominator = self.tools.calculate_availability(**filters)
                        if denominator != 0:
                            res = str(float(nominator) / denominator)
                            res = float(res)
                            res = round(res, 3)
                            res *= 100
                        else:
                            continue
                        if res >= param.get('share_of_display'):
                            count_displays = +1
        target = param.get('target')
        if target is not None:
            if target <= count_displays:
                score = 1
            self.save_level2_and_level3(set_name=set_name, kpi_name=param.get('KPI Name'), result=count_displays,
                                        score=score, threshold=target)
        else:
            self.save_level2_and_level3(set_name=set_name, kpi_name=param.get('KPI Name'), result=count_displays,
                                        score=count_displays, threshold=0)


    def calculate_count_of_display(self, param, set_name):
        scif_scenes = self.scif.loc[self.scif['template_group'] == param.get('Template_Group')]
        if not scif_scenes.empty:
            scif_scenes.dropna(subset=[param.get('Param1')], how='all', inplace=True)
            if not scif_scenes.empty:
                scif_scenes = scif_scenes.loc[scif_scenes[param.get('Param1')] == param.get('Value1')]
        else:
            self.save_level2_and_level3(set_name=set_name, kpi_name=param.get('KPI Name'), result=0,
                                        score=0, threshold=0)
            return
        scenes = scif_scenes['scene_id'].unique().tolist()
        count_displays = 0
        if scenes is not None:
            for scene in scenes:
                matchs1 = self.match_product_in_scene.loc[self.match_product_in_scene['scene_fk'] == scene]
                matchs = matchs1.merge(self.match_display_in_scene, how='left', on=['scene_fk', 'bay_number'])
                bays = matchs['bay_number'].unique().tolist()
                for bay in bays:
                    bay_data = matchs.loc[matchs['bay_number'] == bay]
                    if type(bay_data['display_type'].values) == str:
                        display_type = bay_data['display_type'].values
                    else:
                        display_type = bay_data['display_type'].values[0]
                    if display_type == param.get('Value1'):
                        list = [str(g) for g in param.get("Value2").split(",")]
                        for display in list:
                            res = bay_data.loc[bay_data[param.get('Param2')] == display]
                            if not res.empty:
                                count_displays = +1
        self.save_level2_and_level3(set_name=set_name, kpi_name=param.get('KPI Name'), result=count_displays,
                                    score=count_displays, threshold=0)

    def calculate_facing_count_on_shelf_level(self, param, set_name):
        filters = {}
        scif_scenes = self.scif.loc[self.scif['template_group'] == param.get('Template_Group')]
        scenes = scif_scenes['scene_id'].unique().tolist()
        filters[param.get('Param1')] = [str(g) for g in param.get('Value1').split(",")]
        filters[param.get('Param2')] = int(param.get('Value2'))
        filters['scene_fk'] = scenes
        res_nom = self.tools.calculate_availability(**filters)
        if param.get('Atomic_KPI_Type') == SOS:
            filters2 = {param.get('Param2'): int(param.get('Value2')), 'scene_fk': scenes}
            res_denom = self.tools.calculate_availability(**filters2)
            if res_denom > 0:
                res = str(float(res_nom)/res_denom)
                res = float(res)
                res = round(res, 3)
                res *= 100
                score = res
                res = float("{0:.2f}".format(res))
            else:
                score = 0
                res = float(0)
            self.save_level2_and_level3(set_name=set_name, kpi_name=param.get('KPI Name'), result=res,
                                        score=score, threshold=0)
        else:
            res = res_nom
            self.save_level2_and_level3(set_name=set_name, kpi_name=param.get('KPI Name'), result=res_nom,
                                        score=res, threshold=0)

    def calculate_availability_position(self, param, set_name):
        tested_shelf = []
        ancor_filters = {}
        tested_filters = {}
        result = 0
        score = 0
        ancore_self = [int(g) for g in param.get('Value3').split(",")]
        shelves = self.match_product_in_scene['shelf_number'].unique().tolist()
        for shelf in shelves:
            if int(shelf) not in ancore_self:
                if shelf > 0:
                    tested_shelf.append(shelf)
        scif_scenes = self.scif.loc[self.scif['template_group'] == param.get('Template_Group')]
        scenes = scif_scenes['scene_id'].unique().tolist()
        for scene in scenes:
            ancor_filters[param.get('Param1')] = [str(g) for g in param.get('Value1').split(",")]
            ancor_filters['shelf_number'] = ancore_self
            ancor_filters['scene_fk'] = scene
            res1 = self.tools.calculate_availability(**ancor_filters)
            tested_filters[param.get('Param2')] = [str(g) for g in param.get('Value2').split(",")]
            tested_filters['shelf_number'] = tested_shelf
            tested_filters['scene_fk'] = scene
            res2 = self.tools.calculate_availability(**tested_filters)
            if res1 >= 1 and res2 >= 1:
                result = + 1
        if result >= 1:
            score = 1
        self.save_level2_and_level3(set_name=set_name, kpi_name=param.get('KPI Name'), result=result,
                                    score=score, threshold=0)

    def write_to_db_result(self, fk, result, level, score=None, threshold=None):
        """
        This function the result data frame of every KPI (atomic KPI/KPI/KPI set),
        and appends the insert SQL query into the queries' list, later to be written to the DB.
        """
        attributes = self.create_attributes_dict(fk, result, level, score=score, threshold=threshold)
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
        # result = round(result, 2)
        if level == self.LEVEL1:
            kpi_set_name = self.kpi_static_data[self.kpi_static_data['kpi_set_fk'] == fk]['kpi_set_name'].values[0]
            score_type = '%' if kpi_set_name in self.tools.KPI_SETS_WITH_PERCENT_AS_SCORE else ''
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
                                        fk, kpi_name, result)],
                                      columns=['session_uid', 'store_fk', 'visit_date', 'kpi_fk', 'kpk_name', 'score'])
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

    @log_runtime('Saving to DB')
    def commit_results_data(self,kpi_set_fk=None):
        """
        This function writes all KPI results to the DB, and commits the changes.
        """
        kpi_pks = tuple()
        atomic_pks = tuple()
        if kpi_set_fk is not None:
            query = CCUSQueries.get_atomic_pk_to_delete(self.session_uid, kpi_set_fk)
            kpi_atomic_data = pd.read_sql_query(query, self.rds_conn.db)
            atomic_pks = tuple(kpi_atomic_data['pk'].tolist())
            kpi_data = pd.read_sql_query(query, self.rds_conn.db)
            kpi_pks = tuple(kpi_data['pk'].tolist())
        cur = self.rds_conn.db.cursor()
        if kpi_pks and atomic_pks:
            delete_queries = CCUSQueries.get_delete_session_results_query(self.session_uid, kpi_set_fk, kpi_pks,
                                                                          atomic_pks)
            for query in delete_queries:
                cur.execute(query)
        for query in self.kpi_results_queries:
            cur.execute(query)
        self.rds_conn.db.commit()



# import datetime
#
# import pandas as pd
# from Trax.Algo.Calculations.Core.Constants import Fields as Fd
# from Trax.Algo.Calculations.Core.DataProvider import Data, Keys
# from Trax.Algo.Calculations.Core.Shortcuts import SessionInfo, BaseCalculationsGroup
# from Trax.Cloud.Services.Connector.Keys import DbUsers
# from Trax.Data.Orm.OrmCore import OrmSession
# from KPIUtils_v2.DB.PsProjectConnector import PSProjectConnector
# from Trax.Data.Utils.MySQLservices import get_table_insertion_query as insert
# from Trax.Utils.Logging.Logger import Log
#
# from Projects.CCUS_SAND2.Fetcher import CCUSKPIFetcher

# __author__ = 'nimrodp'
#
# KPI_RESULT = 'report.kpi_results'
# KPK_RESULT = 'report.kpk_results'
# KPS_RESULT = 'report.kps_results'
# AVAILABILITY_GROUP = ('Availability of KO SSD Transaction Packages',
#                       'Availability of KO SSD/ Still Diet or Zero IC Brands outside of the Main Beverage Aisle',
#                       'Availability of Cold KO SSD/Still Diet or Zero IC Brands','Cold Drink Equipment (CDE) In-Stock',
#                       'Availability of KO SSD Transaction Packages','availability of ko tea or coffee skus'
#                       )
# MAX_SCORE = 100
#
#
# class CCUSKPIToolBox:
#     def __init__(self, data_provider, output, set_name=None):
#         self.data_provider = data_provider
#         self.output = output
#         self.products = self.data_provider[Data.ALL_PRODUCTS]
#         self.k_engine = BaseCalculationsGroup(data_provider, output)
#         self.project_name = data_provider.project_name
#         self.session_uid = self.data_provider.session_uid
#         self.products = self.data_provider[Data.ALL_PRODUCTS]
#         self.match_product_in_scene = self.data_provider[Data.MATCHES]
#         self.visit_date = self.data_provider[Data.VISIT_DATE]
#         self.scenes_info = self.data_provider[Data.SCENES_INFO]
#         self.store_info = self.data_provider[Data.STORE_INFO]
#         self.rds_conn = PSProjectConnector(self.project_name, DbUsers.CalculationEng)
#         self.session_info = SessionInfo(data_provider)
#         self.store_id = self.data_provider[Data.STORE_FK]
#         self.scif = self.data_provider[Data.SCENE_ITEM_FACTS]
#         self.kpi_fetcher = CCUSKPIFetcher(self.project_name, self.scif, self.match_product_in_scene)
#         self.survey_response = self.data_provider[Data.SURVEY_RESPONSES]
#         self.sales_rep_fk = self.data_provider[Data.SESSION_INFO]['s_sales_rep_fk'].iloc[0]
#         self.session_fk = self.data_provider[Data.SESSION_INFO]['pk'].iloc[0]
#         self.set_name = set_name
#
#     def calculate_kpi(self, params):
#         """
#         :param params:
#         :return:
#         """
#         for p in params.values()[0]:
#             kpi_res = 0
#             if p.get("level of calculation") == "kpi" and self.store_info.store_type == p.get("store_type"):
#                 if p.get('KPI_Type') in 'Availability':
#                     children = [int(child) for child in p.get("atomic_kpi_fk").split(",")]
#                     atomic_scores = self.calculate_availability_for_atomics(params, children)
#                     if sum(atomic_scores.values()) >= MAX_SCORE * p.get("target"):
#                         kpi_res = MAX_SCORE
#                 if p.get('KPI_Type') in 'Facing SOS':
#                     kpi_scores = self.calculate_sos(p)
#
#                     self.write_to_db_result(p, kpi_res, "level2")
#
#
#     def calculate_availability_for_atomics(self, params, children):
#         """
#         :param params:
#         :return:
#         """
#         atomic_scores = {}
#         for p in params.values()[0]:
#             if p.get("level of calculation") == "atomic" and int(p.get("atomic_kpi_fk")) in children:
#                 atomic_results = {}
#                 for scene in self.scenes_info['scene_fk'].unique().tolist():
#                     scene_results = self.kpi_fetcher.get_object_facings(scene, p)
#                     for sku in scene_results.keys():
#                         if sku not in atomic_results.keys():
#                             atomic_results[sku] = 0
#                         atomic_results[sku] += scene_results[sku]
#                 if len(atomic_results):
#                     atomic_score = 100
#                 else:
#                     atomic_score = 0
#                 if p.get('entity') == 'SKU':
#                     result = len(atomic_results)
#                 else: # number of facings
#                     result = sum(atomic_results.values())
#                 atomic_scores[p.get("atomic_kpi_fk")] = atomic_score
#                 self.write_to_db_result(p, (result, atomic_score), "level3")
#         return atomic_scores
#
#     def calculate_sos(self, params):
#         scores = []
#         kpi_res = self.tools.calculate_share_of_shelf(manufacturer=self.tools.DIAGEO,
#                                                       include_empty=self.tools.EXCLUDE_EMPTY)
#         score = 1 if kpi_res >= params.get(self.tools.TARGET) else 0
#         scores.append(score)
#
#         kpi_fk = self.kpi_static_data[self.kpi_static_data['kpi_name'] ==
#                                       params.get(self.tools.KPI_NAME)]['kpi_fk'].values[0]
#         atomic_kpi_fk = self.kpi_static_data[self.kpi_static_data['kpi_fk'] == kpi_fk]['atomic_kpi_fk'].values[
#             0]
#         self.write_to_db_result(kpi_fk, score, self.LEVEL2)
#         self.write_to_db_result(atomic_kpi_fk, score, self.LEVEL3)
#
#         set_score = (sum(scores) / float(len(scores))) * 100
#         return set_score
#
#
#
#     def write_to_db_result(self, params, score, level):
#         """
#         This function writes KPI results to old tables
#
#         """
#         if level == 'level3':
#             df = self.create_attributes_for_level3_df(params, score)
#             kpi_pk_to_overwrite = self.kpi_fetcher.get_atomic_kpi_fk_to_overwrite(df['session_uid'][0],
#                                                                            df['atomic_kpi_fk'][0])
#             df_dict = df.to_dict()
#             if not kpi_pk_to_overwrite:
#                 query = insert(df_dict, KPI_RESULT)
#             else:
#                 condition = "session_uid = '{}' AND atomic_kpi_fk = {}".format(df['session_uid'][0],
#                                                                                  df['atomic_kpi_fk'][0])
#                 query = self.kpi_fetcher.get_table_update_query(df_dict, KPI_RESULT, condition)
#             self.insert_results_data(query)
#         elif level == 'level2':
#             df = self.create_attributes_for_level2_df(params, score)
#             kpi_pk_to_overwrite = self.kpi_fetcher.get_kpi_fk_to_overwrite(df['session_uid'][0], df['kpi_fk'][0])
#             df_dict = df.to_dict()
#             if not kpi_pk_to_overwrite:
#                 query = insert(df_dict, KPK_RESULT)
#             else:
#                 condition = "session_uid = '{}' AND kpi_fk = {}".format(df['session_uid'][0], df['kpi_fk'][0])
#                 query = self.kpi_fetcher.get_table_update_query(df_dict, KPK_RESULT, condition)
#             self.insert_results_data(query)
#
#     def insert_results_data(self, query):
#         cur = self.rds_conn.db.cursor()
#         cur.execute(query)
#
#         self.rds_conn.db.commit()
#         return
#
#     def create_attributes_for_level2_df(self, params, score):
#         """
#         This function creates a data frame with all attributes needed for saving in level 2 tables
#
#         """
#         attributes_for_table2 = pd.DataFrame([(self.session_uid, self.store_id,
#                                                self.visit_date.isoformat(), params.get('kpi_fk'),
#                                                params.get('kpi_name'), score)],
#                                              columns=['session_uid', 'store_fk', 'visit_date', 'kpi_fk',
#                                                       'kpk_name', 'score'])
#
#         return attributes_for_table2
#
#     def create_attributes_for_level3_df(self, params, score):
#         """
#         This function creates a data frame with all attributes needed for saving in level 3 tables
#
#         """
#         attributes_for_table3 = pd.DataFrame([(params.get('atomic_kpi_name'), self.session_uid, self.set_name,
#                                                self.store_id,
#                                                self.visit_date.isoformat(),
#                                                datetime.datetime.utcnow().isoformat(),
#                                                score[1], params.get("kpi_fk"), params.get("atomic_kpi_fk"),
#                                                score[0], params.get("target"))],
#                                              columns=['display_text', 'session_uid', 'kps_name',
#                                                       'store_fk', 'visit_date',
#                                                       'calculation_time', 'score', 'kpi_fk',
#                                                       'atomic_kpi_fk', 'result', 'threshold'])
#
#         return attributes_for_table3
