from datetime import datetime
import pandas as pd
# from Trax.Utils.Logging.Logger import Log
from Trax.Data.Utils.MySQLservices import get_table_insertion_query as insert
from Trax.Algo.Calculations.Core.DataProvider import Data
from Projects.CCBOTTLERSUS_SAND.REDSCORE.Const import Const
from KPIUtils_v2.DB.Common import Common
from KPIUtils_v2.GlobalDataProvider.PsDataProvider import PsDataProvider
from Projects.CCBOTTLERSUS_SAND.REDSCORE.FunctionsToolBox import FunctionsToolBox

__author__ = 'Elyashiv'


class REDToolBox:

    def __init__(self, data_provider, output, calculation_type, common_db2):
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
        self.store_info = self.data_provider[Data.STORE_INFO]
        self.scif = self.data_provider[Data.SCENE_ITEM_FACTS]
        self.scif = self.scif[self.scif['product_type'] != "Irrelevant"]
        self.ps_data_provider = PsDataProvider(self.data_provider, self.output)
        self.templates = {}
        self.result_values = self.ps_data_provider.get_result_values()
        self.calculation_type = calculation_type
        if self.calculation_type == Const.SOVI:
            self.TEMPLATE_PATH = Const.TEMPLATE_PATH
            self.RED_SCORE = Const.RED_SCORE
            self.RED_SCORE_INTEG = Const.RED_SCORE_INTEG
            for sheet in Const.SHEETS:
                self.templates[sheet] = pd.read_excel(self.TEMPLATE_PATH, sheetname=sheet).fillna('')
            self.converters = self.templates[Const.CONVERTERS]
            self.scenes_results = self.ps_data_provider.get_scene_results(
                self.scene_info['scene_fk'].drop_duplicates().values)
            self.scenes_results = self.scenes_results[[Const.DB_RESULT, Const.DB_SCENE_FK, Const.DB_SCENE_KPI_FK]]
        else:
            self.TEMPLATE_PATH = Const.SURVEY_TEMPLATE_PATH
            self.RED_SCORE = Const.MANUAL_RED_SCORE
            self.RED_SCORE_INTEG = Const.MANUAL_RED_SCORE_INTEG
            for sheet in Const.SHEETS_MANUAL:
                self.templates[sheet] = pd.read_excel(self.TEMPLATE_PATH, sheetname=sheet).fillna('')
        self.store_attr = self.store_info['additional_attribute_15'].iloc[0]
        self.toolbox = FunctionsToolBox(self.data_provider, self.output, self.templates, self.store_attr)
        self.common_db_integ = Common(self.data_provider, self.RED_SCORE_INTEG)
        self.kpi_static_data_integ = self.common_db_integ.get_kpi_static_data()
        self.common_db = Common(self.data_provider, self.RED_SCORE)
        self.common_db2 = common_db2
        self.region = self.store_info['region_name'].iloc[0]
        self.store_type = self.store_info['store_type'].iloc[0]
        if self.store_type in Const.STORE_TYPES:
            self.store_type = Const.STORE_TYPES[self.store_type]
        self.kpi_static_data = self.common_db.get_kpi_static_data()
        main_template = self.templates[Const.KPIS]
        self.templates[Const.KPIS] = main_template[(main_template[Const.REGION] == self.region) &
                                                   (main_template[Const.STORE_TYPE] == self.store_type)]
        self.session_results = pd.DataFrame(columns=Const.COLUMNS_OF_RESULTS)
        self.all_results = pd.DataFrame(columns=Const.COLUMNS_OF_RESULTS)
        self.used_scenes = []
        self.red_score = 0
        self.set_fk = self.common_db2.get_kpi_fk_by_kpi_name(self.RED_SCORE)
        self.set_integ_fk = self.common_db2.get_kpi_fk_by_kpi_name(self.RED_SCORE_INTEG)
        self.weight_factor = self.get_weight_factor()

    # main functions:

    def main_calculation(self, *args, **kwargs):
        """
            This function gets all the scene results from the SceneKPI, after that calculates every session's KPI,
            and in the end it calls "filter results" to choose every KPI and scene and write the results in DB.
        """
        main_template = self.templates[Const.KPIS]
        if self.calculation_type == Const.SOVI:
            session_template = main_template[main_template[Const.SESSION_LEVEL] == Const.V]
            for i, main_line in session_template.iterrows():
                self.calculate_main_kpi(main_line)
        else:
            for i, main_line in main_template.iterrows():
                self.calculate_manual_kpi(main_line)
        if not main_template.empty:
            self.choose_and_write_results()
        return self.red_score

    def calculate_main_kpi(self, main_line):
        """
        This function gets a line from the main_sheet, transfers it to the match function, and checks all of the
        KPIs in the same name in the match sheet.
        :param main_line: series from the template of the main_sheet.
        """
        kpi_name = main_line[Const.KPI_NAME]
        relevant_scif = self.scif
        scene_types = self.toolbox.does_exist(main_line, Const.SCENE_TYPE)
        if scene_types:
            relevant_scif = relevant_scif[relevant_scif['template_name'].isin(scene_types)]
        scene_groups = self.toolbox.does_exist(main_line, Const.SCENE_TYPE_GROUP)
        if scene_groups:
            relevant_scif = relevant_scif[relevant_scif['template_group'].isin(scene_groups)]
        if main_line[Const.SHEET] == Const.SCENE_AVAILABILITY:
            result = False if relevant_scif.empty else True
        else:
            result = self.toolbox.calculate_kpi_by_type(main_line, relevant_scif)
        self.write_to_session_level(kpi_name=kpi_name, result=result)

    def calculate_manual_kpi(self, main_line):
        """
        This function gets a line from the main_sheet, transfers it to the match function, and checks all of the
        KPIs in the same name in the match sheet.
        :param main_line: series from the template of the main_sheet.
        """
        kpi_name = main_line[Const.KPI_NAME]
        relevant_template = self.templates[Const.SURVEY]
        relevant_template = relevant_template[relevant_template[Const.KPI_NAME] == kpi_name]
        target = len(relevant_template) if main_line[Const.GROUP_TARGET] == Const.ALL \
            else main_line[Const.GROUP_TARGET]
        passed_counter = 0
        for i, kpi_line in relevant_template.iterrows():
            answer = self.toolbox.calculate_survey_specific(kpi_line)
            if answer:
                passed_counter += 1
        result = passed_counter >= target
        self.write_to_session_level(kpi_name=kpi_name, result=result)

    # write in DF:

    def write_to_session_level(self, kpi_name, result=0):
        """
        Writes a result in the DF
        :param kpi_name: string
        :param result: boolean
        """
        result_dict = {Const.KPI_NAME: kpi_name, Const.DB_RESULT: result * 1}
        self.session_results = self.session_results.append(result_dict, ignore_index=True)

    def write_to_all_levels(self, kpi_name, result, display_text, weight, scene_fk=None, reuse_scene=False):
        """
        Writes the final result in the "all" DF, add the score to the red score and writes the KPI in the DB
        :param kpi_name: str
        :param result: int
        :param display_text: str
        :param weight: int/float
        :param scene_fk: for the scene's kpi
        :param reuse_scene: this kpi can use scenes that were used
        """
        score = self.get_score(weight)
        result_value = Const.PASS if result > 0 else Const.FAIL
        if result_value == Const.PASS:
            self.red_score += score
        result_dict = {Const.KPI_NAME: kpi_name, Const.DB_RESULT: result, Const.SCORE: score}
        if scene_fk:
            result_dict[Const.DB_SCENE_FK] = scene_fk
            if not reuse_scene:
                self.used_scenes.append(scene_fk)
        self.all_results = self.all_results.append(result_dict, ignore_index=True)
        self.write_to_db(kpi_name, score, display_text=display_text, result_value=result_value)

    def choose_and_write_results(self):
        """
        writes all the KPI in the DB: first the session's ones, second the scene's ones and in the end the ones
        that depends on the previous ones. After all it writes the red score
        """
        main_template = self.templates[Const.KPIS]
        self.write_session_kpis(main_template)
        if self.calculation_type == Const.SOVI:
            self.write_scene_kpis(main_template)
        self.write_condition_kpis(main_template)
        self.write_missings(main_template)
        self.write_to_db(self.RED_SCORE, self.red_score)

    def write_missings(self, main_template):
        """
        write 0 in all the KPIs that didn't get score
        :param main_template:
        """
        for i, main_line in main_template.iterrows():
            kpi_name = main_line[Const.KPI_NAME]
            if not self.all_results[self.all_results[Const.KPI_NAME] == kpi_name].empty:
                continue
            result = 0
            display_text = main_line[Const.DISPLAY_TEXT]
            weight = main_line[Const.WEIGHT]
            self.write_to_all_levels(kpi_name, result, display_text, weight)

    def write_session_kpis(self, main_template):
        """
        iterates all the session's KPIs and saves them
        :param main_template: main_sheet.
        """
        session_template = main_template[main_template[Const.CONDITION] == ""]
        if self.calculation_type == Const.SOVI:
            session_template = session_template[session_template[Const.SESSION_LEVEL] == Const.V]
        for i, main_line in session_template.iterrows():
            kpi_name = main_line[Const.KPI_NAME]
            result = self.session_results[self.session_results[Const.KPI_NAME] == kpi_name]
            if result.empty:
                continue
            result = result.iloc[0][Const.DB_RESULT]
            display_text = main_line[Const.DISPLAY_TEXT]
            weight = main_line[Const.WEIGHT]
            self.write_to_all_levels(kpi_name, result, display_text, weight)

    def write_incremental_kpis(self, scene_template):
        """
        lets the incremental KPIs choose their scenes (if they passed).
        if KPI passed some scenes, we will choose the scene that the children passed
        :param scene_template: filtered main_sheet
        :return: the new template (without the KPI written already)
        """
        incremental_template = scene_template[scene_template[Const.INCREMENTAL] != ""]
        while not incremental_template.empty:
            for i, main_line in incremental_template.iterrows():
                kpi_name = main_line[Const.KPI_NAME]
                reuse_scene = main_line[Const.REUSE_SCENE] == Const.V
                kpi_scene_fk = self.common_db2.get_kpi_fk_by_kpi_name(kpi_name + Const.SCENE_SUFFIX)
                kpi_results = self.scenes_results[self.scenes_results[Const.DB_SCENE_KPI_FK] == kpi_scene_fk]
                if not reuse_scene:
                    kpi_results = kpi_results[~(kpi_results[Const.DB_SCENE_FK].isin(self.used_scenes))]
                true_results = kpi_results[kpi_results[Const.DB_RESULT] > 0]
                increments = main_line[Const.INCREMENTAL]
                if ', ' in increments:
                    first_kpi = increments.split(', ')[0]
                    others = increments.replace(', '.format(first_kpi), '')
                    scene_template.loc[scene_template[Const.KPI_NAME] == first_kpi, Const.INCREMENTAL] = others
                if true_results.empty:
                    scene_template.loc[scene_template[Const.KPI_NAME] == kpi_name, Const.INCREMENTAL] = ""
                else:
                    true_results = true_results.sort_values(by=Const.DB_RESULT, ascending=False)
                    display_text = main_line[Const.DISPLAY_TEXT]
                    weight = main_line[Const.WEIGHT]
                    scene_fk = true_results.iloc[0][Const.DB_SCENE_FK]
                    self.write_to_all_levels(kpi_name, true_results.iloc[0][Const.DB_RESULT], display_text,
                                             weight, scene_fk=scene_fk, reuse_scene=reuse_scene)
                    scene_template = scene_template[~(scene_template[Const.KPI_NAME] == kpi_name)]
            incremental_template = scene_template[scene_template[Const.INCREMENTAL] != ""]
        return scene_template

    def write_regular_scene_kpis(self, scene_template):
        """
        lets the regular KPIs choose their scenes (if they passed).
        Like in the incremental part - if KPI passed some scenes, we will choose the scene that the children passed
        :param scene_template: filtered main_sheet (only scene KPIs, and without the passed incremental)
        :return: the new template (without the KPI written already)
        """
        for i, main_line in scene_template.iterrows():
            kpi_name = main_line[Const.KPI_NAME]
            reuse_scene = main_line[Const.REUSE_SCENE] == Const.V
            kpi_scene_fk = self.common_db2.get_kpi_fk_by_kpi_name(kpi_name + Const.SCENE_SUFFIX)
            kpi_results = self.scenes_results[self.scenes_results[Const.DB_SCENE_KPI_FK] == kpi_scene_fk]
            if not reuse_scene:
                kpi_results = kpi_results[~(kpi_results[Const.DB_SCENE_FK].isin(self.used_scenes))]
            true_results = kpi_results[kpi_results[Const.DB_RESULT] > 0]
            display_text = main_line[Const.DISPLAY_TEXT]
            weight = main_line[Const.WEIGHT]
            if true_results.empty:
                continue
            true_results = true_results.sort_values(by=Const.DB_RESULT, ascending=False)
            scene_fk = true_results.iloc[0][Const.DB_SCENE_FK]
            self.write_to_all_levels(kpi_name, true_results.iloc[0][Const.DB_RESULT], display_text, weight,
                                     scene_fk=scene_fk, reuse_scene=reuse_scene)
            scene_template = scene_template[~(scene_template[Const.KPI_NAME] == kpi_name)]
        return scene_template

    def write_not_passed_scene_kpis(self, scene_template):
        """
        lets the KPIs not passed choose their scenes.
        :param scene_template: filtered main_sheet (only scene KPIs, and without the passed KPIs)
        """
        for i, main_line in scene_template.iterrows():
            kpi_name = main_line[Const.KPI_NAME]
            kpi_scene_fk = self.common_db2.get_kpi_fk_by_kpi_name(kpi_name + Const.SCENE_SUFFIX)
            reuse_scene = main_line[Const.REUSE_SCENE] == Const.V
            kpi_results = self.scenes_results[self.scenes_results[Const.DB_SCENE_KPI_FK] == kpi_scene_fk]
            if not reuse_scene:
                kpi_results = kpi_results[~(kpi_results[Const.DB_SCENE_FK].isin(self.used_scenes))]
            display_text = main_line[Const.DISPLAY_TEXT]
            weight = main_line[Const.WEIGHT]
            if kpi_results.empty:
                continue
            scene_fk = kpi_results.iloc[0][Const.DB_SCENE_FK]
            self.write_to_all_levels(kpi_name, 0, display_text, weight, scene_fk=scene_fk, reuse_scene=reuse_scene)

    def write_scene_kpis(self, main_template):
        """
        iterates every scene_kpi that does not depend on others, and choose the scene they will take:
        1. the incrementals take their scene (if they passed).
        2. the regular KPIs that passed choose their scenes.
        3. the ones that didn't pass choose their random scenes.
        :param main_template: main_sheet.
        """
        scene_template = main_template[(main_template[Const.SESSION_LEVEL] != Const.V) &
                                       (main_template[Const.CONDITION] == "")]
        scene_template = self.write_incremental_kpis(scene_template)
        scene_template = self.write_regular_scene_kpis(scene_template)
        self.write_not_passed_scene_kpis(scene_template)

    def write_condition_kpis(self, main_template):
        """
        writes all the KPI that depend on other KPIs by checking if the parent KPI has passed and in which scene.
        :param main_template: main_sheet
        """
        condition_template = main_template[main_template[Const.CONDITION] != '']
        for i, main_line in condition_template.iterrows():
            condition = main_line[Const.CONDITION]
            kpi_name = main_line[Const.KPI_NAME]
            if self.calculation_type == Const.MANUAL or main_line[Const.SESSION_LEVEL] == Const.V:
                kpi_results = self.session_results[self.session_results[Const.KPI_NAME] == kpi_name]
            else:
                kpi_scene_fk = self.common_db2.get_kpi_fk_by_kpi_name(kpi_name + Const.SCENE_SUFFIX)
                kpi_results = self.scenes_results[self.scenes_results[Const.DB_SCENE_KPI_FK] == kpi_scene_fk]
            condition_result = self.all_results[(self.all_results[Const.KPI_NAME] == condition) &
                                                (self.all_results[Const.DB_RESULT] > 0)]
            if condition_result.empty:
                continue
            condition_result = condition_result.iloc[0]

            if Const.DB_SCENE_FK in condition_result:
                condition_scene = condition_result[Const.DB_SCENE_FK]
            else:
                condition_scene = None

            if condition_scene and Const.DB_SCENE_FK in kpi_results:
                results = kpi_results[kpi_results[Const.DB_SCENE_FK] == condition_scene]
            else:
                results = kpi_results
            if results.empty:
                continue
            result = results.iloc[0][Const.DB_RESULT]
            display_text = main_line[Const.DISPLAY_TEXT]
            weight = main_line[Const.WEIGHT]
            scene_fk = results.iloc[0][Const.DB_SCENE_FK] if Const.DB_SCENE_FK in kpi_results else None
            self.write_to_all_levels(kpi_name, result, display_text, weight, scene_fk=scene_fk)

    def get_weight_factor(self):
        sum_weights = self.templates[Const.KPIS][Const.WEIGHT].sum()
        return sum_weights / 100.0

    def get_score(self, weight):
        return weight / self.weight_factor

    def get_pks_of_result(self, result):
        """
        converts string result to its pk (in static.kpi_result_value)
        :param result: str
        :return: int
        """
        pk = self.result_values[self.result_values['value'] == result]['pk'].iloc[0]
        return pk

    @staticmethod
    def get_0_1_of_result(result):
        """
        converts string result to its pk (in static.kpi_result_value)
        :param result: str
        :return: int
        """
        pk = 0 if result == Const.FAIL else 1
        return pk

    def write_to_db(self, kpi_name, score, display_text='', result_value=Const.FAIL):
        """
        writes result in the DB
        :param kpi_name: str
        :param score: float, the weight of the question
        :param display_text: str
        :param result_value: str, Pass/Fail
        """
        if kpi_name == self.RED_SCORE:
            self.common_db2.write_to_db_result(
                fk=self.set_fk, score=score, numerator_id=Const.MANUFACTURER_FK, denominator_id=self.store_id,
                identifier_result=self.common_db2.get_dictionary(kpi_fk=self.set_fk))
            self.common_db2.write_to_db_result(
                fk=self.set_integ_fk, score=score, numerator_id=Const.MANUFACTURER_FK, denominator_id=self.store_id,
                identifier_result=self.common_db2.get_dictionary(kpi_fk=self.set_integ_fk))
            self.write_to_db_result(
                self.common_db.get_kpi_fk_by_kpi_name(self.RED_SCORE, 1), score=score, level=1)
            self.write_to_db_result(
                self.common_db_integ.get_kpi_fk_by_kpi_name(self.RED_SCORE_INTEG, 1), score=score, level=1,
                set_type=Const.MANUAL)
        else:
            integ_kpi_fk = self.common_db2.get_kpi_fk_by_kpi_name(kpi_name)
            display_kpi_fk = self.common_db2.get_kpi_fk_by_kpi_name(display_text)
            if display_kpi_fk is None:
                display_kpi_fk = self.common_db2.get_kpi_fk_by_kpi_name(display_text[:100])
            result = self.get_pks_of_result(result_value)
            self.common_db2.write_to_db_result(
                fk=display_kpi_fk, score=score, identifier_parent=self.common_db2.get_dictionary(kpi_fk=self.set_fk),
                should_enter=True, result=result, numerator_id=Const.MANUFACTURER_FK, denominator_id=self.store_id)
            result = self.get_0_1_of_result(result_value)
            self.common_db2.write_to_db_result(
                fk=integ_kpi_fk, score=score, should_enter=True, result=result,
                identifier_parent=self.common_db2.get_dictionary(kpi_fk=self.set_integ_fk),
                numerator_id=Const.MANUFACTURER_FK, denominator_id=self.store_id)
            if result_value == Const.FAIL:
                score = 0
            self.write_to_db_result(
                self.common_db.get_kpi_fk_by_kpi_name(kpi_name, 2), score=score, level=2)
            self.write_to_db_result(
                self.common_db.get_kpi_fk_by_kpi_name(kpi_name, 3), score=score, level=3, display_text=display_text)
            self.write_to_db_result(self.common_db_integ.get_kpi_fk_by_kpi_name(
                kpi_name, 3), score=score, level=3, display_text=kpi_name, set_type=Const.MANUAL)

    def write_to_db_result(self, fk, level, score, set_type=Const.SOVI, **kwargs):
        """
        This function creates the result data frame of every KPI (atomic KPI/KPI/KPI set),
        and appends the insert SQL query into the queries' list, later to be written to the DB.
        """
        if kwargs:
            kwargs['score'] = score
            attributes = self.create_attributes_dict(fk=fk, level=level, set_type=set_type, **kwargs)
        else:
            attributes = self.create_attributes_dict(fk=fk, score=score, set_type=set_type, level=level)
        if level == self.common_db.LEVEL1:
            table = self.common_db.KPS_RESULT
        elif level == self.common_db.LEVEL2:
            table = self.common_db.KPK_RESULT
        elif level == self.common_db.LEVEL3:
            table = self.common_db.KPI_RESULT
        else:
            return
        query = insert(attributes, table)
        if set_type == Const.SOVI:
            self.common_db.kpi_results_queries.append(query)
        else:
            self.common_db_integ.kpi_results_queries.append(query)

    def create_attributes_dict(self, score, fk=None, level=None, display_text=None, set_type=Const.SOVI, **kwargs):
        """
        This function creates a data frame with all attributes needed for saving in KPI results tables.
        or
        you can send dict with all values in kwargs
        """
        kpi_static_data = self.kpi_static_data if set_type == Const.SOVI else self.kpi_static_data_integ
        if level == self.common_db.LEVEL1:
            if kwargs:
                kwargs['score'] = score
                values = [val for val in kwargs.values()]
                col = [col for col in kwargs.keys()]
                attributes = pd.DataFrame(values, columns=col)
            else:
                kpi_set_name = kpi_static_data[kpi_static_data['kpi_set_fk'] == fk]['kpi_set_name'].values[0]
                attributes = pd.DataFrame(
                    [(kpi_set_name, self.session_uid, self.store_id, self.visit_date.isoformat(),
                      format(score, '.2f'), fk)],
                    columns=['kps_name', 'session_uid', 'store_fk', 'visit_date', 'score_1', 'kpi_set_fk'])
        elif level == self.common_db.LEVEL2:
            if kwargs:
                kwargs['score'] = score
                values = [val for val in kwargs.values()]
                col = [col for col in kwargs.keys()]
                attributes = pd.DataFrame(values, columns=col)
            else:
                kpi_name = kpi_static_data[kpi_static_data['kpi_fk'] == fk]['kpi_name'].values[0].replace("'", "\\'")
                attributes = pd.DataFrame(
                    [(self.session_uid, self.store_id, self.visit_date.isoformat(), fk, kpi_name, score)],
                    columns=['session_uid', 'store_fk', 'visit_date', 'kpi_fk', 'kpk_name', 'score'])
        elif level == self.common_db.LEVEL3:
            if kwargs:
                kwargs['score'] = score
                values = tuple([val for val in kwargs.values()])
                col = [col for col in kwargs.keys()]
                attributes = pd.DataFrame([values], columns=col)
            else:
                data = kpi_static_data[kpi_static_data['atomic_kpi_fk'] == fk]
                kpi_fk = data['kpi_fk'].values[0]
                kpi_set_name = kpi_static_data[kpi_static_data['atomic_kpi_fk'] == fk]['kpi_set_name'].values[0]
                attributes = pd.DataFrame(
                    [(display_text, self.session_uid, kpi_set_name, self.store_id, self.visit_date.isoformat(),
                      datetime.utcnow().isoformat(), score, kpi_fk, fk)],
                    columns=['display_text', 'session_uid', 'kps_name', 'store_fk', 'visit_date',
                             'calculation_time', 'score', 'kpi_fk', 'atomic_kpi_fk'])
        else:
            attributes = pd.DataFrame()
        return attributes.to_dict()

    def remove_queries_of_calculation_type(self):
        """
        In case that the session has no results in the SOVI KPIs we are deleting all the queries
        and calculating the MANUAL
        :return:
        """
        self.common_db2.kpi_results = pd.DataFrame(columns=self.common_db2.COLUMNS)

    def commit_results(self):
        """
        committing the results in both sets
        """
        self.common_db.delete_results_data_by_kpi_set()
        self.common_db.commit_results_data_without_delete()
        if self.common_db_integ:
            self.common_db_integ.delete_results_data_by_kpi_set()
            self.common_db_integ.commit_results_data_without_delete()
