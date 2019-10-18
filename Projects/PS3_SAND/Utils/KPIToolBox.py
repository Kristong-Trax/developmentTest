from KPIUtils_v2.Utils.GlobalScripts.Scripts import GlobalSessionToolBox
from KPIUtils_v2.Utils.Consts.DataProvider import ProductsConsts, ScifConsts, StoreInfoConsts,\
    TemplatesConsts
from KPIUtils_v2.Utils.Consts.PS import Tables, ExternalTargetsConsts
from KPIUtils_v2.DB.PsProjectConnector import PSProjectConnector
from KPIUtils_v2.Utils.Consts.DB import SceneResultsConsts
from KPIUtils_v2.Utils.Consts.GlobalConsts import MergeConsts, BasicConsts, HelperConsts
from Trax.Utils.Conf.Keys import DbUsers
from KPIUtils_v2.GlobalDataProvider.PsDataProvider import PsDataProvider
from Trax.Data.Utils.MySQLservices import get_table_insertion_query as insert
from Trax.Utils.Logging.Logger import Log
from Projects.PS3_SAND.Data.LocalConsts import Consts
from datetime import datetime, timedelta
# from Trax.Algo.Calculations.Core.DataProvider import Data
import pandas as pd
import numpy as np
import json


PROJECTS_WITH_DYNAMIC_TARGETS = {"googlemx": "2019-07-01"}

__author__ = 'Eli_Sam_Shivi'


class ToolBox(GlobalSessionToolBox):

    def __init__(self, data_provider, output, common):
        GlobalSessionToolBox.__init__(self, data_provider, output, common)
        self.templates = self.data_provider.all_templates
        self.ps_data_provider = PsDataProvider(self.data_provider, self.output)
        self.store_number = self.store_info[StoreInfoConsts.STORE_NUMBER_1][0].encode(HelperConsts.UTF8)
        if self.scenes:
            self.scene_results = self.ps_data_provider.get_scene_results(self.scenes)
        else:
            self.scene_results = pd.DataFrame(columns=[SceneResultsConsts.KPI_LEVEL_2_FK, SceneResultsConsts.SCORE,
                                                       SceneResultsConsts.SCENE_FK, SceneResultsConsts.RESULT])
        self.store_info = self.ps_data_provider.get_ps_store_info(self.store_info)
        self.required_fixtures = []
        self.added_targets_to_templates = {}
        self.chosen_scenes = {Consts.ENTRY: [], Consts.EXIT: []}
        self.insert_store_level = False
        self.targets = self.ps_data_provider.get_kpi_external_targets(
            key_fields=Consts.KEY_JSON_FIELDS, data_fields=Consts.DATA_JSON_FIELDS)
        self.is_there_scenes = {Consts.EXIT: 0, Consts.ENTRY: 0}
        try:
            if self.project_name in PROJECTS_WITH_DYNAMIC_TARGETS.keys() and self.visit_date >=\
                    datetime.strptime(PROJECTS_WITH_DYNAMIC_TARGETS[self.project_name], '%Y-%m-%d').date():
                self.get_iterative_required_fixtures_from_external_targets()
            else:
                self.get_required_fixtures_from_external_targets()
        except Exception as e:
            self.get_required_fixtures_from_external_targets()
            Log.error(e)

# getting the fixture targets functions:

    def get_required_fixtures_from_external_targets(self):
        """
        This function creates a list (self.required_fixtures) the fixture_fk in the template with the required amount
        of it, the names of its Entry and Exit template_name, and the match scenes of this session
        """
        fixture_template = self.targets[self.targets[ExternalTargetsConsts.OPERATION_TYPE] == Consts.TARGETS_OPERATION]
        exit_df = self.templates[
            self.templates[TemplatesConsts.ADDITIONAL_ATTRIBUTE_1].str.strip().str.lower() == Consts.EXIT.lower()][
            [TemplatesConsts.TEMPLATE_GROUP, TemplatesConsts.TEMPLATE_FK]].rename(
            index=str, columns={TemplatesConsts.TEMPLATE_FK: Consts.EXIT_TEMPLATE_FK})
        entry_df = self.templates[
            self.templates[TemplatesConsts.ADDITIONAL_ATTRIBUTE_1].str.strip().str.lower() == Consts.ENTRY.lower()][
            [TemplatesConsts.TEMPLATE_GROUP, TemplatesConsts.TEMPLATE_FK]].rename(
            index=str, columns={TemplatesConsts.TEMPLATE_FK: Consts.ENTRY_TEMPLATE_FK})
        entry_exit = pd.merge(exit_df, entry_df, how=MergeConsts.OUTER, on=TemplatesConsts.TEMPLATE_GROUP)
        # entry_exit.loc[entry_exit['exit_template_fk'].isnull(), 'exit_template_fk'] = entry_exit[
        #     entry_exit['exit_template_fk'].isnull()]['entry_template_fk']
        # entry_exit.loc[entry_exit['entry_template_fk'].isnull(), 'entry_template_fk'] = entry_exit[
        #     entry_exit['entry_template_fk'].isnull()]['exit_template_fk']
        if fixture_template.empty:
            Log.warning(Consts.MSG_NO_FIXTURE_TARGETS_FOR_DATE)
            return
        relevant_fixtures = fixture_template[
            fixture_template[StoreInfoConsts.STORE_NUMBER_1].str.encode(HelperConsts.UTF8) == self.store_number]
        if relevant_fixtures.empty:
            Log.warning(Consts.MSG_NO_FIXTURE_TARGETS_FOR_STORE.format(self.store_number))
            return
        for template_group in relevant_fixtures[TemplatesConsts.TEMPLATE_GROUP].unique().tolist():
            fixture_entry_exit = entry_exit[
                entry_exit[TemplatesConsts.TEMPLATE_GROUP].str.encode(HelperConsts.UTF8) ==
                template_group.encode(HelperConsts.UTF8)]
            required_amount = relevant_fixtures[
                relevant_fixtures[TemplatesConsts.TEMPLATE_GROUP].str.encode(HelperConsts.UTF8) ==
                template_group.encode(HelperConsts.UTF8)][Consts.FIXTURES_TARGET].sum()
            entry_fk, exit_fk = None, None
            if not fixture_entry_exit.empty:
                fixture_entry_exit = fixture_entry_exit.iloc[0]
                entry_fk = fixture_entry_exit[Consts.ENTRY_TEMPLATE_FK]
                exit_fk = fixture_entry_exit[Consts.EXIT_TEMPLATE_FK]
            entry_scenes = self.scif[self.scif[ScifConsts.TEMPLATE_FK]
                                     == entry_fk][ScifConsts.SCENE_FK].unique().tolist()
            exit_scenes = self.scif[self.scif[ScifConsts.TEMPLATE_FK] == exit_fk][ScifConsts.SCENE_FK].unique().tolist()
            fixture_dict = {
                Consts.REQUIRED_AMOUNT: required_amount,
                Consts.TEMPLATE_FKS: {Consts.ENTRY: entry_fk, Consts.EXIT: exit_fk},
                Consts.SCENE_FKS: {Consts.ENTRY: entry_scenes, Consts.EXIT: exit_scenes},
                Consts.TEMPLATE_GROUP: template_group}
            self.required_fixtures.append(fixture_dict)

    def get_iterative_required_fixtures_from_external_targets(self):
        """
        This function creates a list (self.required_fixtures) the fixture_fk in the template with the required amount
        of it, the names of its Entry and Exit template_name, and the match scenes of this session
        """
        exit_df = self.templates[
            self.templates[TemplatesConsts.ADDITIONAL_ATTRIBUTE_1].str.strip().str.lower() == Consts.EXIT.lower()][
            [TemplatesConsts.TEMPLATE_GROUP, TemplatesConsts.TEMPLATE_FK]].rename(
            index=str, columns={TemplatesConsts.TEMPLATE_FK: Consts.EXIT_TEMPLATE_FK})
        entry_df = self.templates[
            self.templates[TemplatesConsts.ADDITIONAL_ATTRIBUTE_1].str.strip().str.lower() == Consts.ENTRY.lower()][
            [TemplatesConsts.TEMPLATE_GROUP, TemplatesConsts.TEMPLATE_FK]].rename(
            index=str, columns={TemplatesConsts.TEMPLATE_FK: Consts.ENTRY_TEMPLATE_FK})
        template_group_details = pd.merge(exit_df, entry_df, how=MergeConsts.OUTER, on=TemplatesConsts.TEMPLATE_GROUP)
        # template_group_details.loc[template_group_details['exit_template_fk'].isnull(),
        # 'exit_template_fk'] = template_group_details[
        #     template_group_details['exit_template_fk'].isnull()]['entry_template_fk']
        # template_group_details.loc[template_group_details['entry_template_fk'].isnull(),
        # 'entry_template_fk'] = template_group_details[
        #     template_group_details['entry_template_fk'].isnull()]['exit_template_fk']
        relevant_fixtures = self.get_fixture_targets(template_group_details)
        for template_group in relevant_fixtures[TemplatesConsts.TEMPLATE_GROUP].unique().tolist():
            fixture_entry_exit = template_group_details[
                template_group_details[TemplatesConsts.TEMPLATE_GROUP].str.encode(HelperConsts.UTF8)
                == template_group.encode(HelperConsts.UTF8)]
            required_amount = relevant_fixtures[
                relevant_fixtures[TemplatesConsts.TEMPLATE_GROUP].str.encode(HelperConsts.UTF8)
                == template_group.encode(HelperConsts.UTF8)][
                Consts.FIXTURES_TARGET].sum()
            entry_fk, exit_fk = None, None
            if not fixture_entry_exit.empty:
                fixture_entry_exit = fixture_entry_exit.iloc[0]
                entry_fk = fixture_entry_exit[Consts.ENTRY_TEMPLATE_FK]
                exit_fk = fixture_entry_exit[Consts.EXIT_TEMPLATE_FK]
            entry_scenes = self.scif[self.scif[ScifConsts.TEMPLATE_FK]
                                     == entry_fk][ScifConsts.SCENE_FK].unique().tolist()
            exit_scenes = self.scif[self.scif[ScifConsts.TEMPLATE_FK] == exit_fk][ScifConsts.SCENE_FK].unique().tolist()
            fixture_dict = {
                Consts.REQUIRED_AMOUNT: required_amount, Consts.TEMPLATE_GROUP: template_group,
                Consts.TEMPLATE_FKS: {Consts.ENTRY: entry_fk, Consts.EXIT: exit_fk},
                Consts.SCENE_FKS: {Consts.ENTRY: entry_scenes, Consts.EXIT: exit_scenes}}
            self.required_fixtures.append(fixture_dict)

    def get_fixture_targets(self, template_group_details):
        kpi_fk = self.get_kpi_fk_by_kpi_name(Consts.PASSED_RULE)
        scenes_results = self.scene_results[(self.scene_results[SceneResultsConsts.KPI_LEVEL_2_FK] == kpi_fk) &
                                            (self.scene_results[SceneResultsConsts.SCORE] == 1)]
        templates_count = self.scif[
            (self.scif[ScifConsts.SCENE_ID].isin(scenes_results[SceneResultsConsts.SCENE_FK])) &
            (self.scif[ScifConsts.ADDITIONAL_ATTRIBUTE_1] == Consts.EXIT) &
            ~(self.scif[ScifConsts.TEMPLATE_GROUP].isin(Consts.EXCLUDE_TEMPLATE_GROUP))][
            [ScifConsts.TEMPLATE_GROUP, ScifConsts.SCENE_FK]].drop_duplicates().groupby(ScifConsts.TEMPLATE_GROUP)\
            .count()[ScifConsts.SCENE_FK].to_dict()
        fixture_template = self.get_external_fixture_targets()
        add_targets, targets_end_pks = [], []
        for template_group in templates_count.keys():
            template_actual_sum = templates_count[template_group]
            current_template_target = fixture_template[
                fixture_template[Consts.TEMPLATE_GROUP].str.encode(HelperConsts.UTF8)
                == template_group.encode(HelperConsts.UTF8)]
            if current_template_target.empty:
                self.added_targets_to_templates[template_group] = template_actual_sum
                add_targets.append({Consts.TEMPLATE_GROUP: template_group, Consts.FIXTURES_TARGET: template_actual_sum,
                                    Consts.AUTOMATIC_ADDED: template_actual_sum})
                continue
            current_target, current_automatic_added, target_fk = current_template_target.iloc[0][
                [Consts.FIXTURES_TARGET, Consts.AUTOMATIC_ADDED, ExternalTargetsConsts.EXTERNAL_TARGET_FK]]
            template_actual_sum = max(template_actual_sum, current_target)
            target_to_add = template_actual_sum - current_target
            if target_to_add > 0:
                targets_end_pks.append(target_fk)
                Log.info(Consts.MSG_HIGHER_TARGET.format(template_group.encode(HelperConsts.UTF8)))
                self.added_targets_to_templates[template_group] = target_to_add
                add_targets.append({Consts.TEMPLATE_GROUP: template_group, Consts.FIXTURES_TARGET: template_actual_sum,
                                    Consts.AUTOMATIC_ADDED: target_to_add})
            else:
                template_new_targets = current_template_target[
                    current_template_target[Consts.SESSION_OF_NEW_TARGET] == self.session_uid]
                if not template_new_targets.empty:
                    self.added_targets_to_templates[template_group] = template_new_targets.iloc[0][
                        Consts.AUTOMATIC_ADDED]
        for template_group in set(fixture_template[Consts.TEMPLATE_GROUP]) - set(
                template_group_details[TemplatesConsts.TEMPLATE_GROUP]):
            # TODO: It doesn't solve the case there are more than 1 record for store+template
            current_template_target = fixture_template[
                fixture_template[TemplatesConsts.TEMPLATE_GROUP].str.encode(HelperConsts.UTF8)
                == template_group.encode(HelperConsts.UTF8)]
            Log.info(Consts.MSG_TEMPLATE_GROUP_DELETED.format(template_group.encode(HelperConsts.UTF8)))
            targets_end_pks.append(current_template_target[ExternalTargetsConsts.EXTERNAL_TARGET_FK].iloc[0])
        if targets_end_pks or add_targets:
            self.send_queries(targets_end_pks, add_targets)
            self.targets = self.ps_data_provider.get_kpi_external_targets(
                key_fields=Consts.KEY_JSON_FIELDS, data_fields=Consts.DATA_JSON_FIELDS)
            fixture_template = self.get_external_fixture_targets()
        return fixture_template

    def send_queries(self, targets_end_pks, add_targets):
        end_query, add_queries = None, []
        yesterdate, visit_date_str = str(self.visit_date - timedelta(1)), str(self.visit_date)
        current_time = str(datetime.now())
        if targets_end_pks:
            end_query = Consts.UPDATE_EXTERNAL_TARGET_QUERY.format(yesterdate, str(targets_end_pks)[1:-1])
        if add_targets:
            kpi_fk = self.get_kpi_fk_by_kpi_name(
                Consts.KPIS_DICT[Consts.FIXTURE_AVAILABILITY][Consts.TEMPLATE_LEVEL])
            for add_target in add_targets:
                key_json = {StoreInfoConsts.STORE_NUMBER_1: self.store_number,
                            Consts.TEMPLATE_GROUP: add_target[Consts.TEMPLATE_GROUP]}
                data_json = {Consts.FIXTURES_TARGET: add_target[Consts.FIXTURES_TARGET],
                             Consts.AUTOMATIC_ADDED: add_target[Consts.AUTOMATIC_ADDED],
                             Consts.SESSION_OF_NEW_TARGET: self.session_uid}
                insert_dict = {
                    ExternalTargetsConsts.KPI_OPERATION_TYPE_FK: {0: '1'},
                    ExternalTargetsConsts.START_DATE: {0: visit_date_str},
                    ExternalTargetsConsts.KEY_JSON: {0: self.convert_json_to_str(key_json)},
                    ExternalTargetsConsts.DATA_JSON: {0: self.convert_json_to_str(data_json)},
                    ExternalTargetsConsts.RECEIVED_TIME: {0: current_time},
                    ExternalTargetsConsts.KPI_LEVEL_2_FK: {0: kpi_fk}}
                add_queries.append(insert(insert_dict, Tables.EXTERNAL_TARGETS))
            add_queries = self.common.merge_insert_queries(add_queries)
        local_con = PSProjectConnector(self.project_name, DbUsers.CalculationEng)
        cur = local_con.db.cursor()
        Log.info(Consts.MSG_COMMIT_EXTERNAL_TARGETS)
        if add_queries:
            cur.execute(add_queries[0])
        if end_query:
            cur.execute(end_query)
        local_con.db.commit()

    @staticmethod
    def convert_json_to_str(json_dict):
        json_str = json.dumps(json_dict, ensure_ascii=False)
        json_str = json_str.replace("'", "\\'")
        return json_str

    def get_external_fixture_targets(self):
        fixture_template = self.targets[self.targets[ExternalTargetsConsts.OPERATION_TYPE] == Consts.TARGETS_OPERATION]
        if fixture_template.empty:
            return pd.DataFrame(columns=[Consts.TEMPLATE_GROUP, Consts.FIXTURES_TARGET])
        fixture_template.loc[fixture_template[Consts.AUTOMATIC_ADDED].isnull(), Consts.AUTOMATIC_ADDED] = 0
        fixture_template = fixture_template[[
            ExternalTargetsConsts.EXTERNAL_TARGET_FK, StoreInfoConsts.STORE_NUMBER_1, Consts.TEMPLATE_GROUP,
            Consts.FIXTURES_TARGET, Consts.AUTOMATIC_ADDED, Consts.SESSION_OF_NEW_TARGET]]
        fixture_template = fixture_template[
            fixture_template[StoreInfoConsts.STORE_NUMBER_1].str.encode(HelperConsts.UTF8)
            == self.store_number.encode(HelperConsts.UTF8)]
        return fixture_template

# Fixture Availability:

    def get_fixture_availability(self):
        """
        Calculates for every fixture (from the self.required_fixtures) the amount of exist scenes out of the required
        """
        expected_fixtures_sum, actual_fixtures_sum = 0.0, 0.0
        kpi_fk = self.get_kpi_fk_by_kpi_name(Consts.KPIS_DICT[Consts.FIXTURE_AVAILABILITY][Consts.TEMPLATE_LEVEL])
        required_templates = []
        for fixture in self.required_fixtures:
            template_group, actual_fixtures, expected_fixtures = self.write_required_fixtures(fixture, kpi_fk)
            if actual_fixtures > expected_fixtures:
                actual_fixtures = expected_fixtures
            actual_fixtures_sum += actual_fixtures
            expected_fixtures_sum += expected_fixtures
            required_templates.append(template_group)
        self.write_not_required_fixtures(required_templates, kpi_fk)
        if actual_fixtures_sum > 0:
            self.insert_store_level = True
        score = self.get_percentage(actual_fixtures_sum, expected_fixtures_sum, allow_0_in_den=True)
        store_kpi_fk = self.get_kpi_fk_by_kpi_name(
            Consts.KPIS_DICT[Consts.FIXTURE_AVAILABILITY][Consts.STORE_LEVEL])
        self.write_to_db(
            fk=store_kpi_fk, result=score,
            identifier_result=Consts.KPIS_DICT[Consts.FIXTURE_AVAILABILITY][Consts.STORE_LEVEL],
            score=score, numerator_result=actual_fixtures_sum, denominator_result=expected_fixtures_sum)

    def write_required_fixtures(self, fixture, kpi_fk):
        template_group = fixture[Consts.TEMPLATE_GROUP]
        fixture_fk = fixture[Consts.TEMPLATE_FKS][Consts.EXIT]
        numerator = max(len(fixture[Consts.SCENE_FKS][Consts.EXIT]), len(fixture[Consts.SCENE_FKS][Consts.ENTRY]))
        denominator = fixture[Consts.REQUIRED_AMOUNT]
        ratio = self.get_percentage(numerator, denominator, allow_0_in_den=True)
        score, numerator_after_action = 0, numerator
        if ratio >= 100:
            ratio, score, numerator_after_action = 100, 1, denominator
        new_target_uploaded = None
        if template_group in self.added_targets_to_templates.keys():
            new_target_uploaded = self.added_targets_to_templates[fixture[Consts.TEMPLATE_GROUP]]
        self.write_to_db(
            fk=kpi_fk, numerator_id=fixture_fk, numerator_result=numerator, weight=new_target_uploaded,
            denominator_result=denominator, score=score, result=ratio, target=numerator_after_action,
            identifier_parent=Consts.KPIS_DICT[Consts.FIXTURE_AVAILABILITY][Consts.STORE_LEVEL], should_enter=True)
        added_target_kpi_fk = self.get_kpi_fk_by_kpi_name(Consts.ADDED_EXTERNAL_TARGETS)
        self.write_to_db(fk=added_target_kpi_fk, numerator_id=fixture_fk, result=new_target_uploaded)
        return template_group, numerator, denominator

    def write_not_required_fixtures(self, expected_templates, kpi_fk):
        actual_templates = self.scene_info.merge(self.templates[[
            TemplatesConsts.TEMPLATE_FK, TemplatesConsts.ADDITIONAL_ATTRIBUTE_1, TemplatesConsts.TEMPLATE_GROUP]],
                                                 on=TemplatesConsts.TEMPLATE_FK)
        expected_template_groups = actual_templates[actual_templates[TemplatesConsts.TEMPLATE_GROUP].isin(
            expected_templates)][TemplatesConsts.TEMPLATE_GROUP].tolist()
        unexpected_scenes = actual_templates[~actual_templates[TemplatesConsts.TEMPLATE_GROUP].isin(
            expected_template_groups)]
        for template_group in unexpected_scenes[TemplatesConsts.TEMPLATE_GROUP].unique().tolist():
            if template_group is None:
                continue
            template_group_scenes = unexpected_scenes[unexpected_scenes[TemplatesConsts.TEMPLATE_GROUP].str.encode(
                HelperConsts.UTF8) == template_group.encode(HelperConsts.UTF8)]
            entry_scenes = template_group_scenes[
                template_group_scenes[TemplatesConsts.ADDITIONAL_ATTRIBUTE_1].str.strip().str.lower() ==
                Consts.ENTRY.lower()]
            exit_scenes = template_group_scenes[
                template_group_scenes[TemplatesConsts.ADDITIONAL_ATTRIBUTE_1].str.strip().str.lower() ==
                Consts.EXIT.lower()]
            actual = max(len(entry_scenes), len(exit_scenes))
            fixture_fk = entry_scenes if exit_scenes.empty else exit_scenes
            if not fixture_fk.empty:
                fixture_fk = fixture_fk[TemplatesConsts.TEMPLATE_FK].iloc[0]
                self.write_to_db(
                    fk=kpi_fk, numerator_id=fixture_fk, numerator_result=actual, denominator_id=fixture_fk,
                    denominator_result=0, score=0, result=0, should_enter=True,
                    identifier_parent=Consts.KPIS_DICT[Consts.FIXTURE_AVAILABILITY][Consts.STORE_LEVEL])

# POGs:

    def get_pog_store(self):
        fixture_kpi_fk = self.get_kpi_fk_by_kpi_name(Consts.KPIS_DICT[Consts.POG_COMPLIANCE][Consts.FIXTURE_LEVEL])
        pog_scene_results = self.scene_results[self.scene_results[SceneResultsConsts.KPI_LEVEL_2_FK] == fixture_kpi_fk]
        sum_of_entry_nums, sum_of_entry_dens = 0, 0
        sum_of_exit_nums, sum_of_exit_dens = 0, 0
        entry_scores, exit_scores = [], []
        for fixture in self.required_fixtures:
            if fixture[Consts.REQUIRED_AMOUNT] == 0:
                continue
            entry_nums, entry_dens, exit_nums, exit_dens, entry_score, exit_score = self.get_pog_visit(
                fixture, pog_scene_results)
            entry_scores.append(entry_score)
            exit_scores.append(exit_score)
            sum_of_entry_nums += entry_nums
            sum_of_entry_dens += entry_dens
            sum_of_exit_nums += exit_nums
            sum_of_exit_dens += exit_dens
        # exit_score = self.get_percentage(sum_of_exit_nums, sum_of_exit_dens, allow_0_in_den=True)
        exit_kpi_fk = self.get_kpi_fk_by_kpi_name(
            Consts.KPIS_DICT[Consts.POG_COMPLIANCE][Consts.STORE_LEVEL][Consts.EXIT])
        exit_score = self.get_average(exit_scores)
        self.write_to_db(
            fk=exit_kpi_fk, result=exit_score, score=exit_score, target=self.is_there_scenes[Consts.EXIT],
            identifier_result=Consts.KPIS_DICT[Consts.POG_COMPLIANCE][Consts.STORE_LEVEL][Consts.EXIT],
            numerator_result=sum_of_exit_nums, denominator_result=sum_of_exit_dens)
        if self.insert_store_level:
            # entry_score = self.get_percentage(sum_of_entry_nums, sum_of_entry_dens, allow_0_in_den=True)
            entry_score = self.get_average(entry_scores)
            entry_kpi_fk = self.get_kpi_fk_by_kpi_name(
                Consts.KPIS_DICT[Consts.POG_COMPLIANCE][Consts.STORE_LEVEL][Consts.ENTRY])
            self.write_to_db(
                fk=entry_kpi_fk, result=entry_score, score=entry_score, target=self.is_there_scenes[Consts.ENTRY],
                numerator_result=sum_of_entry_nums, denominator_result=sum_of_entry_dens)

    def get_pog_visit(self, fixture, pog_scene_results):
        """
        Gets a fixture, and writes in the DB its POG score
        :param fixture: dict with entry + exit names, match scene_fks Etc (from self.required_fixtures)
        :param pog_scene_results: filtered scene_results
        :return: the POG average (for the set score)
        """
        required_amount = int(fixture[Consts.REQUIRED_AMOUNT])
        fixture_fk = fixture[Consts.TEMPLATE_FKS][Consts.EXIT]
        exit_pog_results, exit_scenes, exit_score, exit_nums, exit_dens = self.get_pog_template(
            fixture, pog_scene_results, Consts.EXIT, 0, fixture_fk, required_amount)
        entry_pog_results, entry_scenes, entry_score, entry_nums, entry_dens = self.get_pog_template(
            fixture, pog_scene_results, Consts.ENTRY, exit_score, fixture_fk, required_amount)
        self.look_for_anomalies(entry_pog_results, exit_pog_results, exit_scenes + entry_scenes)
        pog_kpi_fk = self.get_kpi_fk_by_kpi_name(Consts.KPIS_DICT[Consts.POG_COMPLIANCE][Consts.VISIT_LEVEL])
        pog_delta = exit_score - entry_score
        identifier_result = self.get_dictionary(fixture=fixture_fk, kpi_fk=pog_kpi_fk)
        for scene_result_fk in exit_pog_results[BasicConsts.PK].values:
            self.write_to_db(should_enter=True, scene_result_fk=scene_result_fk,
                             identifier_parent=identifier_result, only_hierarchy=True)
        self.write_to_db(
            fk=pog_kpi_fk, numerator_id=fixture_fk, target=required_amount, result=exit_score,
            identifier_parent=Consts.KPIS_DICT[Consts.POG_COMPLIANCE][Consts.STORE_LEVEL][Consts.EXIT],
            numerator_result=pog_delta, score=exit_score, should_enter=True, identifier_result=identifier_result)
        self.write_chosen_scenes_in_db(exit_scenes=exit_scenes, entry_scenes=entry_scenes)
        return entry_nums, entry_dens, exit_nums, exit_dens, exit_score, entry_score

    def get_pog_template(self, fixture, pog_scene_results, exit_entry, default_value, fixture_fk, required_amount):
        kpi_fk = self.get_kpi_fk_by_kpi_name(
            Consts.KPIS_DICT[Consts.POG_COMPLIANCE][Consts.TEMPLATE_LEVEL][exit_entry])
        scenes = fixture[Consts.SCENE_FKS][exit_entry]
        pog_results = pog_scene_results[pog_scene_results[SceneResultsConsts.SCENE_FK].isin(scenes)]
        scenes, pog_results = self.get_best_scenes(pog_results, required_amount)
        self.chosen_scenes[exit_entry] += scenes
        fixture[Consts.SCENE_FKS][exit_entry] = scenes
        nums, dens = self.get_sum_of_nums_dens(pog_results)
        # score = self.get_percentage(nums, dens) if dens > 0 else default_value
        score = 0
        if len(scenes) > 0:
            score = pog_results['score'].describe()['mean']
            self.is_there_scenes[exit_entry] += len(scenes)
            self.write_to_db(fk=kpi_fk, numerator_id=fixture_fk, result=score, numerator_result=nums,
                             denominator_result=dens, score=score, target=required_amount)
        return pog_results, scenes, score, nums, dens

    def look_for_anomalies(self, entry_pog_results, exit_pog_results, scenes):
        """
        Looks for the possible anomalies and returns the pk of the match result value.
        :param entry_pog_results: DF
        :param exit_pog_results: DF
        :param scenes: list of all the considered scenes
        :return: pk
        """
        if entry_pog_results[SceneResultsConsts.SCENE_FK].count()\
                != exit_pog_results[SceneResultsConsts.SCENE_FK].count():
            anomaly = Consts.DIFFERENT_NUMBER_OF_ENTRY_AND_EXIT_SCENES
        elif exit_pog_results.empty or entry_pog_results.empty:
            anomaly = Consts.NO_SCENE_WITH_SCENE_TYPE
        elif 0 in entry_pog_results[SceneResultsConsts.RESULT].tolist() or 0 in \
                exit_pog_results[SceneResultsConsts.RESULT].tolist():
            anomaly = Consts.SCENES_WITHOUT_POG
        elif entry_pog_results[SceneResultsConsts.DENOMINATOR_RESULT].iloc[0] \
                != exit_pog_results[SceneResultsConsts.DENOMINATOR_RESULT].iloc[0]:
            anomaly = Consts.DIFFERENT_PRODUCT_POG_ANOMALY
        elif self.check_delta_between_the_entry_and_exit(scenes):
            anomaly = Consts.DIFFERENT_PRODUCT_SCENE_ANOMALY
        else:
            anomaly = Consts.NO_ANOMALY
        Log.debug(anomaly)

    def check_delta_between_the_entry_and_exit(self, scenes, deviation_percentage=0.1):
        """
        Check if there is too big difference between the number of facings in the scenes by checking the distance of
         each one from the average of them all.
        :param scenes: only the required scenes (list of ints)
        :param deviation_percentage: the maximum approved distance.
        :return: True if the distance is big.
        """
        facings = []
        for scene in scenes:
            facings.append(self.scif[self.scif[ScifConsts.SCENE_FK] == scene][ScifConsts.FACINGS].sum())
        avg = reduce(lambda x, y: x + y, facings) / len(facings)
        range_20 = (avg * (1 + deviation_percentage), avg * (1 - deviation_percentage))
        for scene_facings in facings:
            if scene_facings > range_20[0] or scene_facings < range_20[1]:
                return True
        return False

    def write_chosen_scenes_in_db(self, entry_scenes, exit_scenes):
        """
        This kpi is writing which of the scenes are the required scenes, the scenes calculated in the POG and OSA score.
        """
        entry_scenes_kpi_fk = self.get_kpi_fk_by_kpi_name(Consts.CHOSEN_SCENES_ENTRY)
        exit_scene_kpi_fk = self.get_kpi_fk_by_kpi_name(Consts.CHOSEN_SCENES_EXIT)
        for scene_fk in entry_scenes:
            self.write_to_db(fk=entry_scenes_kpi_fk, numerator_id=scene_fk, result=1)
        for scene_fk in exit_scenes:
            self.write_to_db(fk=exit_scene_kpi_fk, numerator_id=scene_fk, result=1)

    @staticmethod
    def get_best_scenes(scene_results, required_amount):
        if None in scene_results[SceneResultsConsts.RESULT].tolist():
            return None, scene_results
        best_scores = scene_results.sort_values(by=SceneResultsConsts.SCORE, ascending=False)[:required_amount]
        scenes = best_scores[SceneResultsConsts.SCENE_FK].unique().tolist()
        return scenes, best_scores

# Facings Compliance:

    def get_facings_compliance_store(self):
        fixture_facings_kpi_fk = self.get_kpi_fk_by_kpi_name(
            Consts.KPIS_DICT[Consts.FACINGS_COMPLIANCE][Consts.FIXTURE_LEVEL])
        fc_scene_results = self.scene_results[
            self.scene_results[SceneResultsConsts.KPI_LEVEL_2_FK] == fixture_facings_kpi_fk]
        sum_of_entry_nums, sum_of_entry_dens = 0, 0
        sum_of_exit_nums, sum_of_exit_dens = 0, 0
        entry_scores, exit_scores = [], []
        for fixture in self.required_fixtures:
            if fixture[Consts.REQUIRED_AMOUNT] == 0:
                continue
            exit_nums, exit_dens, exit_score = self.get_facings_compliance_template(
                fixture, fc_scene_results, Consts.EXIT, 0)
            entry_nums, entry_dens, entry_score = self.get_facings_compliance_template(
                fixture, fc_scene_results, Consts.ENTRY, exit_score)
            entry_scores.append(entry_score)
            exit_scores.append(exit_score)
            sum_of_entry_nums += entry_nums
            sum_of_entry_dens += entry_dens
            sum_of_exit_nums += exit_nums
            sum_of_exit_dens += exit_dens
        if self.insert_store_level:
            # exit_score = self.get_percentage(sum_of_exit_nums, sum_of_exit_dens, allow_0_in_den=True)
            # entry_score = self.get_percentage(sum_of_entry_nums, sum_of_entry_dens, allow_0_in_den=True)
            exit_score = self.get_average(exit_scores)
            entry_score = self.get_average(entry_scores)
            exit_kpi_fk = self.get_kpi_fk_by_kpi_name(
                Consts.KPIS_DICT[Consts.FACINGS_COMPLIANCE][Consts.STORE_LEVEL][Consts.EXIT])
            entry_kpi_fk = self.get_kpi_fk_by_kpi_name(
                Consts.KPIS_DICT[Consts.FACINGS_COMPLIANCE][Consts.STORE_LEVEL][Consts.ENTRY])
            self.write_to_db(fk=exit_kpi_fk, result=exit_score, score=exit_score,
                             numerator_result=sum_of_exit_nums, denominator_result=sum_of_exit_dens)
            self.write_to_db(fk=entry_kpi_fk, result=entry_score, score=entry_score,
                             numerator_result=sum_of_entry_nums, denominator_result=sum_of_entry_dens)

    def get_facings_compliance_template(self, fixture, fc_scene_results, entry_exit, default_score):
        """
        Gets a fixture, and writes in the DB its POG score
        :param fixture: dict with entry + exit names, match scene_fks Etc (from self.required_fixtures)
        :param fc_scene_results: filtered scene_results
        :param entry_exit: EXIT or ENTRY
        :param default_score: the exit score if it's entry, and 0 if it's exit
        :return: the POG average (for the set score)
        """
        kpi_fk = self.get_kpi_fk_by_kpi_name(
            Consts.KPIS_DICT[Consts.FACINGS_COMPLIANCE][Consts.TEMPLATE_LEVEL][entry_exit])
        required_amount = int(fixture[Consts.REQUIRED_AMOUNT])
        fixture_fk = fixture[Consts.TEMPLATE_FKS][Consts.EXIT]
        scenes = fixture[Consts.SCENE_FKS][entry_exit]
        fc_results = fc_scene_results[fc_scene_results[SceneResultsConsts.SCENE_FK].isin(scenes)]
        nums, dens = self.get_sum_of_nums_dens(fc_results)
        # score = self.get_percentage(nums, dens, allow_0_in_den=True) if dens > 0 else default_score
        score = 0
        if len(scenes) > 0:
            score = fc_results['score'].describe()['mean']
            self.write_to_db(fk=kpi_fk, numerator_id=fixture_fk, result=score,
                             numerator_result=nums, denominator_result=dens, score=score, target=required_amount)
        return nums, dens, score

# Denom Availability:

    def get_da_store(self):
        """
        Calculates for every fixture the OSA (%) and the OOS (#)
        """
        fixture_osa_kpi_fk = self.get_kpi_fk_by_kpi_name(
            Consts.KPIS_DICT[Consts.DENOM_AVAILABILITY][Consts.FIXTURE_LEVEL])
        osa_scene_results = self.scene_results[self.scene_results[SceneResultsConsts.KPI_LEVEL_2_FK]
                                               == fixture_osa_kpi_fk]
        osa_exit_scores, oos_exit_scores, osa_entry_scores, oos_entry_scores = [], [], [], []
        for fixture in self.required_fixtures:
            if fixture[Consts.REQUIRED_AMOUNT] == 0:
                continue
            avg_osa_exit, avg_oos_exit, avg_osa_entry, avg_oos_entry = self.get_da_visit(fixture, osa_scene_results)
            osa_exit_scores.append(avg_osa_exit)
            osa_entry_scores.append(avg_osa_entry)
            oos_exit_scores.append(avg_oos_exit)
            oos_entry_scores.append(avg_oos_entry)
        if self.insert_store_level:
            self.write_da_store_in_db(osa_exit_scores, oos_exit_scores, Consts.EXIT)
            self.write_da_store_in_db(osa_entry_scores, oos_entry_scores, Consts.ENTRY)

    def write_da_store_in_db(self, osa_scores, oos_scores, entry_exit):
        osa_score = self.get_average(osa_scores)
        oos_score = self.get_average(oos_scores)
        kpi_fk = self.get_kpi_fk_by_kpi_name(
            Consts.KPIS_DICT[Consts.DENOM_AVAILABILITY][Consts.STORE_LEVEL][entry_exit])
        self.write_to_db(fk=kpi_fk, result=oos_score, score=osa_score)

    def get_da_visit(self, fixture, osa_scene_results):
        """
        Gets a fixture, and writes in the DB its OOS+OSA score
        :param fixture: dict with entry + exit names, match scene_fks Etc (from self.required_fixtures)
        :param osa_scene_results: filtered scene_results
        :return: the POG average (for the set score)
        """
        osa_kpi_fk = self.get_kpi_fk_by_kpi_name(Consts.KPIS_DICT[Consts.DENOM_AVAILABILITY][Consts.VISIT_LEVEL])
        required_amount = int(fixture[Consts.REQUIRED_AMOUNT])
        fixture_fk = fixture[Consts.TEMPLATE_FKS][Consts.EXIT]
        avg_osa_exit, avg_oos_exit = self.get_da_template(
            fixture, osa_scene_results, Consts.EXIT, fixture_fk, required_amount)
        avg_osa_entry, avg_oos_entry = self.get_da_template(
            fixture, osa_scene_results, Consts.ENTRY, fixture_fk, required_amount)
        osa_delta = avg_osa_exit if np.isnan(avg_osa_entry) else avg_osa_exit - avg_osa_entry
        oos_delta = avg_oos_exit if np.isnan(avg_oos_entry) else avg_oos_exit - avg_oos_entry
        self.write_to_db(
            fk=osa_kpi_fk, numerator_id=fixture_fk, target=required_amount,
            numerator_result=osa_delta, denominator_result=oos_delta, result=avg_oos_exit, score=avg_osa_exit)
        return avg_osa_exit, avg_oos_exit, avg_osa_entry, avg_oos_entry

    def get_da_template(self, fixture, osa_scene_results, exit_entry, fixture_fk, required_amount):
        """
        Gets a fixture, and writes in the DB its OOS+OSA score
        :param fixture: dict with entry + exit names, match scene_fks Etc (from self.required_fixtures)
        :param osa_scene_results: filtered scene_results
        :param exit_entry: str, EXIT or ENTRY
        :param fixture_fk: number
        :param required_amount: the expected fixtures
        :return: the POG average (for the set score)
        """
        kpi_fk = self.get_kpi_fk_by_kpi_name(
            Consts.KPIS_DICT[Consts.DENOM_AVAILABILITY][Consts.TEMPLATE_LEVEL][exit_entry])
        osa_results = osa_scene_results[osa_scene_results[SceneResultsConsts.SCENE_FK].isin(fixture[Consts.SCENE_FKS]
                                                                                            [exit_entry])]
        avg_osa, avg_oos = 0, 0
        amount = len(osa_results)
        if amount > 0:
            avg_osa = osa_results['score'].describe()['mean']
            avg_oos = osa_results['result'].describe()['mean']
            self.write_to_db(fk=kpi_fk, numerator_id=fixture_fk, result=avg_oos, score=avg_osa, target=required_amount)
        return avg_osa, avg_oos

# SOC:

    def get_share_of_competitor_store(self):
        if self.insert_store_level:
            relative_kpi_fk = self.get_kpi_fk_by_kpi_name(
                Consts.KPIS_DICT[Consts.SHARE_OF_COMPETITOR][Consts.FIXTURE_LEVEL])
            total_kpi_fk = self.get_kpi_fk_by_kpi_name(Consts.SOS_SCENE)
            scene_results = self.scene_results[self.scene_results[SceneResultsConsts.KPI_LEVEL_2_FK] == relative_kpi_fk]
            total_scene_results = self.scene_results[self.scene_results[SceneResultsConsts.KPI_LEVEL_2_FK]
                                                     == total_kpi_fk]
            apple_fk = self.all_products[self.all_products[ProductsConsts.BRAND_NAME]
                                         == Consts.APPLE_BRAND][ProductsConsts.BRAND_FK]
            apple_fk = None if apple_fk.empty else apple_fk.iloc[0]
            google_fk = self.all_products[self.all_products[ProductsConsts.BRAND_NAME]
                                          == Consts.GOOGLE_BRAND][ProductsConsts.BRAND_FK].iloc[0]
            self.write_share_of_competitor_store(total_scene_results, scene_results, google_fk, apple_fk, Consts.EXIT)
            self.write_share_of_competitor_store(total_scene_results, scene_results, google_fk, apple_fk, Consts.ENTRY)

    def write_share_of_competitor_store(self, total_scene_results, scene_results, google_fk, apple_fk, exit_entry):
        total_facings = total_scene_results[total_scene_results[SceneResultsConsts.SCENE_FK].isin(
            self.chosen_scenes[exit_entry])][
            [SceneResultsConsts.SCENE_FK,
             SceneResultsConsts.DENOMINATOR_RESULT]].drop_duplicates()[SceneResultsConsts.DENOMINATOR_RESULT].sum()
        comparison = scene_results[(scene_results[SceneResultsConsts.DENOMINATOR_ID] == apple_fk) &
                                   (scene_results[SceneResultsConsts.SCENE_FK].isin(self.chosen_scenes[exit_entry]))]
        if comparison.empty:
            apple_sum = 0
        else:
            apple_sum = comparison[SceneResultsConsts.DENOMINATOR_RESULT].sum()
        comparison = scene_results[(scene_results[SceneResultsConsts.DENOMINATOR_ID] == google_fk) &
                                   (scene_results[SceneResultsConsts.SCENE_FK].isin(self.chosen_scenes[exit_entry]))]
        google_sum = comparison[SceneResultsConsts.NUMERATOR_RESULT].sum()
        score = self.get_percentage(google_sum, apple_sum, allow_0_in_den=True)
        kpi_fk = self.get_kpi_fk_by_kpi_name(
            Consts.KPIS_DICT[Consts.SHARE_OF_COMPETITOR][Consts.STORE_LEVEL][exit_entry])
        self.write_to_db(
            fk=kpi_fk, result=total_facings, numerator_id=google_fk, denominator_id=apple_fk, score=score,
            numerator_result=google_sum, denominator_result=apple_sum)

# Missing Denominations

    def get_missing_denominations_store(self):
        if self.insert_store_level:
            exit_required_scenes = []
            for fixture in self.required_fixtures:
                exit_required_scenes += fixture[Consts.SCENE_FKS][Consts.EXIT]
            scene_kpi_fk = self.get_kpi_fk_by_kpi_name(Consts.MISSING_DENOMINATIONS)
            session_kpi_fk = self.get_kpi_fk_by_kpi_name(Consts.MISSING_DENOMINATIONS_STORE)
            relevant_results = self.scene_results[
                (self.scene_results[SceneResultsConsts.KPI_LEVEL_2_FK] == scene_kpi_fk) &
                (self.scene_results[SceneResultsConsts.SCENE_FK].isin(exit_required_scenes))][
                [SceneResultsConsts.NUMERATOR_ID, SceneResultsConsts.RESULT]]
            results = relevant_results.groupby(SceneResultsConsts.NUMERATOR_ID).sum()[SceneResultsConsts.RESULT]
            for product_fk in results.keys():
                self.write_to_db(fk=session_kpi_fk, numerator_id=product_fk,
                                 result=results[product_fk], score=results[product_fk])

# helpers:

    @staticmethod
    def get_sum_of_nums_dens(scene_results):
        numerators = scene_results[SceneResultsConsts.NUMERATOR_RESULT].sum()
        denominators = scene_results[SceneResultsConsts.DENOMINATOR_RESULT].sum()
        return numerators, denominators

    @staticmethod
    def get_average(list_of_scores):
        if len(list_of_scores) > 0:
            list_of_scores[0] = float(list_of_scores[0])
            final_score = sum(list_of_scores) / len(list_of_scores)
        else:
            final_score = 0
        return final_score
