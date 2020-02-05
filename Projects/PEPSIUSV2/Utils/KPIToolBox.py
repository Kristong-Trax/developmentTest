
from Trax.Tools.ProfessionalServices.PsConsts.DB import SessionResultsConsts as Src
from Trax.Tools.ProfessionalServices.PsConsts.DataProvider import ScifConsts as Sc
from KPIUtils_v2.Utils.GlobalScripts.Scripts import GlobalSessionToolBox
from KPIUtils_v2.Calculations.AssortmentCalculations import Assortment
from Projects.PEPSIUSV2.Data.LocalConsts import Consts as Lc
from Trax.Utils.Logging.Logger import Log
import pandas as pd

__author__ = 'idanr'


class PepsiUSV2ToolBox(GlobalSessionToolBox):

    def __init__(self, data_provider, output):
        GlobalSessionToolBox.__init__(self, data_provider, output)
        self.assortment = Assortment(data_provider)
        self.display_in_scene = data_provider.match_display_in_scene
        self.static_display = data_provider.static_display
        self.manufacturer_fk = int(self.manufacturer_fk)
        self._add_display_data_to_scif()

    def main_calculation(self):
        self._calculate_display_compliance()
        self._calculate_sos_sets()
        self.commit_results()

    def _add_display_data_to_scif(self):
        """ This method adds the relevant display pk and name to Scene Item Facts.
        Every scene should have exactly one Display tagged in."""
        display_data = self.display_in_scene.drop_duplicates(Sc.SCENE_FK)[['display_fk', 'display_name', Sc.SCENE_FK]]
        self.scif = self.scif.merge(display_data, on=Sc.SCENE_FK, how='left')

    def _calculate_display_compliance(self):
        """
        This method calculates the Display Compliance KPI that is based on the assortment calculation.
        First, it filtered scif by the relevant display and then calculates the assortment KPIs.
        Please note that the logic is different than the assortment itself, 5 facings are enough to pass.
        """
        pallet_group_results = self._calculate_assortment_by_display(Lc.PALLET_DISPLAY)
        end_cap_group_results = self._calculate_assortment_by_display(Lc.END_CAP_DISPLAY)
        group_results = pallet_group_results.append(end_cap_group_results)
        if not group_results.empty:
            self._calculate_display_compliance_store_result(group_results)

    def _filter_lvl3_results_by_pallet(self, lvl3_results, display_name):
        """ Every assortment to product has an additional attribute with the pallet the client expect it to be.
        So this method filters the lvl3_results by the relevant display name."""
        lvl3_results[Lc.DISPLAY_NAME] = lvl3_results[Lc.ASSORTMENT_ATTR].map(lambda x: eval(x)[Lc.DISPLAY_TYPE])
        lvl3_results = lvl3_results.loc[lvl3_results[Lc.DISPLAY_NAME] == display_name]
        display_static_data = self.static_display[['pk', Lc.DISPLAY_NAME]].rename({'pk': Lc.DISPLAY_FK}, axis=1)
        lvl3_results = lvl3_results.merge(display_static_data, on=Lc.DISPLAY_NAME, how='left')
        return lvl3_results

    def _calculate_display_compliance_store_result(self, group_results):
        """ This method aggregates the group results and calculates the store result for display compliance"""
        passed_groups, total_groups = group_results.result.sum() / float(100), len(group_results)
        score = self.get_percentage(passed_groups, total_groups)
        kpi_fk = self.common.get_kpi_fk_by_kpi_type(Lc.DISPLAY_COMP_STORE_LEVEL_FK)
        self.write_to_db(fk=kpi_fk, numerator_id=self.manufacturer_fk, denominator_id=self.store_id,
                         numerator_result=passed_groups, denominator_result=total_groups,
                         score=score, result=score, identifier_result=self.manufacturer_fk)

    def _get_filtered_assortment_lvl3_results(self, filtered_scif, display_name):
        """ This method calculates the lvl3 results and filters only the relevant results for the
        current display_name"""
        lvl3_results = self.assortment.calculate_lvl3_assortment_by_filtered_scif(filtered_scif)
        lvl3_results = self._filter_lvl3_results_by_pallet(lvl3_results, display_name)
        lvl3_results['in_store'] = lvl3_results['in_store'].apply(lambda res: res * 100)
        return lvl3_results

    def _save_assortment_results(self, assortment_res, assort_lvl):
        """
        This method converts the assortment DataFrame to the expected DB results.
        """
        cols_to_save, cols_rename_dict, denominator_res, kpi_id, parent_id = self._get_ass_consts_by_level(assort_lvl)
        results_df = assortment_res[cols_to_save]
        self._set_kpi_df_identifiers(results_df, kpi_id, parent_id, assort_lvl, Lc.ASSORTMENT_ID_SUFFIX)  # todo: warn
        results_df.rename(cols_rename_dict, inplace=True, axis=1)
        results_df[Src.SCORE] = results_df.loc[:, Src.RESULT]  # todo: warning
        results_df = results_df.assign(denominator_result=denominator_res)
        self.common.save_json_to_new_tables(results_df.to_dict('records'))

    def _save_sos_results(self, sos_res_df, sos_lvl, category_name, suffix_identifier):
        """
        This method converts the SOS DataFrame to the expected DB results.
        :param sos_res_df: A group SOS DataFrame by manufacturer or brand
        :param sos_lvl: A const with the relevant SOS lvl the being saved. It affects the consts and identifiers.
        """
        cols_rename_dict, kpi_identifier, parent_identifier = self._get_sos_consts_by_level(sos_lvl)
        self._set_kpi_df_identifiers(sos_res_df, kpi_identifier, parent_identifier, sos_lvl, suffix_identifier)
        results_df = sos_res_df.rename(cols_rename_dict, inplace=False, axis=1)
        results_df[Src.SCORE] = results_df.apply(
            lambda row: self.get_percentage(row.numerator_result, row.denominator_result), axis=1)
        results_df[Src.RESULT] = results_df[Src.SCORE]
        results_df['fk'] = self._get_sos_kpi_fk_by_category_and_lvl(category_name, sos_lvl, suffix_identifier)
        self.common.save_json_to_new_tables(results_df.to_dict('records'))

    def _set_kpi_df_identifiers(self, results_df, kpi_id_cols, parent_id_cols, kpi_level, suffix_to_add=''):
        """ This method gets assortment or SIS results and consts with the columns that should be the
        KPI and parent identifiers. It creates the identifier_result and parent_result and adds the
        `Should Enter` columns if necessary"""
        valid_cols = results_df.columns.unique().to_list()
        if kpi_id_cols and set(kpi_id_cols).issubset(valid_cols):
            results_df.loc[:, 'identifier_result'] = results_df.apply(
                lambda row: self._get_assortment_identifier(row, kpi_id_cols, kpi_level, suffix_to_add), axis=1)
        if parent_id_cols and set(parent_id_cols).issubset(valid_cols):
            results_df.loc[:, 'identifier_parent'] = results_df.apply(
                lambda row: self._get_assortment_identifier(row, parent_id_cols, kpi_level-1, suffix_to_add), axis=1)
            results_df.loc[:, 'should_enter'] = True

    @staticmethod
    def _get_assortment_identifier(row, cols_to_concat, kpi_level, additional_suffix=''):
        """
        This method defines an identifier based on the relevant columns.
        Please note! The columns must by instance of Integer or Float.
        :param cols_to_concat: keys of the series (the row)
        :param kpi_level: The relevant kpi level to save
        :param additional_suffix (Str): Additional Suffix to add.
        @:return A concatenation of the columns' values and the additional_suffix.
        E.g: if additional_suffix='SOS' and cols_to_concat = ['brand_fk'] we can get: '7_sos'.
        """
        res_id = '_'.join([str(int(row[col])) if isinstance(row[col], (int, float)) else '' for col in cols_to_concat])
        res_id = '_'.join([res_id, str(additional_suffix), str(kpi_level)]) if additional_suffix else res_id
        return res_id

    @staticmethod
    def _get_sos_consts_by_level(sos_lvl):
        """This method gets the SOS lvl (SKU / GROUP / STORE) and returns a tuple of 5 consts:
        Column to save, rename dict, denominator_result, kpi identifier cols, parent identifier columns
        """
        if sos_lvl == Lc.SOS_ALL_BRAND_LVL:
            return Lc.BRAND_SOS_RENAME_DICT, None, Lc.SOS_ALL_MANU_ID
        elif sos_lvl == Lc.SOS_ALL_MANU_LVL:
            return Lc.ALL_MANU_SOS_RENAME_DICT, Lc.SOS_ALL_MANU_ID, Lc.SOS_OWN_MANU_ID

    @staticmethod
    def _get_ass_consts_by_level(assortment_lvl):
        """This method gets the assortment lvl (SOS_ALL_MANU_LVL / SOS_ALL_BRAND_LVL) and returns a tuple of 3 consts:
        rename dict, kpi identifier cols, parent identifier columns
        """
        if assortment_lvl == Lc.SKU_LVL:
            return Lc.SKU_COLS_TO_SAVE, Lc.SKU_COLS_RENAME, 1, None, Lc.GROUP_IDE
        elif assortment_lvl == Lc.GROUP_LVL:
            return Lc.GROUP_COLS_TO_KEEP, Lc.GROUP_COLS_RENAME, Lc.GROUP_FACING_TARGET, Lc.GROUP_IDE, Lc.STORE_IDE

    def _calculate_display_compliance_group_results(self, lvl3_results):
        """ This method gets the filtered lvl3 results and calculates the final results for every group."""
        group_res = lvl3_results.groupby(['kpi_fk_lvl2', 'assortment_group_fk', 'display_fk'], as_index=False).sum()
        group_res[Src.RESULT] = group_res.loc[:, 'facings'].apply(
            lambda res: 100 if res >= Lc.GROUP_FACING_TARGET else 0)
        group_res = group_res.assign(manufacturer_fk=self.manufacturer_fk)
        return group_res

    def _calculate_assortment_by_display(self, display_name):
        """This method gets the display name and calculates the assortment for all of the SKUs in this display"""
        filtered_scif = self.scif.loc[self.scif.display_name == display_name]
        sku_lvl_results = self._get_filtered_assortment_lvl3_results(filtered_scif, display_name)
        if not sku_lvl_results.empty:
            group_lvl_results = self._calculate_display_compliance_group_results(sku_lvl_results)
            self._save_assortment_results(sku_lvl_results, Lc.SKU_LVL)
            self._save_assortment_results(group_lvl_results, Lc.GROUP_LVL)
            return group_lvl_results
        return pd.DataFrame()

    def _calculate_sos_sets(self):
        """ This method calculates the linear and the facings SOS kpi sets.
        Each of them has three KPI sets, each per category (Tea, CSD, Energy).
        Both has: 1.  All manufacturer in category and brand in category KPIs, while the Linear has an additional
        own manufacturer sos vs target and the facings regular own manufacturer in store.
        """
        sos_results = []
        sos_results.extend(self._calculate_linear_and_facings_sos_by_category(Lc.CSD_CAT))
        sos_results.extend(self._calculate_linear_and_facings_sos_by_category(Lc.TEA_CAT))
        sos_results.extend(self._calculate_linear_and_facings_sos_by_category(Lc.ENERGY_CAT))
        self.common.save_json_to_new_tables(sos_results)

    def _calculate_linear_and_facings_sos_by_category(self, category_name):
        """This method gets the category name and calculate the entire set.
        The calculation considers ONLY SKUs and tasks that are relevant to this category!"""
        relevant_scenes = self._get_relevant_scene_per_category(category_name)
        filtered_scif = self.scif.loc[(self.scif.category == category_name) & self.scif.scene_fk.isin(relevant_scenes)]
        category_fk = self._get_category_fk_by_name(category_name)
        if filtered_scif.empty or not category_fk:
            return []
        self._calculate_linear_sos_results(filtered_scif, category_fk, category_name)
        self._calculate_facings_sos_results(filtered_scif, category_fk, category_name)

    def _get_category_fk_by_name(self, category_name):
        """ This method gets category name and returns the relevant fk. If doesn't exist, it returns 0"""
        category_fk = self.all_products.loc[self.all_products.category == category_name][Sc.CATEGORY_FK].unique()
        if not category_fk:
            Log.error(Lc.WRONG_CATEGORY_LOG.format(category_name))
            return 0
        return category_fk[0]

    def _calculate_linear_sos_results(self, filtered_scif, category_fk, category_name):
        """
        This method gets the filtered scif by category and return the SOS results.
        There are three KPIs: 1. Own Manufacturer vs target, 2. All manufacturers in store, all brands in store
        """
        self._calculate_own_manufacturer_vs_target(filtered_scif, category_fk, category_name)
        self._calculate_manufacturers_sos(filtered_scif, category_fk, category_name, sos_attr=Lc.SOS_LINEAR_LEN_ATTR)
        self._calculate_brands_sos(filtered_scif, category_fk, category_name, sos_attr=Lc.SOS_LINEAR_LEN_ATTR)

    def _calculate_facings_sos_results(self, filtered_scif, category_name, category_fk):
        """
        This method gets the filtered scif by category and return the Facings SOS results.
        There are three KPIs: 1. Own Manufacturer in store, 2. All manufacturers in store, all brands in store
        """
        self._calculate_brands_sos(filtered_scif, category_fk, category_name, sos_attr=Lc.SOS_FACINGS_ATTR)
        self._calculate_manufacturers_sos(filtered_scif, category_fk, category_name, sos_attr=Lc.SOS_FACINGS_ATTR)
        self._calculate_own_manufacturer_facings_sos(filtered_scif, category_fk)

    def _calculate_own_manufacturer_facings_sos(self, filtered_scif, category_fk):
        """ This method calculates the own manufacturer facings sos result """
        total_cat_facings = filtered_scif[Lc.SOS_FACINGS_ATTR].sum()
        own_manufacturer_scif = filtered_scif.loc[filtered_scif.manufacturer_fk == self.manufacturer_fk]
        own_manu_facings = own_manufacturer_scif[Lc.SOS_FACINGS_ATTR].sum()
        score = self.get_percentage(own_manu_facings, total_cat_facings)
        kpi_fk = self.get_kpi_fk_by_kpi_type(Lc.FACINGS_SOS_STORE_LEVEL_KPI)
        kpi_identifier = '_'.join([str(category_fk),  Lc.FACINGS_ID_SUFFIX])
        self.write_to_db(fk=kpi_fk, numerator_id=self.manufacturer_fk, denominator_id=self.store_id,
                         numerator_result=own_manu_facings, denominator_result=total_cat_facings,
                         context_id=category_fk, score=score, result=score, identifier_result=kpi_identifier)

    def _calculate_own_manufacturer_vs_target(self, filtered_scif, category_fk, category_name):
        """
        This method calculates Pepsi linear SOS and compares it to the target of the store
        :return: A DataFrame with one row of SOS results
        """
        # Todo: suggest Tim that target will be saved in target
        # Todo: store target !! currently mocked
        own_manufacturer_scif = filtered_scif.loc[filtered_scif.manufacturer_fk == self.manufacturer_fk]
        store_target = self.store_info.additional_attribute_1.values[0] if 0 else 11000  # Todo: Missing store target
        own_manu_sos = own_manufacturer_scif[Lc.SOS_LINEAR_LEN_ATTR].sum()
        score, result = self._calculate_sos_vs_target_score_and_result(own_manu_sos, store_target)
        kpi_fk = self._get_sos_kpi_fk_by_category_and_lvl(category_name, Lc.SOS_OWN_MANU_LVL, Lc.LINEAR_ID_SUFFIX)
        kpi_identifier = '_'.join([str(category_fk),  Lc.LINEAR_ID_SUFFIX])
        self.write_to_db(fk=kpi_fk, numerator_id=self.manufacturer_fk, denominator_id=self.store_id,
                         numerator_result=own_manu_sos, denominator_result=store_target, context_id=category_fk,
                         score=score, result=result, identifier_result=kpi_identifier)

    def _calculate_manufacturers_sos(self, filtered_scif, category_fk, cat_name, sos_attr):
        """
        This method calculates all of the manufacturers linear sos results
        """
        manu_res_df = self._calculate_sos_by_attr(filtered_scif, category_fk, Sc.MANUFACTURER_FK, sos_attr)
        kpi_suffix_identifier = Lc.FACINGS_ID_SUFFIX if sos_attr == Lc.SOS_FACINGS_ATTR else Lc.LINEAR_ID_SUFFIX
        self._save_sos_results(manu_res_df, Lc.SOS_ALL_MANU_LVL, cat_name, suffix_identifier=kpi_suffix_identifier)

    def _calculate_brands_sos(self, filtered_scif, cat_fk, cat_name, sos_attr):
        """
        This method calculates all of the manufacturers linear sos results
        """
        brands_res_df = self._calculate_sos_by_attr(filtered_scif, cat_fk, [Sc.BRAND_FK, Sc.MANUFACTURER_FK], sos_attr)
        kpi_suffix_identifier = Lc.FACINGS_ID_SUFFIX if sos_attr == Lc.SOS_FACINGS_ATTR else Lc.LINEAR_ID_SUFFIX
        self._save_sos_results(brands_res_df, Lc.SOS_ALL_BRAND_LVL, cat_name, suffix_identifier=kpi_suffix_identifier)

    @staticmethod
    def _calculate_sos_by_attr(filtered_scif, category_fk, attr_to_group_by, sos_attr):
        """
        This method gets the filtered scif and the entity to group by and it calculates the linear sos.
        And the end it adds to the results the category_fk and the total sos.
        :param filtered_scif: Filtered Scene item facts with SKU with the current category being calculated
        :param category_fk: current category_fk that is being calculated now (CSD / TEA / ENERGY)
        :param attr_to_group_by: attribute to group the scif by E.g: 'manufacturer_fk' / 'brand_fk'
        :param sos_attr: The SOS attribute to sum ('facings' or 'gross_len_ign_stack')
        :return: A DataFrame with 4 columns: `entity_to_group_by`, net_len_ign_stack, total_linear_sos, category_fk
        """
        results_df = filtered_scif.groupby(attr_to_group_by, as_index=False)[sos_attr].sum()
        total_sos = results_df[sos_attr].sum()
        results_df = results_df.assign(category_fk=category_fk, total_sos=total_sos)
        return results_df

    def _calculate_sos_vs_target_score_and_result(self, sum_of_linear_sos, store_target):
        """ This method gets the own manufacturer sos sum and the store target and calculates the score and result.
        The score is basically numerator / denominator and the results is 100 if the target was passed"""
        score = min(self.get_percentage(sum_of_linear_sos, store_target), 100)
        result = 100 if sum_of_linear_sos >= store_target else 0
        return score, result

    def _get_sos_kpi_fk_by_category_and_lvl(self, category_name, lvl, sos_attr):
        """This method gets a category name and sos kpi lvl and returns the relevant kpi fk"""
        general_kpi_name = Lc.MAPPER_KPI_LVL_AND_NAME[lvl]
        category_name_in_the_kpi = self._get_category_name_in_kpi(category_name)
        sos_attr_in_kpi = self._get_sos_attr_in_kpi(sos_attr)
        kpi_fk = self.get_kpi_fk_by_kpi_type(general_kpi_name.format(category_name_in_the_kpi, sos_attr_in_kpi))
        return kpi_fk

    @staticmethod
    def _get_category_name_in_kpi(category_name):
        """ The KPI names are having the category in it, this method returns the exact name that appears in the
        KPI. This method created so in case the category name will change, the kpi won't be affected."""
        upper_category_name = category_name.upper()
        if 'TEA' in upper_category_name:
            return 'Tea'
        elif 'ENERGY' in upper_category_name:
            return 'Energy'
        elif 'CSD' in upper_category_name:
            return 'CSD'
        else:
            Log.error(Lc.MISSING_KPI_FOR_CATEGORY.format(category_name))

    @staticmethod
    def _get_sos_attr_in_kpi(sos_attr):
        """ The KPI names are having the sos_attr in it, this method returns the exact name that appears in the
        KPI. This method created so in case the sos attribute will change, the kpi won't be affected."""
        upper_category_name = sos_attr.upper()
        if 'FACINGS' in upper_category_name:
            return 'Facings'
        else:
            return 'Linear'

    def _get_relevant_scene_per_category(self, category_name):
        """ This method returns the relevant scenes to consider in the sos calculation.
        The project team defines specific scene that we should consider them, and ONLY them.
        """
        relevant_task_names_lst = Lc.SCENE_CATEGORY_MAPPER[category_name]
        relevant_scenes_df = self.scif.loc[self.scif.template_name.isin(relevant_task_names_lst)]
        return relevant_scenes_df.scene_fk.unique().tolist()
