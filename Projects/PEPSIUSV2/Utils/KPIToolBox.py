
from Trax.Utils.Logging.Logger import Log
import pandas as pd

# from KPIUtils_v2.Utils.Consts.DataProvider import
# from KPIUtils_v2.Utils.Consts.DB import
# from KPIUtils_v2.Utils.Consts.GlobalConsts import
# from KPIUtils_v2.Utils.Consts.Messages import
# from KPIUtils_v2.Utils.Consts.Custom import
# from KPIUtils_v2.Utils.Consts.OldDB import


from Trax.Tools.ProfessionalServices.PsConsts.DB import SessionResultsConsts as Src
from KPIUtils_v2.Utils.GlobalScripts.Scripts import GlobalSessionToolBox
from KPIUtils_v2.Calculations.AssortmentCalculations import Assortment
from Projects.PEPSIUSV2.Data.LocalConsts import Consts as Lc

__author__ = 'idanr'


class ToolBox(GlobalSessionToolBox):

    def __init__(self, data_provider, output):
        GlobalSessionToolBox.__init__(self, data_provider, output)
        self.assortment = Assortment(data_provider)
        self.match_display_in_scene = data_provider.match_display_in_scene
        self._add_display_data_to_scif()

    def main_calculation(self):
        self._calculate_display_compliance()
        self._calculate_linear_sos_compliance()
        self.commit_results()

    def _add_display_data_to_scif(self):
        """ This method adds the relevant display pk and name to Scene Item Facts.
        Every scene should have exactly one Display tagged in."""
        display_data = self.match_display_in_scene.drop_duplicates('scene_fk')
        self.scif = self.scif.merge(display_data, on='scene_fk', how='left')

    def _calculate_display_compliance(self):
        """
        This method calculates the Display Compliance KPI that is based on the assortment calculation.
        First, it filtered scif by the relevant display and then calculates the assortment KPIs.
        Please note that the logic is different than the assortment itself, 5 facings are enough to pass.
        """
        pallet_group_results = self._calculate_assortment_by_display(Lc.PALLET_DISPLAY)
        end_cap_group_results = self._calculate_assortment_by_display(Lc.END_CAP_DISPLAY)
        group_results = pallet_group_results.append(end_cap_group_results)
        self._calculate_display_compliance_store_result(group_results)

    def _filter_lvl3_results_by_pallet(self, lvl3_results, display_name):
        """ Every assortment to product has an additional attribute with the pallet the client expect it to be.
        So this method filters the lvl3_results by the relevant display name."""
        lvl3_results[Lc.DISPLAY_NAME] = lvl3_results[Lc.ASSORTMENT_ATTR].map(lambda x: eval(x)[Lc.DISPLAY_TYPE])
        lvl3_results = lvl3_results.loc[lvl3_results[Lc.DISPLAY_NAME] == display_name]
        filtered_match_display_in_scene = self.match_display_in_scene[[Lc.DISPLAY_FK, Lc.DISPLAY_NAME]]
        lvl3_results = lvl3_results.merge(filtered_match_display_in_scene, on=Lc.DISPLAY_NAME, how='left')
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
        self._set_kpi_df_identifiers(results_df, kpi_id, parent_id)  # todo: warning
        results_df.rename(cols_rename_dict, inplace=True, axis=1)
        results_df[Src.SCORE] = results_df.loc[:, Src.RESULT]  # todo: warning
        results_df = results_df.assign(denominator_result=denominator_res)
        self.common.save_json_to_new_tables(results_df.to_dict('records'))

    def _save_sos_results(self, sos_res_df, sos_lvl):
        """
        This method converts the SOS DataFrame to the expected DB results.
        :param sos_res_df: A group SOS DataFrame by manufacturer or brand
        :param sos_lvl: A const with the relevant SOS lvl the being saved. It affects the consts and identifiers.
        """
        cols_rename_dict, kpi_identifier, parent_identifier = self._get_sos_consts_by_level(sos_lvl)
        self._set_kpi_df_identifiers(sos_res_df, kpi_identifier, parent_identifier)  # todo: warning
        results_df = sos_res_df.rename(cols_rename_dict, inplace=False, axis=1)
        results_df[Src.SCORE] = results_df.apply(
            lambda row: self.get_percentage(row.numerator_result, row.denominator_result), axis=1)
        results_df[Src.RESULT] = results_df.loc[:, Src.SCORE]
        self.common.save_json_to_new_tables(results_df.to_dict('records'))

    def _set_kpi_df_identifiers(self, results_df, kpi_id_cols, parent_id_cols):
        """ This method gets assortment or SIS results and consts with the columns that should be the
        KPI and parent identifiers. It creates the identifier_result and parent_result and adds the
        `Should Enter` columns if necessary"""
        valid_cols = results_df.columns.unique().to_list()
        if kpi_id_cols and set(kpi_id_cols).issubset(valid_cols):
            results_df.loc[:, 'identifier_result'] = results_df.apply(
                lambda row: self._get_assortment_identifier(row, kpi_id_cols), axis=1)
        if parent_id_cols and set(parent_id_cols).issubset(valid_cols):
            results_df.loc[:, 'identifier_parent'] = results_df.apply(
                lambda row: self._get_assortment_identifier(row, parent_id_cols), axis=1)
            results_df.loc[:, 'should_enter'] = True

    @staticmethod
    def _get_assortment_identifier(row, columns_to_concat):
        """
        This method defines an identifier based on the relevant columns.
        Please note! The columns must by instance of Integer or Float.
        """
        return '_'.join([str(int(row[col])) if isinstance(row[col], (int, float)) else '' for col in columns_to_concat])

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

    def _calculate_display_compliance_store_level(self, group_res):
        """ This method gets the group results and returns a DataFrame with the Store results"""
        pass

    def _calculate_assortment_by_display(self, display_name):
        """This method gets the display name and calculates the assortment for all of the SKUs in this display"""
        filtered_scif = self.scif.loc[self.scif.display_name == display_name]
        if filtered_scif.empty:
            return pd.DataFrame()
        sku_lvl_results = self._get_filtered_assortment_lvl3_results(filtered_scif, display_name)
        group_lvl_results = self._calculate_display_compliance_group_results(sku_lvl_results)
        self._save_assortment_results(sku_lvl_results, Lc.SKU_LVL)
        self._save_assortment_results(group_lvl_results, Lc.GROUP_LVL)
        return group_lvl_results

    def _calculate_linear_sos_compliance(self):
        """ This method calculates the linear sos kpi compliance.
        There are three KPI sets, each per category (Tea, CSD, Energy).
        Every set has three levels: 1. Own manufacturer vs Target, 2. All manufacturer in category,
        3. brand in category.
        """
        sos_results = []
        sos_results.extend(self._calculate_linear_sos_by_category(Lc.CSD_CAT))
        sos_results.extend(self._calculate_linear_sos_by_category(Lc.TEA_CAT))
        sos_results.extend(self._calculate_linear_sos_by_category(Lc.ENERGY_CAT))
        return sos_results

    def _calculate_linear_sos_by_category(self, category_name):
        """This method gets the category name and calculate the entire set.
        The calculation considers ONLY SKUs and tasks that are relevant to this category!"""
        relevant_scenes = self._get_relevant_scene_per_category(category_name)
        filtered_scif = self.scif.loc[(self.scif.category == category_name) & self.scif.scene_fk.isin(relevant_scenes)]
        category_fk = self._get_category_fk_by_name(category_name)
        if filtered_scif.empty or not category_fk:
            return []
        self._calculate_linear_sos_results(filtered_scif, category_fk, category_name)

    def _get_category_fk_by_name(self, category_name):
        """ This method gets category name and returns the relevant fk. If doesn't exist, it returns 0"""
        category_fk = self.all_products.loc[self.all_products.category == category_name]['category_fk'].unique()
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
        self._calculate_manufacturers_sos(filtered_scif, category_fk)
        self._calculate_brands_sos(filtered_scif, category_fk)

    def _calculate_own_manufacturer_vs_target(self, filtered_scif, category_fk, category_name):
        """
        This method calculates Pepsi linear SOS and compares it to the target of the store
        :return: A DataFrame with one row of SOS results
        """
        own_manufacturer_scif = filtered_scif.loc[filtered_scif.manufacturer_fk == self.manufacturer_fk]
        store_target = self.store_info.additional_attribute_1.values[0]  # Todo: ???
        sum_of_linear_sos = own_manufacturer_scif['net_len_ign_stack'].sum()
        score, result = self._calculate_sos_vs_target_score_and_result(sum_of_linear_sos, store_target)
        kpi_fk = self._get_sos_kpi_fk_by_category_and_lvl(category_name, Lc.SOS_OWN_MANU_LVL)
        self.write_to_db(fk=kpi_fk, numerator_id=self.manufacturer_fk, denominator_id=self.store_id,
                         numerator_result=sum_of_linear_sos, denominator_result=store_target, context_id=category_fk,
                         score=score, result=result, identifier_result=category_fk)

    def _calculate_manufacturers_sos(self, filtered_scif, category_fk):
        """
        This method calculates all of the manufacturers linear sos results
        """
        manufacturers_res_df = self._calculate_general_sos_df_by_attr(filtered_scif, category_fk, 'manufacturer_fk')
        self._save_sos_results(manufacturers_res_df, sos_lvl=Lc.SOS_ALL_MANU_LVL)

    def _calculate_brands_sos(self, filtered_scif, cat_fk):
        """
        This method calculates all of the manufacturers linear sos results
        """
        brands_res_df = self._calculate_general_sos_df_by_attr(filtered_scif, cat_fk, ['brand_fk', 'manufacturer_fk'])
        self._save_sos_results(brands_res_df, sos_lvl=Lc.SOS_ALL_BRAND_LVL)

    @staticmethod
    def _calculate_general_sos_df_by_attr(filtered_scif, category_fk, attr_to_group_by):
        """
        This method gets the filtered scif and the entity to group by and it calculates the linear sos.
        And the end it adds to the results the category_fk and the total sos.
        :param filtered_scif: Filtered Scene item facts with SKU with the current category being calculated
        :param category_fk: current category_fk that is being calculated now (CSD / TEA / ENERGY)
        :param attr_to_group_by: attribute to group the scif by E.g: 'manufacturer_fk' / 'brand_fk'
        :return: A DataFrame with 4 columns: `entity_to_group_by`, net_len_ign_stack, total_linear_sos, category_fk
        """
        results_df = filtered_scif.groupby(attr_to_group_by, as_index=False)['net_len_ign_stack'].sum()
        total_linear_sos = results_df['net_len_ign_stack'].sum()
        results_df = results_df.assign(category_fk=category_fk, total_linear_sos=total_linear_sos)
        return results_df

    def _calculate_sos_vs_target_score_and_result(self, sum_of_linear_sos, store_target):
        """ This method gets the own manufacturer sos sum and the store target and calculates the score and result.
        The score is basically numerator / denominator and the results is 100 if the target was passed"""
        score = self.get_percentage(sum_of_linear_sos, store_target)
        result = 100 if sum_of_linear_sos >= store_target else 0
        return score, result

    def _get_sos_kpi_fk_by_category_and_lvl(self, category_name, lvl):
        """This method gets a category name and sos kpi lvl and returns the relevant kpi fk"""
        general_kpi_name = Lc.MAPPER_KPI_LVL_AND_NAME[lvl]
        # Todo: add here another check
        # Todo: add here another check
        # Todo: add here another check
        # Todo: add here another check
        # Todo: add here another check
        kpi_fk = self.get_kpi_fk_by_kpi_type(general_kpi_name.format(category_name))
        return kpi_fk

    def _get_relevant_scene_per_category(self, category_name):
        """ This method returns the relevant scenes to consider in the sos calculation"""
        # Todo
        return self.scene_info.scene_fk.unique().tolist()
