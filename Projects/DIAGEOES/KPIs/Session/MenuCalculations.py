from Trax.Algo.Calculations.Core.KPI.UnifiedKPICalculation import UnifiedCalculationsScript
from Projects.DIAGEORU.KPIs.util import DiageoUtil
from KPIUtils_v2.Utils.Consts.DataProvider import ProductsConsts, ScifConsts


class MenuCalculations(UnifiedCalculationsScript):

    def __init__(self, data_provider, config_params=None, **kwargs):
        super(MenuCalculations, self).__init__(data_provider, config_params=config_params, **kwargs)
        self.util = DiageoUtil(data_provider)

    def kpi_type(self):
        # ???ASD?aSD
        return'asevel'

    def calculate(self):
        """
        this class calculates all the menu KPI's depending on the parameter given
        """
        #add config params
        res_list = list()
        category_fk = None
        identifier_parent = None
        if self.util.menu is None:
            self.util.Log.warning("Menu class was not initialized for this project")
            return res_list

        if self._config_params['type'] == 'Manufacturer Level':
            menu_fk = self.util.commonV2.get_kpi_fk_by_kpi_name('Menu - Manufacturer Level')
            category_fk = self.util.menu.get_category_cocktail_fk()
            func = self.util.menu.get_dfs_dict_of_cocktails()
        elif self._config_params['type'] == 'Manufacturer in Sub_Category':
            menu_fk = self.util.commonV2.get_kpi_fk_by_kpi_name('Menu - Manufacturer in Sub_Category')
            func = self.util.menu.get_dfs_dict_of_cocktails_manufacturer_in_sub_cat()
        elif self._config_params['type'] == 'SKU Level':
            menu_fk = self.util.commonV2.get_kpi_fk_by_kpi_name('Menu - SKU Level')
            func = self.util.menu.get_dfs_dict_of_menus_product_level()
            category_fk = self.util.menu.get_category_cocktail_fk()
            identifier_parent = True
        else:
            self.util.Log.warning("Incorrect Kpi chosen")
            return res_list

        Scene_list = self.util.scenes_info[self.util.scenes_info['status'] == 6]['scene_fk'].unique().tolist()

        for scene_fk in Scene_list:
            cocktails_dict_manufacturer = func()[scene_fk]
            for i, line in cocktails_dict_manufacturer.iterrows():
                identifier_parent = {ProductsConsts.BRAND_FK: line[ProductsConsts.BRAND_FK],
                                     ProductsConsts.CATEGORY_FK: category_fk} if identifier_parent else None
                res_list.append(self.util.build_dictionary_for_db_insert_v2(
                    fk=menu_fk, numerator_id=line[ProductsConsts.MANUFACTURER_FK],
                    numerator_result=line[ScifConsts.FACINGS],
                    result=line['ratio'], denominator_id=category_fk if category_fk else
                    line[ProductsConsts.SUB_CATEGORY_FK],
                    denominator_result=line['total_facings'], identifier_parent=identifier_parent,
                    score=line['ratio']))

        for res in res_list:
            self.write_to_db_result(**res)
