from Trax.Algo.Calculations.Core.KPI.UnifiedKPICalculation import UnifiedCalculationsScript
from KPIUtils.GlobalProjects.DIAGEO.Utils.Consts import DiageoKpiNames
from Projects.DIAGEORU.KPIs.util import DiageoUtil
from KPIUtils_v2.Utils.Consts.DataProvider import MatchesConsts, ProductsConsts
from Projects.DIAGEORU.KPIs.util import DiageoConsts


class DiageoShelfPlacementSKU(UnifiedCalculationsScript):

    def __init__(self, data_provider, config_params=None, **kwargs):
        super(DiageoShelfPlacementSKU, self).__init__(data_provider, config_params=config_params, **kwargs)
        self.util = DiageoUtil(data_provider)

    def kpi_type(self):
        return DiageoKpiNames.VERTICAL_SHELF_PLACEMENT

    def handle_unusual_shelf_num(self, shelf_num_from_bottom):
        """
        This method handles cases with number of shelves that wasn't defined in the template (big ones)
        :param shelf_num_from_bottom: The top shelf number from bottom in the scene
        :return: List with the shelf map according to the Consts.MINIMUM_SHELF_TARGETS + gap filled
        """
        max_shelf_in_dict = max(DiageoConsts.MINIMUM_SHELF_TARGETS)
        res = DiageoConsts.MINIMUM_SHELF_TARGETS[max_shelf_in_dict][:]    # Copy the list
        res.extend([res[-1]] * (shelf_num_from_bottom - max_shelf_in_dict))   # Adding the gap
        return res

    def get_shelf_mapping_for_scenes(self):
        """
        this function maps all relevant scenes to the relevant shelf placement
        :return: shelf_mapping : a dictionary with the scene_fk as key and the shelf placement template as a val
        """
        top_shelves = self.util.match_product_in_scene.groupby(MatchesConsts.SCENE_FK)['shelf_number_from_bottom'].max()  # get the height of the top shelf in each scene
        max_shelf = top_shelves.max()  # get the maximum shelf
        top_shelves = top_shelves.to_dict()
        for scene, top_shelf in top_shelves.iteritems():
            if top_shelf not in DiageoConsts.MINIMUM_SHELF_TARGETS:
                top_shelves[scene] = self.handle_unusual_shelf_num(top_shelf)
            else:
                top_shelves[scene] = DiageoConsts.MINIMUM_SHELF_TARGETS[top_shelf]
        return top_shelves, max_shelf

    def row_values_validation(self, entity_type, value, target,max_shelf):
        """
        This method validates every row in the shelf placement data. At first it checks if the values exist in the DB
        and later on it checks if the value can be found in the visit.
        :return: If everything is ok True, otherwise, False.
        """
        valid_values = True
        # Sanity checks for the data
        if not entity_type or not value or not target:
            valid_values = False
        elif int(target) > max_shelf:
            valid_values = False
        elif self.util.all_products.loc[self.util.all_products[entity_type] == value].empty:
            valid_values = False
        if not valid_values:
            self.util.Log.warning(DiageoConsts.WRONG_VALUES_IN_THE_TEMPLATE.format(entity_type, value, target))
            return False
        # Check if the entity & value can be found in this visit
        return not self.util.scif.loc[self.util.scif[entity_type] == value].empty

    def calculate_shelf_placement_per_row(self, entity_type, value, target_shelf_group, shelves_map_per_scene):
        """
        this function decides for all entities in the session what their shelf placement is and if it fits the
        target, one entity that passes in the session is enough for a passing grade(per entity type and value given).
        :param entity_type: The relevant entity. E.g: product_ean_code
        :param value: The entity's value.
        :param target_shelf_group: The locations which the product suppose to be in.
        :param shelves_map_per_scene: Location's mapping per scene. E.g: {123: ['B',E'], 234: ['B','E','E','T']}
        :return: A tuple (passed/Failed, location(of the first entity )). E.g: (100, 'E') or (0, 'T').
        """
        entity_type = MatchesConsts.PRODUCT_FK if entity_type == ProductsConsts.PRODUCT_EAN_CODE else entity_type
        value = int(value) if entity_type in [MatchesConsts.PRODUCT_FK] else value
        filtered_matches = self.util.match_product_in_scene.loc[self.util.match_product_in_scene[entity_type] == value][
            [entity_type, MatchesConsts.SCENE_FK, DiageoConsts.SHELF_NUM_FROM_BOTTOM]]  # find relevant entities
        filtered_matches[DiageoConsts.SHELF_NUM_FROM_BOTTOM] = filtered_matches.apply(
            lambda x: shelves_map_per_scene[x[MatchesConsts.SCENE_FK]][x[DiageoConsts.SHELF_NUM_FROM_BOTTOM] - 1],
            axis=1)  # match location to letter (note that the shelf num column is now a letter)
        results = filtered_matches[filtered_matches[DiageoConsts.SHELF_NUM_FROM_BOTTOM].isin(target_shelf_group)]  # check if placement is in the target
        if not results.empty:
            return 100, results[DiageoConsts.SHELF_NUM_FROM_BOTTOM].iloc[0]  # at least one entity passed so just the first one that did
        else:
            return 0, filtered_matches[DiageoConsts.SHELF_NUM_FROM_BOTTOM].iloc[0]  # no entities are correctly positioned so the first ones location

    def calculate(self):
        """
        This function calculates the global vertical shelf placement KPI.
        Explanation: The relevant template is uploaded to the S3 and is being downloaded during the calculation.
        There are required columns for the template: 'entity type', 'entity value' and 'location'.
        """
        template_data = self.util.get_template_data(DiageoKpiNames.VERTICAL_SHELF_PLACEMENT)
        vsp_kpi_fk = self.util.commonV2.get_kpi_fk_by_kpi_type(DiageoKpiNames.VERTICAL_SHELF_PLACEMENT)  # get kpi fk
        res_list = []
        shelves_map_per_scene, max_shelf = self.get_shelf_mapping_for_scenes()
        if not shelves_map_per_scene or not template_data:
            self.util.Log.warning(DiageoConsts.EMPTY_DATA_LOG)
            return
        for row in template_data:  # get a score for each row in the template
            entity_type, value, target = row[DiageoConsts.ENTITY_TYPE], row[DiageoConsts.ENTITY_VALUE],\
                                         row[DiageoConsts.VSP_LOCATION]
            if not self.row_values_validation(entity_type, value, target, max_shelf):
                continue
            shelves_target_group = DiageoConsts.SHELVES_GROUPS_CONVERSION[int(target)]
            numerator_id = self.util.get_numerator_id_by_entity(entity_type, value)  # The relevant entity's fk
            kpi_fk = self.util.get_kpi_fk_by_entity_type(entity_type)
            result, location = self.calculate_shelf_placement_per_row(entity_type, numerator_id, shelves_target_group,
                                                                       shelves_map_per_scene)

            score_entity_fk, target_entity_fk = self.util.get_results_value_entities(location, shelves_target_group)
            res_list.append(self.util.build_dictionary_for_db_insert_v2(fk=kpi_fk, numerator_id=numerator_id,
                                                                   target=target_entity_fk, result=result,
                                                                   score=score_entity_fk, identifier_result=kpi_fk,
                                                                   should_enter=True, identifier_parent=vsp_kpi_fk))

        for res in res_list:
            self.write_to_db_result(**res)
