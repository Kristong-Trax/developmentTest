from Trax.Algo.Calculations.Core.Utils import ToolBox


class FlowTools(object):

    @staticmethod
    def progression(df, group_by, progression_list, progression_field, at_least_one=False, all_present=False,
                    left_to_right=False, cross_bays=False, cross_shelves=False, include_stacking=False):
        """

        :param df: :class:`pandas.DataFrame`
        :param group_by:
        :param progression_list:
        :param progression_field:
        :param at_least_one:
        :param all_present:
        :param left_to_right:
        :param cross_bays:
        :param cross_shelves:
        :param include_stacking:
        :return:
        :rtype:
        """
        grouped = df.groupby(by=group_by)
        match_sequences_per_group_list = list()
        for name, group_df in grouped:
            group_match_sequences = FlowTools.prepare_match_sequences(group_df, left_to_right, cross_bays, cross_shelves,
                                                                      include_stacking)
            match_sequences_per_group_list.append(group_match_sequences)

        progression_res = FlowTools._calculate_progression(match_sequences_per_group_list, progression_list,
                                                           progression_field, at_least_one, all_present)

        return progression_res

    @staticmethod
    def _calculate_progression(match_sequences_list, progression_list, progression_field, at_least_one, all_present):
        results_per_scene = list()
        for list_per_scene in match_sequences_list:
            field_value_list = ToolBox._get_field_value_from_match(progression_field, list_per_scene)
            numbers_list = ToolBox._convert_to_numbers(field_value_list, progression_list, 999)
            bool_list = ToolBox._are_numbers_in_sequence(numbers_list)
            all_numbers_are_in_sequence = all(bool_val for bool_val in bool_list)
            if at_least_one:
                all_non_wanted_list = ToolBox._all_not_in_progression(numbers_list, 999)
                all_non_wanted = all(bool_val for bool_val in all_non_wanted_list)
                bool_val = all_numbers_are_in_sequence and not all_non_wanted
            elif all_present:
                x = ToolBox._convert_list_to_numbers(progression_list, progression_list, 999)
                bool_list = ToolBox._are_all_numbers_in_sequence_strong(numbers_list, x)
                all_numbers_are_in_sequence_and_present = all(bool_val for bool_val in bool_list)
                bool_val = all_numbers_are_in_sequence_and_present
            else:
                bool_val = all_numbers_are_in_sequence
            results_per_scene.append(bool_val)
        final_res = all(scene_res for scene_res in results_per_scene)
        return final_res

    @staticmethod
    def prepare_match_sequences(df, left_to_right, cross_bays, cross_shelves, include_stacking):
        """

        :param df: :class:`pandas.DataFrame`
        :param Bool left_to_right:
        :param Bool cross_bays:
        :param Bool cross_shelves:
        :param Bool include_stacking:
        :return:
        :rtype:
        """
        main_list = list()
        to_sort = df
        if not include_stacking:
            to_sort = df.query('stacking_layer==1')
        if left_to_right:
            ascending_list = [True, True, True]
        else:
            ascending_list = [True, False, False]
        ord_by = 'shelf_number, bay_number, facing_sequence_number'
        order_by_fields = ord_by.strip().split(', ')

        ordered_matches = to_sort.sort_values(by=order_by_fields, ascending=ascending_list)

        current_shelf = None
        current_bay = None
        temp_list = list()
        for i, row in ordered_matches.iterrows():
            shelf_number = row['shelf_number']
            bay_number = row['bay_number']
            if current_shelf is None and current_bay is None:
                current_shelf = shelf_number
                current_bay = bay_number
            else:
                if not cross_shelves:
                    if shelf_number != current_shelf:
                        current_shelf = shelf_number
                        main_list.append(temp_list)
                        temp_list = list()
                    if bay_number != current_bay:
                        if not cross_bays and bay_number > current_bay:
                            main_list.append(temp_list)
                            temp_list = list()
                        current_bay = bay_number
            if not cross_shelves:
                temp_list.append(row)
            else:
                main_list.append(row)
        if not cross_shelves:
            if len(temp_list) > 0:
                main_list.append(temp_list)
        return main_list