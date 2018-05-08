
from Trax.Utils.Logging.Logger import Log


__author__ = 'nimrodp'


PNG_MANUFACTURER = 'Procter & Gamble'

KPI_RESULT = 'report.kpi_results'
KPK_RESULT = 'report.kpk_results'
KPS_RESULT = 'report.kps_results'


class PNGAUEmptySpaces:

    IRRELEVANT = 'Irrelevant'
    EMPTY = 'Empty'

    LEFT = -1
    RIGHT = 1

    LEVEL1 = 1
    LEVEL2 = 2
    LEVEL3 = 3

    def __init__(self, matches):
        self.matches = matches
        self.empty_spaces = {}

    def calculate_empty_spaces(self):
        empty_facings = self.matches[self.matches['product_type'] == self.EMPTY]
        if empty_facings.empty:
            Log.info('No empty spaces appeared in this visit')
        else:
            empty_sequences = self.get_empty_sequences(empty_facings)
            for sequence in empty_sequences:
                start_facing, end_facing, sequence = sequence
                anchor_for_all_empties = None
                if start_facing is None and end_facing is None:
                    continue
                elif start_facing is None:
                    anchor_for_all_empties = end_facing
                elif end_facing is None:
                    anchor_for_all_empties = start_facing
                if anchor_for_all_empties is not None:
                    self.update_empty_spaces(anchor_for_all_empties, len(sequence))
                else:
                    if len(sequence) == 1 and start_facing['manufacturer_name'] != PNG_MANUFACTURER and \
                                    end_facing['manufacturer_name'] == PNG_MANUFACTURER:
                        self.update_empty_spaces(end_facing, number_of_empties=1)
                    elif len(sequence) % 2 == 0:
                        self.update_empty_spaces(start_facing, number_of_empties=(len(sequence) / 2))
                        self.update_empty_spaces(end_facing, number_of_empties=(len(sequence) / 2))
                    else:
                        self.update_empty_spaces(start_facing, number_of_empties=(len(sequence) / 2 + 1))
                        self.update_empty_spaces(end_facing, number_of_empties=(len(sequence) / 2))

    def update_empty_spaces(self, anchor_facing, number_of_empties):
        anchor_type = anchor_facing['product_type']
        anchor_category = anchor_facing['category']
        anchor_manufacturer = anchor_facing['manufacturer_name']
        if anchor_category not in self.empty_spaces.keys():
            self.empty_spaces[anchor_category] = {'png': 0, 'other': 0, 'irrelevant': 0}
        if anchor_type == self.IRRELEVANT:
            self.empty_spaces[anchor_category]['irrelevant'] += number_of_empties
        elif anchor_manufacturer == PNG_MANUFACTURER:
            self.empty_spaces[anchor_category]['png'] += number_of_empties
        else:
            self.empty_spaces[anchor_category]['other'] += number_of_empties

    def get_empty_sequences(self, empty_facings):
        empty_sequences = []
        for scene in empty_facings['scene_fk'].unique():
            matches_for_scene = self.matches[self.matches['scene_fk'] == scene]
            empties_for_scene = empty_facings[empty_facings['scene_fk'] == scene]
            calculate_per_bay = self.get_scene_bay_status(matches_for_scene)
            empty_facings_already_calculated = []
            for f in xrange(len(empties_for_scene)):
                facing = empties_for_scene.iloc[f]
                if facing['scene_match_fk'] not in empty_facings_already_calculated:
                    start_facing = end_facing = None
                    empty_sequence = [facing]
                    left_facing = self.get_neighbour_facing(facing, matches_for_scene, self.LEFT, calculate_per_bay)
                    if left_facing is not None:
                        if left_facing['product_type'] == self.EMPTY:
                            continue
                        start_facing = left_facing
                    right_facing = self.get_neighbour_facing(facing, matches_for_scene, self.RIGHT, calculate_per_bay)
                    while right_facing is not None and right_facing['product_type'] == self.EMPTY:
                        empty_sequence.append(right_facing)
                        empty_facings_already_calculated.append(right_facing['scene_match_fk'])
                        right_facing = self.get_neighbour_facing(right_facing, matches_for_scene, self.RIGHT, calculate_per_bay)
                    if right_facing is not None:
                        end_facing = right_facing
                    empty_sequences.append((start_facing, end_facing, empty_sequence))
        return empty_sequences

    @staticmethod
    def get_neighbour_facing(anchor, matches, left_or_right=LEFT, calculate_per_bay=False):
        anchor_bay = anchor['bay_number']
        anchor_shelf = anchor['shelf_number']
        anchor_facing = anchor['facing_sequence_number']
        if 0 < (anchor_facing + left_or_right) <= anchor['n_shelf_items']:
            neighbour_facing = matches[(matches['bay_number'] == anchor_bay) &
                                       (matches['shelf_number'] == anchor_shelf) &
                                       (matches['facing_sequence_number'] == anchor_facing + left_or_right)]
        else:
            if calculate_per_bay:
                neighbour_facing = None
            else:
                if anchor_facing + left_or_right == 0:
                    if anchor_bay == 1:
                        neighbour_facing = None
                    else:
                        right_bay = matches[(matches['bay_number'] == anchor_bay - 1) &
                                            (matches['shelf_number'] == anchor_shelf)]
                        if not right_bay.empty:
                            max_facing = right_bay['n_shelf_items'].values[0]
                            neighbour_facing = right_bay[right_bay['facing_sequence_number'] == max_facing]
                        else:
                            neighbour_facing = None
                else:
                    if anchor_bay == matches['bay_number'].max():
                        neighbour_facing = None
                    else:
                        neighbour_facing = matches[(matches['bay_number'] == anchor_bay + 1) &
                                                   (matches['shelf_number'] == anchor_shelf) &
                                                   (matches['facing_sequence_number'] == 1)]

        if neighbour_facing is not None and not neighbour_facing.empty:
            neighbour_facing = neighbour_facing.iloc[0]
        else:
            neighbour_facing = None
        return neighbour_facing

    @staticmethod
    def get_scene_bay_status(matches_for_scene):
        number_of_shelves = None
        for bay in matches_for_scene['bay_number'].unique():
            max_shelf = matches_for_scene[matches_for_scene['bay_number'] == bay]['shelf_number'].max()
            if number_of_shelves is not None and max_shelf != number_of_shelves:
                return True
            number_of_shelves = max_shelf
        return False
