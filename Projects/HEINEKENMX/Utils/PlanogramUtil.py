import pandas as pd

__author__ = 'huntery'

TEMPLATE_SCENE_TYPE = 'Nombre de Tarea'
TEMPLATE_DOOR_ID = 'Puertas'
TEMPLATE_SHELF_NUMBER = 'y'
TEMPLATE_SEQUENCE_NUMBER = 'x'
TEMPLATE_FACINGS_COUNT = 'Frentes'


class HeinekenRealogram(object):
    def __init__(self, scene_mpis, scene_type, template_fk, planogram_template_data, products_to_filter_by=None):
        self.scene_fk = self._get_scene_fk(scene_mpis)
        self.mpis = scene_mpis[(scene_mpis['scene_fk'] == self.scene_fk) &
                               (scene_mpis['stacking_layer'] == 1)]
        self._check_mpis_for_leading_product_fk()
        self.template_fk = template_fk
        self.scene_type = scene_type
        self.planograms = self._generate_planograms_by_door(planogram_template_data)
        self.realogram = self._generate_realogram()
        self._filter_realogram_and_planogram(products_to_filter_by)
        self.correctly_placed_tags = self._calculate_correctly_placed_tags()
        self.incorrectly_placed_tags = self._calculate_incorrectly_placed_tags()
        self.extra_tags = self._calculate_extra_tags()
        self.extra_facings = self._calculate_extra_facings()
        self.number_of_skus_in_planogram = self._get_number_of_skus_in_planogram()
        self.number_of_positions_in_planogram = self._get_number_of_positions_in_planogram()

    def _check_mpis_for_leading_product_fk(self):
        if 'leading_product_fk' not in self.mpis.columns.tolist():
            self.mpis['leading_product_fk'] = self.mpis['product_fk']

    def _get_scene_fk(self, mpis):
        return mpis['scene_fk'].iloc[0]

    def _filter_realogram_and_planogram(self, products_to_filter_by):
        if products_to_filter_by:
            self.realogram = \
                self.realogram[(self.realogram['product_fk'].isin(products_to_filter_by)) |
                               (self.realogram['target_product_fk'].isin(products_to_filter_by))]
            for door_id in self.planograms.keys():
                filtered_planogram = self.planograms[door_id]
                filtered_planogram = \
                    filtered_planogram[filtered_planogram['target_product_fk'].isin(products_to_filter_by)]
                self.planograms[door_id] = filtered_planogram

    def _generate_planograms_by_door(self, planogram_template_data):
        planograms = {}
        relevant_template = \
            planogram_template_data[planogram_template_data[TEMPLATE_SCENE_TYPE] == self.scene_type]

        for door in relevant_template[TEMPLATE_DOOR_ID].unique().tolist():
            door_template = relevant_template[relevant_template[TEMPLATE_DOOR_ID] == door]
            planograms[door] = self._expand_template_data_into_planogram(door_template)

        return planograms

    @staticmethod
    def _expand_template_data_into_planogram(template_data):
        planogram = pd.DataFrame(columns=['target_product_fk', 'target_shelf', 'target_sequence_number'])
        for product_line in template_data.sort_values(
                by=[TEMPLATE_SHELF_NUMBER, TEMPLATE_SEQUENCE_NUMBER]).itertuples():
            for i in range(int(getattr(product_line, TEMPLATE_FACINGS_COUNT))):
                planogram.loc[len(planogram), planogram.columns.tolist()] = \
                    [product_line.target_product_fk,
                     getattr(product_line, TEMPLATE_SHELF_NUMBER),
                     getattr(product_line, TEMPLATE_SEQUENCE_NUMBER) + i]
        return planogram

    def _generate_realogram(self):
        realograms = []
        for door_id, door_planogram in self.planograms.items():
            door_realogram = self.mpis[self.mpis['bay_number'] == door_id]
            door_realogram = pd.merge(door_realogram, door_planogram, how='outer',
                                      left_on=['shelf_number', 'facing_sequence_number'],
                                      right_on=['target_shelf', 'target_sequence_number'])
            products_in_planogram = door_planogram['target_product_fk'].unique().tolist()
            door_realogram['sku_in_planogram'] = pd.np.nan  # add column to DF
            # flag tags for whether or not they exist in the planogram
            door_realogram.loc[door_realogram['target_shelf'].isna(), 'sku_in_planogram'] = \
                door_realogram.loc[door_realogram['target_shelf'].isna(),
                                   'leading_product_fk'].isin(products_in_planogram)
            realograms.append(door_realogram)
        return pd.concat(realograms)

    def _calculate_correctly_placed_tags(self):
        return self.realogram[(self.realogram['leading_product_fk'] == self.realogram['target_product_fk'])]

    def _calculate_incorrectly_placed_tags(self):
        return self.realogram[(self.realogram['leading_product_fk'] != self.realogram['target_product_fk']) |
                              (self.realogram['leading_product_fk']).isna()]

    def _calculate_extra_tags(self):
        return self.realogram[(self.realogram['sku_in_planogram'].notna()) &
                              (self.realogram['sku_in_planogram'] == False) &
                              (self.realogram['target_shelf'].isna())]

    def _calculate_extra_facings(self):
        return self.realogram[(self.realogram['sku_in_planogram'].notna()) &
                              (self.realogram['sku_in_planogram']) &
                              (self.realogram['target_shelf'].isna())]

    def calculate_correctly_placed_skus(self):
        incorrectly_placed_skus = self.incorrectly_placed_tags['target_product_fk'].unique().tolist()
        correctly_placed_skus = \
            self.correctly_placed_tags[~self.correctly_placed_tags['target_product_fk'].isin(incorrectly_placed_skus)]
        correctly_placed_skus = \
            correctly_placed_skus.groupby('target_product_fk', as_index=False)['scene_match_fk'].count()
        correctly_placed_skus.rename(columns={'scene_match_fk': 'facings'}, inplace=True)
        return correctly_placed_skus

    def calculate_incorrectly_placed_skus(self):
        incorrectly_placed_skus = \
            self.incorrectly_placed_tags.groupby('target_product_fk', as_index=False)['scene_match_fk'].count()
        incorrectly_placed_skus.rename(columns={'scene_match_fk': 'facings'}, inplace=True)
        return incorrectly_placed_skus

    def calculate_extra_skus(self):
        extra_skus = \
            self.extra_tags.groupby('target_product_fk', as_index=False)['scene_match_fk'].count()
        extra_skus.rename(columns={'scene_match_fk': 'facings'}, inplace=True)
        return extra_skus

    def _get_number_of_skus_in_planogram(self):
        planogram_skus = []
        for door_planogram in self.planograms.values():
            door_skus = door_planogram['target_product_fk'].unique().tolist()
            for sku in door_skus:
                if sku not in planogram_skus:
                    planogram_skus.append(sku)
        return len(planogram_skus)

    def _get_number_of_positions_in_planogram(self):
        number_of_positions = 0
        for door_planogram in self.planograms.values():
            number_of_positions += len(door_planogram)
        return number_of_positions



