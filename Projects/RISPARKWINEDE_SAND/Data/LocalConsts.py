
class Consts(object):
    LVL2_CATEGORY_HEADERS = ['assortment_super_group_fk', 'assortment_group_fk', 'assortment_fk', 'target',
                    'kpi_fk_lvl1', 'kpi_fk_lvl2', 'group_target_date', 'super_group_target', 'category_fk']
    EMPTY_VAL = -1

    LVL3_SESSION_RESULTS_COL = ['kpi_level_2_fk', 'numerator_id', 'numerator_result', 'denominator_id',
                                'denominator_result', 'result', 'score']
    LVL2_SESSION_RESULTS_COL = ['kpi_level_2_fk', 'numerator_id', 'numerator_result', 'denominator_id',
                                'denominator_result', 'result', 'target', 'score']
    SKU_LEVEL = 3
    GROUPS_LEVEL = 2
