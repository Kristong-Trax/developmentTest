
class Consts(object):
    LVL2_GROUP_HEADERS = ['assortment_fk', 'kpi_fk_lvl2', 'category_fk']
    LVL2_CATEGORY_HEADERS = ['kpi_fk_lvl2', 'category_fk']
    EMPTY_VAL = -1

    LVL3_SESSION_RESULTS_COL = ['kpi_level_2_fk', 'numerator_id', 'numerator_result', 'denominator_id',
                                'denominator_result', 'result', 'score']
    LVL2_SESSION_RESULTS_COL = ['kpi_level_2_fk', 'numerator_id', 'numerator_result', 'denominator_id',
                                'denominator_result', 'result', 'target', 'score']
    SKU_LEVEL = 3
    GROUPS_LEVEL = 2
