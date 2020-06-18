
class Consts(object):
    LVL2_GROUP_HEADERS = ['assortment_group_fk', 'kpi_fk_lvl2', 'category_fk']
    LVL2_CATEGORY_HEADERS = ['kpi_fk_lvl2', 'category_fk']
    EMPTY_VAL = -1

    LVL3_SESSION_RESULTS_COL = ['kpi_fk', 'numerator_id', 'numerator_result', 'denominator_id',
                                'denominator_result', 'result', 'score', 'should_enter', 'identifier_parent']
    LVL2_SESSION_RESULTS_COL = ['kpi_fk', 'numerator_id', 'numerator_result', 'denominator_id',
                                'denominator_result', 'result', 'target', 'score','should_enter', 'identifier_result', 'identifier_parent']
    SKU_LEVEL = 3
    GROUPS_LEVEL = 2

    DIST_SKU_LVL ='DISTRIBUTION SKU OUT OF GROUP'
    DIST_GROUP_LVL= 'DISTRIBUTION GROUP OUT OF CATEGORY'
    DIST_CAT_LVL = 'DISTRIBUTION CATEGORY OUT OF CATEGORY'
    DIST_STORE_LVL = 'DISTRIBUTION IN STORE'
    OOS_SKU_LVL =  'OOS SKU OUT OF GROUP'
    OOS_GROUP_LVL ='OOS GROUP OUT OF STORE'
    OOS_STORE_LVL = 'OOS IN STORE'
