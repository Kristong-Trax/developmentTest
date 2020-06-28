
class Consts(object):
    # df columns lists
    LVL2_GROUP_HEADERS = ['kpi_fk_lvl2', 'category_fk']
    WINE_GROUP_HEADERS = ['kpi_fk_lvl2']
    LVL2_CATEGORY_HEADERS = ['kpi_fk_lvl2', 'category_fk']
    LVL3_SESSION_RESULTS_COL = ['fk', 'numerator_id', 'numerator_result', 'denominator_id',
                                'denominator_result', 'result', 'score', 'should_enter', 'identifier_parent']
    LVL2_SESSION_RESULTS_COL = ['fk', 'numerator_id', 'numerator_result', 'denominator_id',
                                'denominator_result', 'result', 'score', 'should_enter', 'identifier_result', 'identifier_parent']
    LVL1_SESSION_RESULTS_COL = ['fk', 'numerator_id', 'numerator_result', 'denominator_id',
                                'denominator_result', 'result', 'score', 'identifier_result']

    # attributes

    SKU_LEVEL = 3
    GROUPS_LEVEL = 2
    STORE_LEVEL = 1
    EMPTY_VAL = -1

    FACINGS = 'facings'
    FACINGS_IGN_STACK = 'facings_ign_stack'

    # kpi names
    DIST_SKU_LVL = 'DISTRIBUTION SKU OUT OF GROUP'
    DIST_GROUP_LVL = 'DISTRIBUTION GROUP OUT OF CATEGORY'
    DIST_CAT_LVL = 'DISTRIBUTION CATEGORY OUT OF STORE'
    OOS_CAT_LVL = 'OOS CATEGORY OUT OF STORE'
    DIST_STORE_LVL = 'DISTRIBUTION IN STORE'
    OOS_SKU_LVL = 'OOS SKU OUT OF GROUP'
    OOS_GROUP_LVL = 'OOS GROUP OUT OF CATEGORY'
    OOS_STORE_LVL = 'OOS IN STORE'
    DISTRIBUTION_LEVEL_2_NAMES = ['Could Stock', 'DirectOrder', 'Must Stock']
    WINE_LEVEL_2_NAMES = ['Wine Availability']
    WINE_STORE_LEVEL = 'STORE LEVEL WINE AVAILABILITY'
    WINE_SKU_LVL = 'Wine Availability - SKU'
    FACINGS_STACKING_KPI = 'SKU_Facings_ Exclude_Stacking'
    FACING_EXCLUDE_STACK_KPI = 'SKU_Facings_ Exclude_Stacking'
    OOS = 'OOS'
    DISTRIBUTION = 'DISTRIBUTED'# based on kpi_result_value value

    FACINGS_CATEGORIES = ['Sparkling Pure', 'Sparkling Mix']
    IGNORE_PRODUCT_TYPE = ['Empty', 'Irrelevant', 'Other']
    SOS_CATEGORY = ['Sparkling Pure', 'Sparkling Mix']
