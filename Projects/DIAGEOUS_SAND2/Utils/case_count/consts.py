

class Consts(object):

    # KPI Names
    TOTAL_CASES_KPI = 'Display Size Total Cases'
    FACINGS_KPI = 'Display Size Facings'
    SHOPPABLE_CASES_KPI = 'Display Size Shoppable Cases'
    IMPLIED_SHOPPABLE_CASES_KPI = 'Display Size Implied Shoppable Cases'
    NON_SHOPPABLE_CASES_KPI = 'Display Size Non-Shoppable Cases'

    DISPLAY_SKU = 'closest_sku_to_the_display'
    RELEVANT_DISPLAYS_SUFFIX = "Open|Close|open|close"
    DISPLAY_IN_SCENE_FK = 'display_in_scene_fk'
    FACINGS_SKU_TYPES = ['Bottle', 'Carton', 'Pouch', 'VAP']
    RLV_FIELDS_FOR_MATCHES_CLOSET_DISPLAY_CALC = ['scene_match_fk', 'rect_x', 'rect_y', 'scene_fk']
    RLV_FIELDS_FOR_DISPLAY_IN_SCENE_CLOSET_TAG_CALC = ['pk', 'rect_x', 'rect_y', 'scene_fk', 'display_name']
