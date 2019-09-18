from KPIUtils_v2.Utils.Consts.DataProvider import ProductsConsts, MatchesConsts

class Consts(object):

    FLEXIBLE_MODE = 'Flexible Mode'
    STRICT_MODE = 'Strict Mode'

    ATTRIBUTES_TO_SAVE = [MatchesConsts.SCENE_MATCH_FK, ProductsConsts.PRODUCT_NAME, ProductsConsts.PRODUCT_FK, ProductsConsts.PRODUCT_TYPE, ProductsConsts.PRODUCT_EAN_CODE, 'sub_brand_name',
                          MatchesConsts.BAY_NUMBER, MatchesConsts.WIDTH_MM_ADVANCE, MatchesConsts.HEIGHT_MM_ADVANCE]
    KPI_RESULT = 'report.kpi_results'
    KPK_RESULT = 'report.kpk_results'
    KPS_RESULT = 'report.kps_results'

    IN_ASSORTMENT = 'in_assortment_osa'
    IS_OOS = 'oos_osa'
    PSERVICE_CUSTOM_SCIF = 'pservice.custom_scene_item_facts'
    DEFAULT = 'Default'
    TOP = 'Top'
    BOTTOM = 'Bottom'

    FACING_SOS = 'Facing SOS'
    FACING_SOS_BY_SCENE = 'Facing SOS by Scene'
    LINEAR_SOS = 'Linear SOS'
    SHELF_SPACE_LENGTH = 'Shelf Space Length'
    SHELF_SPACE_LENGTH_BY_SCENE = 'Shelf Space Length by Scene'
    FACING_COUNT = 'Facing Count'
    FACING_COUNT_BY_SCENE = 'Facing Count By Scene'
    DISTRIBUTION = 'Distribution'
    DISTRIBUTION_BY_SCENE = 'Distribution By Scene'
    SHARE_OF_DISPLAY = 'Share of Display'
    COUNT_OF_SCENES = 'Count of Scenes'
    COUNT_OF_SCENES_BY_SCENE_TYPE = 'Count of Scenes by scene type'
    COUNT_OF_POSM = 'Count of POSM'
    POSM_COUNT = 'POSM Count'
    POSM_ASSORTMENT = 'POSM Assortment'
    SURVEY_QUESTION = 'Survey Question'

    SHELF_POSITION = 'Shelf Position'
    BRANDS = 'Brand'
    MANUFACTURERS = 'Manufacturer'
    AGGREGATED_SCORE = 'Aggregated Score'
    REFERENCE_KPI = 'Reference KPI'

    CATEGORY_PRIMARY_SHELF = 'Category Primary Shelf'
    DISPLAY = 'Display'
    PRIMARY_SHELF = 'Primary Shelf'

    KPI_TYPE = 'KPI Type'
    SCENE_TYPES = 'Scene Types'
    KPI_NAME = 'KPI Name'
    CUSTOM_SHEET = 'Custom Sheet'
    PER_CATEGORY = 'Per Category'
    SUB_CALCULATION = 'Sub Calculation'
    VALUES_TO_INCLUDE = 'Values to Include'
    SHELF_LEVEL = 'Shelf Level'
    WEIGHT = 'Weight'
    SET_NAME = 'Set Name'
    UNICODE_DASH = u' \u2013 '

    CATEGORY = 'Category'
    POSM_NAME = 'POSM Name'
    POSM_TYPE = 'POSM Type'
    PRODUCT_NAME = 'Product Name'
    PRODUCT_EAN = 'Product EAN'

    SURVEY_ID = 'Survey Question ID'
    SURVEY_TEXT = 'Survey Question Text'

    SEPARATOR = ','
    SCENE_TYPES_TO_INCLUDE = 'Scene Types to Include'


    HIERARCHY = 'Hierarchy'
    GOLDEN_ZONE = 'Golden Zone'
    GOLDEN_ZONE_CRITERIA = 'Golden Zone Criteria'
    BLOCK = 'Block'
    ADJACENCY = 'Adjacency'
    ANCHOR = 'Anchor'
    PERFECT_EXECUTION = 'Perfect Execution'
    CATEGORY_LIST = 'Data List'
    PRODUCT_GROUP = 'Product Groups'
    VERTICAL = 'Vertical Block'
    GROUP_GOLDEN_ZONE_THRESHOLD = 'Threshold'
    PRODUCT_GROUP_ID = 'Product Group Id'
    ALLOWED_PRODUCT_GROUP_ID = 'ALLOWED;Product Group Id'
    KPI_FORMAT = 'Category: {category} - Brand: {brand} - Product group id: {group} - KPI Question: {question}'
