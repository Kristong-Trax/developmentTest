from Trax.Tools.ProfessionalServices.PsConsts.DataProvider import ScifConsts as Sc
from Trax.Tools.ProfessionalServices.PsConsts.DB import SessionResultsConsts as Src
from Trax.Tools.ProfessionalServices.PsConsts.PS import AssortmentGroupConsts as Agc


class Consts(object):

    # Pepsi Categories
    CSD_CAT = 'CSD'
    TEA_CAT = 'RTD Tea'
    ENERGY_CAT = 'Energy'

    # Display for Display Compliance
    END_CAP_DISPLAY = 'Endcap'
    PALLET_DISPLAY = 'Pallet'

    LINEAR_ID_SUFFIX = 'linear'
    FACINGS_ID_SUFFIX = 'facings'
    ASSORTMENT_ID_SUFFIX = 'assortment'

    CLIENT_BRAND = 'client_brand'
    SUB_BRAND = 'sub_brand'
    CLIENT_BRAND_FK = 'client_brand_fk'
    SUB_BRAND_FK = 'sub_brand_fk'

    SCENE_CATEGORY_MAPPER = {CSD_CAT: ['Carbonated Soft Drinks'], TEA_CAT: ['RTD Tea'], ENERGY_CAT: ['Energy Drinks']}

    # KPI Names
    DISPLAY_COMP_STORE_LEVEL_FK = 'Compliant Displays'
    SOS_OWN_MANUFACTURER_GENERAL_NAME = 'Linear SOS Compliance {}'
    MANUFACTURERS_SOS_GENERAL_NAME = 'Manufacturer Share of {} {}'
    BRAND_SOS_GENERAL_NAME = 'Brand Share of {} {}'
    SUB_BRAND_SOS_GENERAL_NAME = 'Sub Brand Share of {} {}'
    FACINGS_SOS_STORE_LEVEL_KPI = 'Facings SOS'

    # Assortment Consts
    SKU_LVL = 3
    GROUP_LVL = 2
    STORE_LVL = SOS_OWN_MANU_LVL = 1
    DISPLAY_NAME = 'display_name'
    DISPLAY_TYPE = 'Display Type'  # Attribute from the assortment template
    DISPLAY_FK = 'display_fk'
    ASSORTMENT_ATTR = 'additional_attributes'
    GROUP_FACING_TARGET = 5
    SKU_COLS_TO_SAVE = ['kpi_fk_lvl3', Sc.PRODUCT_FK, Sc.FACINGS, 'in_store', Agc.ASSORTMENT_GROUP_FK, DISPLAY_FK]
    SKU_COLS_RENAME = {'kpi_fk_lvl3': 'fk', Sc.PRODUCT_FK: Src.NUMERATOR_ID, Sc.FACINGS: Src.NUMERATOR_RESULT,
                       DISPLAY_FK: Src.CONTEXT_ID, 'in_store': Src.RESULT, Agc.ASSORTMENT_GROUP_FK: Src.DENOMINATOR_ID}
    GROUP_IDE = [Agc.ASSORTMENT_GROUP_FK]
    GROUP_COLS_TO_KEEP = [Agc.ASSORTMENT_GROUP_FK, 'kpi_fk_lvl2', Sc.FACINGS, DISPLAY_FK, Src.RESULT,
                          Sc.MANUFACTURER_FK]
    GROUP_COLS_RENAME = {'kpi_fk_lvl2': 'fk', Agc.ASSORTMENT_GROUP_FK: Src.NUMERATOR_ID, DISPLAY_FK: Src.DENOMINATOR_ID,
                         Sc.FACINGS: Src.NUMERATOR_RESULT, Sc.MANUFACTURER_FK: Src.CONTEXT_ID}
    STORE_IDE = [Sc.MANUFACTURER_FK]

    # SOS Consts
    SOS_SUB_BRAND_LVL = 4
    SOS_BRAND_LVL = 3
    SOS_MANU_LVL = 2
    SOS_LINEAR_LEN_ATTR = 'gross_len_ign_stack'
    SOS_FACINGS_ATTR = 'facings'
    TOTAL_SOS = 'total_sos'

    MAPPER_KPI_LVL_AND_NAME = {SOS_OWN_MANU_LVL: SOS_OWN_MANUFACTURER_GENERAL_NAME,
                               SOS_MANU_LVL: MANUFACTURERS_SOS_GENERAL_NAME,
                               SOS_BRAND_LVL: BRAND_SOS_GENERAL_NAME,
                               SOS_SUB_BRAND_LVL: SUB_BRAND_SOS_GENERAL_NAME}

    SUB_BRAND_SOS_RENAME_DICT = {SUB_BRAND_FK: Src.NUMERATOR_ID, Sc.CATEGORY_FK: Src.DENOMINATOR_ID,
                                 SOS_LINEAR_LEN_ATTR: Src.NUMERATOR_RESULT,
                                 SOS_FACINGS_ATTR: Src.NUMERATOR_RESULT, TOTAL_SOS: Src.DENOMINATOR_RESULT}

    BRAND_SOS_RENAME_DICT = {CLIENT_BRAND_FK: Src.NUMERATOR_ID, Sc.CATEGORY_FK: Src.DENOMINATOR_ID,
                             SOS_LINEAR_LEN_ATTR: Src.NUMERATOR_RESULT,
                             SOS_FACINGS_ATTR: Src.NUMERATOR_RESULT, TOTAL_SOS: Src.DENOMINATOR_RESULT}

    ALL_MANU_SOS_RENAME_DICT = {Sc.MANUFACTURER_FK: Src.NUMERATOR_ID, Sc.CATEGORY_FK: Src.DENOMINATOR_ID,
                                SOS_LINEAR_LEN_ATTR: Src.NUMERATOR_RESULT, SOS_FACINGS_ATTR: Src.NUMERATOR_RESULT,
                                TOTAL_SOS: Src.DENOMINATOR_RESULT}

    # Logs
    WRONG_CATEGORY_LOG = "The following category does not exist: {}"
    MISSING_KPI_FOR_CATEGORY = "The following category doesn't connected to any KPI: {}"
