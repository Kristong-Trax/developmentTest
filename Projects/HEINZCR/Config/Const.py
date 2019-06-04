# -*- coding: utf-8 -*-
import os

__author__ = 'Hunter'


class Const(object):
    PRICE_ADHERENCE_TEMPLATE_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)),
                                                 'Price Adherence Targets 3Ene2019.xlsx')
    EXTRA_SPACES_RELEVANT_SUB_CATEGORIES_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)),
                                                             'Extra_spaces_relevant_sub_categories_2019_06_03.xlsx')
    EXTRA_SPACES_SURVEY_QUESTION_TEXT = \
        "¿Tiene el punto de venta al menos una exhibición especial y en ella al menos dos de las categoría foco? "

    PERFECT_STORE = 'Perfect Store Score'

    POWER_SKU = 'PowerSKU'
    POWER_SKU_SUB_CATEGORY = 'PowerSKU - Sub-Category'
    POWER_SKU_TOTAL = 'PowerSKU - Total Score'

    SOS_SUB_CATEGORY = 'Sub-Category SOS'
    SOS_SUB_CATEGORY_TOTAL = 'Sub-Category SOS - Total Score'

    POWER_SKU_PRICE_ADHERENCE = 'PowerSKU Price Adherence'
    POWER_SKU_PRICE_ADHERENCE_SUB_CATEGORY = 'PowerSKU Price Adherence - Sub-Category'
    POWER_SKU_PRICE_ADHERENCE_TOTAL = 'PowerSKU Price Adherence - Total Score'

    PERFECT_STORE_EXTRA_SPACES_SUB_CATEGORY = 'Perfect Store Extra Spaces - Sub-Category'
    PERFECT_STORE_EXTRA_SPACES_TOTAL = 'Perfect Store Extra Spaces - Total Score'
