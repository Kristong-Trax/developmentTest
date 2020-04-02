# -*- coding: utf-8 -*-
import os

__author__ = 'Hunter'


class Const(object):
    OWN_MANUFACTURER_FK = 1
    PRICE_ADHERENCE_TEMPLATE_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)),
                                                 'Price Adherence Targets 01Apr2020.xlsx')
    EXTRA_SPACES_RELEVANT_SUB_CATEGORIES_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)),
                                                             'Extra_spaces_relevant_sub_categories_2019_11_25.xlsx')
    STORE_TARGETS_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)),
                                      'Targets Store Excecution Score_ 24 Jun 2019.xlsx')

    SUB_CATEGORY_TARGET_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)),
                                      'sub_category_scores_v1.xlsx')

    # had to do this because of weird encoding problems
    EXTRA_SPACES_SURVEY_QUESTION_FK = 54
    BONUS_QUESTION_FK = 56

    PERFECT_STORE = 'Perfect Store Score'
    PERFECT_STORE_SUB_CATEGORY = 'Perfect Store Score - Sub Category'

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

    BONUS_QUESTION = 'Bonus Question'
    BONUS_QUESTION_SUB_CATEGORY = 'Bonus Question - Sub Category'

    KPI_WEIGHTS ={
        "POWERSKU" : "D",
        "SOS": "G",
        "EXTRA": "M",
        "PRICE": "P",
        "Bonus": "B",

    }