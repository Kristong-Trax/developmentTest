#!/usr/bin/python
# -*- coding: utf-8 -*-

_author_ = 'NicolasKeeton'


class Const(object):

    # sheets Score
    RESTAURANTE_SHEET, BAR_BOTECO_SHEET, BAR_LANCHONETE_SHEET = 'RESTAURANTE', 'BAR BOTECO', 'BAR LANCHONETE'
    DROGARIA_SHEET, LOJA_SHEET, MERCERIA_SHEET, PADARIA = 'DROGARIA', 'LOJA DE CONVENIÊNCIA', 'MERCEARIA', 'PADARIA'
    AS_14_SHEET, AS_5_SHEET, ATACADO_SHEET = 'AS 1-4', 'AS 5+', 'ATACADO'

    # Sheet main template
    KPIS = "KPIs"
    SOVI = "SOVI"

    SHEETS_SCORE = {RESTAURANTE_SHEET, BAR_BOTECO_SHEET, BAR_LANCHONETE_SHEET, DROGARIA_SHEET, LOJA_SHEET,
                    MERCERIA_SHEET, PADARIA, AS_14_SHEET, AS_5_SHEET, ATACADO_SHEET}

    SHEETS_MAIN = {KPIS, SOVI}

    # generic columns
    KPI_NAME = "KPI NAME"

    # columns for Score_sheets
    KPI = 'Kpi'
    LOW_PERCENT = 'Low'
    HIGH_PERCENT = 'High'
    ACCEPTANCE_PERCENT = 'range'
    SCORE = 'Score'

    # column for KPIs
    Type = "Type"
    SCENE_TYPES = "Scene Types"
    STORE_TYPES = "STORE TYPES"
    TEMPLATE_NAME = "TEMPLATE_NAME"
    TEMPLATE_GROUP = "TEMPLATE GROUP"

    # column of SOVI:
    SOVI_NAME = "KPI NAME"
    KPI_RULE = "KPI RULE"
    DEN_TYPE = "denominator param"
    DEN_VALUE = "denominator value"
    DEN_TYPES_1 = "denominator param 1"
    DEN_VALUES_1 = "denominator value 1"
    NUM_TYPE = "numerator param"
    NUM_VALUE = "numerator value"
    NUM_TYPES_1 = "numerator param 1"
    NUM_VALUES_1 = "numerator value 1"
    NUM_TYPES_2 = "numerator param 2"
    NUM_VALUES_2 = "numerator value 2"
    NUM_TYPES_3 = "numerator param 3"
    NUM_VALUES_3 = "numerator value 3"

    SOS = "SOS"
    TOTAL = "total"
