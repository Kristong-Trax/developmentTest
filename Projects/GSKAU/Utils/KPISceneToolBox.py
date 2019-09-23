import pandas as pd

from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Cloud.Services.Connector.Keys import DbUsers
from KPIUtils_v2.DB.PsProjectConnector import PSProjectConnector
from KPIUtils_v2.GlobalDataProvider.PsDataProvider import PsDataProvider

from Trax.Utils.Logging.Logger import Log

__author__ = 'nidhinb'
# The KPIs
GSK_DISPLAY_PRESENCE = 'GSK_DISPLAY_PRESENCE'
GSK_DISPLAY_PRESENCE_SKU = 'GSK_DISPLAY_PRESENCE_SKU'
GSK_DISPLAY_SKU_COMPLIANCE = 'GSK_DISPLAY_SKU_COMPLIANCE'
GSK_DISPLAY_PRICE_COMPLIANCE = 'GSK_DISPLAY_PRICE_COMPLIANCE'
GSK_DISPLAY_BAY_PURITY = 'GSK_DISPLAY_BAY_PURITY'
# Other Constants
POS_TYPE = 'POS'
KPI_TYPE_COL = 'type'
POSM_KEY = 'posm_pk'
# Constant IDs
GSK_MANUFACTURER_ID = 2
EMPTY_PRODUCT_ID = 0
# the keys are named as per the config file
STORE_IDENTIFIERS = [
    'additional_attribute_1', 'additional_attribute_2',
    'region_fk', 'store_type', 'store_number_1', 'address_city',
    'retailer_fk',
]
SCENE_IDENTIFIERS = [
    'template_fk',
]
POSM_PK_KEY = 'posm_pk'
# ALLOWED_POSM_EAN_KEY = 'allowed_posm_eans'
OPTIONAL_EAN_KEY = 'optional_eans'
MANDATORY_EANS_KEY = 'mandatory_eans'
POSM_IDENTIFIERS = [
    POSM_PK_KEY,
    # ALLOWED_POSM_EAN_KEY,
    OPTIONAL_EAN_KEY,
    MANDATORY_EANS_KEY,
]


def _sanitize_csv(data):
    if type(data) == list:
        # not handling ["val1", "val2,val3", "val4"]
        return [x.strip() for x in data if x.strip()]
    else:
        return [each.strip() for each in data.split(',') if each.strip()]


class GSKAUSceneToolBox:

    def __init__(self, data_provider, output, common):
        self.output = output
        self.data_provider = data_provider
        self.common = common
        self.project_name = self.data_provider.project_name
        self.session_uid = self.data_provider.session_uid
        self.products = self.data_provider[Data.PRODUCTS]
        self.templates = self.data_provider[Data.TEMPLATES]
        self.all_products = self.data_provider[Data.ALL_PRODUCTS]
        self.scif = self.data_provider.scene_item_facts
        self.match_product_in_scene = self.data_provider[Data.MATCHES]
        self.visit_date = self.data_provider[Data.VISIT_DATE]
        self.session_info = self.data_provider[Data.SESSION_INFO]
        self.scene_info = self.data_provider[Data.SCENES_INFO]
        self.store_info = self.data_provider[Data.STORE_INFO]
        self.store_id = self.store_info.iloc[0].store_fk
        self.store_type = self.data_provider.store_type
        self.rds_conn = PSProjectConnector(self.project_name, DbUsers.CalculationEng)
        self.kpi_static_data = self.common.get_kpi_static_data()
        self.ps_data_provider = PsDataProvider(self.data_provider, self.output)
        self.targets = self.ps_data_provider.get_kpi_external_targets()

        self.kpi_results_queries = []
        self.missing_products = []

    def calculate_display_compliance(self):
        kpi_display_presence = self.kpi_static_data[
            (self.kpi_static_data[KPI_TYPE_COL] == GSK_DISPLAY_PRESENCE)
            & (self.kpi_static_data['delete_time'].isnull())]
        kpi_display_sku_compliance = self.kpi_static_data[
            (self.kpi_static_data[KPI_TYPE_COL] == GSK_DISPLAY_SKU_COMPLIANCE)
            & (self.kpi_static_data['delete_time'].isnull())]
        kpi_display_price_compliance = self.kpi_static_data[
            (self.kpi_static_data[KPI_TYPE_COL] == GSK_DISPLAY_PRICE_COMPLIANCE)
            & (self.kpi_static_data['delete_time'].isnull())]
        kpi_display_bay_purity = self.kpi_static_data[
            (self.kpi_static_data[KPI_TYPE_COL] == GSK_DISPLAY_BAY_PURITY)
            & (self.kpi_static_data['delete_time'].isnull())]
        kpi_display_presence_sku = self.kpi_static_data[
            (self.kpi_static_data[KPI_TYPE_COL] == GSK_DISPLAY_PRESENCE_SKU)
            & (self.kpi_static_data['delete_time'].isnull())]
        secondary_display_targets = self.targets[
            self.targets['kpi_fk'] == kpi_display_presence['pk'].iloc[0]]
        # if no targets return
        if secondary_display_targets.empty:
            Log.warning('There is no target policy for calculating secondary display compliance.')
            return False
        else:
            current_scene_fk = self.scene_info.iloc[0].scene_fk
            display_per_sku_per_scene_calculated = False
            for idx, each_target in secondary_display_targets.iterrows():
                # loop through each external target to fit the current store
                has_posm_recognized = False
                multi_posm_or_bay = False
                mandatory_sku_compliance = False
                optional_sku_compliance = False
                price_compliance = False
                is_scene_relevant = False
                scene_relevant_targets = pd.DataFrame()
                # check store relevance
                store_relevant_targets = each_target[STORE_IDENTIFIERS].dropna()
                _bool_store_check_df = self.store_info[list(store_relevant_targets.keys())] \
                                       == store_relevant_targets.values
                is_store_relevant = _bool_store_check_df.all(axis=None)
                if is_store_relevant:
                    # check scene relevance
                    scene_relevant_targets = each_target[SCENE_IDENTIFIERS].dropna()
                    _bool_scene_check_df = self.scene_info[list(scene_relevant_targets.keys())] \
                                           == scene_relevant_targets.values
                    is_scene_relevant = _bool_scene_check_df.all(axis=None)
                if is_store_relevant and is_scene_relevant:
                    # calculate display compliance for the matched external targets
                    Log.info('The session: {sess} - scene: {scene} is relevant for calculating '
                             'secondary display compliance.'
                             .format(sess=self.session_uid, scene=current_scene_fk))
                    posm_relevant_targets = each_target[POSM_IDENTIFIERS].dropna()
                    mandatory_eans = _sanitize_csv(posm_relevant_targets[MANDATORY_EANS_KEY])
                    optional_posm_eans = []
                    if OPTIONAL_EAN_KEY in posm_relevant_targets:
                        optional_posm_eans = _sanitize_csv(posm_relevant_targets[OPTIONAL_EAN_KEY])
                    # save detailed sku presence
                    posm_to_check = each_target[POSM_PK_KEY]
                    # FIND THE SCENES WHICH HAS THE POSM to check for multiposm or multibays
                    is_posm_absent = self.scif[self.scif['product_fk'] == posm_to_check].empty
                    if is_posm_absent:
                        Log.info('The scene: {scene} is relevant but POSM {pos} is not present. '
                                 'Save and start new scene.'
                                 .format(scene=current_scene_fk, pos=posm_to_check))
                        # calculate display per sku -- POSM is absent
                        if not display_per_sku_per_scene_calculated:
                            display_per_sku_per_scene_calculated = self.save_display_presence_per_sku(
                                kpi=kpi_display_presence_sku,
                                numerator_result=0,  # 0 posm not recognized
                            )
                        if len(self.match_product_in_scene['bay_number'].unique()) > 1 or \
                                len(self.scif[self.scif['product_type'] == POS_TYPE]) > 1:
                            Log.info(
                                'The scene: {scene} is relevant and multi_bay_posm is True. '
                                'Purity per bay is calculated and going to next scene.'
                                    .format(scene=current_scene_fk, pos=posm_to_check))
                            multi_posm_or_bay = True
                            self.save_purity_per_bay(kpi_display_bay_purity)
                        self.save_display_compliance_data(
                            [
                                {'pk': kpi_display_presence.iloc[0].pk, 'result': int(has_posm_recognized),
                                 'score': int(multi_posm_or_bay), 'numerator_id': posm_to_check,
                                 'numerator_result': posm_to_check},
                                {'pk': kpi_display_sku_compliance.iloc[0].pk, 'result': float(mandatory_sku_compliance),
                                 'score': float(optional_sku_compliance), 'denominator_id': posm_to_check,
                                 'denominator_result': posm_to_check},
                                {'pk': kpi_display_price_compliance.iloc[0].pk, 'result': float(price_compliance),
                                 'score': float(price_compliance), 'denominator_id': posm_to_check,
                                 'denominator_result': posm_to_check},
                            ]
                        )
                        continue
                    # this scene has the posm
                    Log.info('The scene: {scene} is relevant and POSM {pos} is present.'
                             .format(scene=current_scene_fk, pos=posm_to_check))
                    has_posm_recognized = True
                    # check if this scene has multi posm or multi bays
                    if len(self.match_product_in_scene['bay_number'].unique()) > 1 or \
                            len(self.scif[self.scif['product_type'] == POS_TYPE]) > 1 or \
                            not self.scif[(self.scif['product_type'] == POS_TYPE) &
                                          (self.scif['facings'] > 1)].empty:
                        # Its multi posm or bay -- only purity calc per bay is possible
                        Log.info('The scene: {scene} is relevant and POSM {pos} is present but multi_bay_posm is True. '
                                 'Purity per bay is calculated and going to next scene.'
                                 .format(scene=current_scene_fk, pos=posm_to_check))
                        multi_posm_or_bay = True
                        # calculate display per sku for multi posm/multi bay
                        if not display_per_sku_per_scene_calculated:
                            display_per_sku_per_scene_calculated = self.save_display_presence_per_sku(
                                kpi=kpi_display_presence_sku,
                                numerator_result=2,  # 2 multi posm
                            )
                        self.save_display_compliance_data(
                            [
                                {'pk': kpi_display_presence.iloc[0].pk, 'result': int(has_posm_recognized),
                                 'score': int(multi_posm_or_bay), 'numerator_id': posm_to_check,
                                 'numerator_result': posm_to_check},
                                {'pk': kpi_display_sku_compliance.iloc[0].pk, 'result': float(mandatory_sku_compliance),
                                 'score': float(optional_sku_compliance), 'denominator_id': posm_to_check,
                                 'denominator_result': posm_to_check},
                                {'pk': kpi_display_price_compliance.iloc[0].pk, 'result': float(price_compliance),
                                 'score': float(price_compliance), 'denominator_id': posm_to_check,
                                 'denominator_result': posm_to_check},
                            ]
                        )
                        self.save_purity_per_bay(kpi_display_bay_purity)
                        continue

                    Log.info('The scene: {scene} is relevant and POSM {pos} is present with only one bay.'
                             .format(scene=current_scene_fk, pos=posm_to_check))
                    # save purity per bay
                    self.save_purity_per_bay(kpi_display_bay_purity)
                    # calculate display per sku for ALL SUCCESS
                    if not display_per_sku_per_scene_calculated:
                        display_per_sku_per_scene_calculated = self.save_display_presence_per_sku(
                            kpi=kpi_display_presence_sku,
                            posm_to_check=posm_to_check,
                            numerator_result=1,  # 1--one one posm
                            mandatory_eans=mandatory_eans,
                            optional_posm_eans=optional_posm_eans)
                    # calculate compliance
                    mandatory_sku_compliance = self.get_ean_presence_rate(mandatory_eans)
                    optional_sku_compliance = self.get_ean_presence_rate(optional_posm_eans)
                    if mandatory_sku_compliance:
                        price_compliance = self.get_price_presence_rate(mandatory_eans)
                    self.save_display_compliance_data(
                        [
                            {'pk': kpi_display_presence.iloc[0].pk, 'result': int(has_posm_recognized),
                             'score': int(multi_posm_or_bay), 'numerator_id': posm_to_check,
                             'numerator_result': posm_to_check},
                            {'pk': kpi_display_sku_compliance.iloc[0].pk, 'result': float(mandatory_sku_compliance),
                             'score': float(optional_sku_compliance), 'denominator_id': posm_to_check,
                             'denominator_result': posm_to_check},
                            {'pk': kpi_display_price_compliance.iloc[0].pk, 'result': float(price_compliance),
                             'score': float(price_compliance), 'denominator_id': posm_to_check,
                             'denominator_result': posm_to_check},
                        ]
                    )
                    continue
                else:
                    # the session/store is not part of the KPI targets
                    Log.info('The session: {sess} - scene: {scene}, the current kpi target [pk={t_pk}] '
                             'is not valid. Keep Looking...'
                             .format(sess=self.session_uid, scene=current_scene_fk,
                                     t_pk=each_target.external_target_fk))
                    if scene_relevant_targets.empty:
                        # Store failed
                        Log.info("Store info is {curr_data} but target is {store_data}".format(
                            curr_data=self.store_info.iloc[0][list(store_relevant_targets.keys())].to_json(),
                            store_data=store_relevant_targets.to_json()
                        ))
                    else:
                        Log.info("Scene info is {curr_data} but target is {store_data}".format(
                            curr_data=self.scene_info.iloc[0][list(scene_relevant_targets.keys())].to_json(),
                            store_data=scene_relevant_targets.to_json()
                        ))
                    continue
            else:
                if not display_per_sku_per_scene_calculated:
                    # check if its secondary display type
                    if not self.templates.loc[(self.templates['template_group'] == 'Secondary display') &
                                              (~self.templates['template_name'].isin(['Clipstrip', 'Hangsell']))].empty:
                        Log.info("Secondary Display => Session: {sess} - scene {scene} didn't qualify "
                                 "any external targets.".format(sess=self.session_uid,
                                                                scene=self.scene_info.iloc[0].scene_fk,
                                                                ))
                        display_per_sku_per_scene_calculated = self.save_display_presence_per_sku(
                            kpi=kpi_display_presence_sku,
                            numerator_result=0)  # 0--posm not recognized

    def get_ean_presence_rate(self, ean_list):
        """
        This method takes the list of eans, checks availability in scif and returns percentage
        of items among the input list; which are present
        """
        Log.info('Calculate ean presence rate for : {scene}.'.format(scene=self.scene_info.iloc[0].scene_fk))
        if not ean_list:
            return 0.0
        present_ean_count = len(self.scif[self.scif['product_ean_code'].isin(ean_list)])
        return present_ean_count / float(len(ean_list)) * 100

    def get_price_presence_rate(self, ean_list):
        """
        This method takes ean list as input and returns percentage of eans which has price.
        """
        Log.info('Calculate price presence rate for : {scene}.'.format(scene=self.scene_info.iloc[0].scene_fk))
        if not ean_list:
            return 0.0
        scif_to_check = self.scif[self.scif['product_ean_code'].isin(ean_list)]
        price_fields = ['median_price', 'median_promo_price']
        present_price_count = 0
        for idx, each_data in scif_to_check.iterrows():
            if each_data[price_fields].apply(pd.notnull).any():
                present_price_count += 1
        return present_price_count / float(len(ean_list)) * 100

    def save_purity_per_bay(self, kpi_bay_purity):
        Log.info('Calculate purity per bay for : {scene}.'.format(scene=self.scene_info.iloc[0].scene_fk))
        # could have done with scif
        total_prod_in_scene_count = len(
            self.match_product_in_scene[self.match_product_in_scene['product_fk'] != 0]
        )
        mpis_grouped_by_bay = self.match_product_in_scene.groupby(['bay_number'])
        for bay_number, mpis in mpis_grouped_by_bay:
            mpis_with_prod = mpis.merge(self.products, how='left', on=['product_fk'], suffixes=('', '_prod'))
            gsk_prod_count = len(mpis_with_prod[mpis_with_prod['manufacturer_fk'] == 2])
            if total_prod_in_scene_count:
                purity = gsk_prod_count / float(total_prod_in_scene_count) * 100
                Log.info('Save purity per bay for scene: {scene}; bay: {bay} & purity: {purity}.'
                         .format(scene=self.scene_info.iloc[0].scene_fk,
                                 bay=bay_number,
                                 purity=purity
                                 ))
                self.common.write_to_db_result(
                    fk=kpi_bay_purity.iloc[0].pk,
                    context_id=self.store_id,
                    numerator_id=self.store_id,
                    denominator_id=self.store_id,
                    numerator_result=self.store_id,
                    denominator_result=self.store_id,
                    result=purity,
                    score=bay_number,
                    by_scene=True,
                )
        return True

    def save_display_compliance_data(self, data):
        for each_kpi_data in data:
            Log.info("Saving Display Compliance kpi_fk {pk} for session: {sess} - scene {scene}".format(
                pk=each_kpi_data.get('pk'),
                sess=self.session_uid,
                scene=self.scene_info.iloc[0].scene_fk,
            ))
            self.common.write_to_db_result(
                fk=each_kpi_data.get('pk'),
                numerator_id=each_kpi_data.get('numerator_id', self.store_id),
                denominator_id=each_kpi_data.get('denominator_id', self.store_id),
                numerator_result=each_kpi_data.get('numerator_result', self.store_id),
                denominator_result=each_kpi_data.get('denominator_result', self.store_id),
                result=each_kpi_data.get('result', None),
                score=each_kpi_data.get('score', None),
                context_id=self.store_id,
                by_scene=True,
            )
        return True

    def save_display_presence_per_sku(self, kpi, posm_to_check=None, numerator_result=None,
                                      mandatory_eans=None, optional_posm_eans=None):
        # This should be done once only per scene
        current_scene_fk = self.scene_info.iloc[0].scene_fk
        context_fk = self.scene_info.iloc[0].template_fk
        if posm_to_check and (mandatory_eans or optional_posm_eans):
            # the scene is valid as per external targets template
            # PER SCENE which are secondary display
            # save POSM as numerator; each product as denominator and template as context
            Log.info('Calculate display presence per sku. The session: {sess} - scene: {scene} is valid.'
                     .format(sess=self.session_uid, scene=current_scene_fk))
            numerator_id = posm_to_check
        else:
            # the scene doesn't have mandatory or optional eans
            # PER SCENE which are secondary display
            # save numerator as empty; each empty as denominator and template as context
            Log.info('Calculate display presence per sku. The session: {sess} - scene: {scene} is invalid.'
                     .format(sess=self.session_uid, scene=current_scene_fk))
            numerator_id = 0  # General Empty
        # result => [ 0=Optional, 1=mandatory, 2=NA]
        # numerator_result => [0-- posm not recognized; 1--one one posm;  2--multi posm]
        for idx, each_row in self.scif.iterrows():
            result = 2  # NA
            if mandatory_eans and each_row.product_ean_code in mandatory_eans:
                result = 1  # mandatory
            elif optional_posm_eans and each_row.product_ean_code in optional_posm_eans:
                result = 0  # optional
            self.common.write_to_db_result(
                fk=kpi.iloc[0].pk,
                numerator_id=numerator_id,  # its the POSM to check or General Empty if not recognized
                numerator_result=numerator_result,  # whether POSM is 0, 1 or 2
                denominator_id=each_row.item_id,  # each product in scene
                score=min(each_row.median_price, each_row.median_promo_price) or -1,  # -1 means not saved in DB
                result=result,  # 0-optional, 1-mandatory, 2- NA
                context_id=context_fk,  # template of scene
                by_scene=True,
            )
        return True
