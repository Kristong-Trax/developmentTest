from Trax.Algo.Calculations.Core.Utils import Validation



def sos_with_num_and_dem(kpi_line, num_scif, den_scif, facings_field):

    try:
        Validation.is_empty_df(den_scif)
        Validation.is_empty_df(num_scif)
        Validation.df_columns_equality(den_scif, num_scif)
        Validation.is_subset(den_scif, num_scif)
    except Exception, e:
        msg = "Data verification failed: {}.".format(e)
        # raise Exception(msg)
        # print(msg)
        return None, None, None
    num = num_scif[facings_field].sum()
    den = den_scif[facings_field].sum()

    ratio = round(num / float(den), 2)
    # numerator_id=product_fk,
    # self.common.write_to_db_result(fk=kpi_fk, numerator_result=num, denominator_result=den,
    #                                result=ratio, by_scene=True)

    # self.common.write_to_db_result(fk=kpi_fk, numerator_result=num,
    #                                    denominator_result=den, result=ratio, by_scene=True,
    #                                    identifier_parent=self.common_db2.get_dictionary(
    #                                        parent_name='Total Coke Cooler Purity'),
    #                                    should_enter=True)
    return ratio, num, den

    # def get_filter_condition(self, df, **filters):
    #     """
    #     :param df: The data frame to be filters.
    #     :param filters: These are the parameters which the data frame is filtered by.
    #                    Every parameter would be a tuple of the value and an include/exclude flag.
    #                    INPUT EXAMPLE (1):   manufacturer_name = ('Diageo', DIAGEOAUPNGAMERICAGENERALToolBox.INCLUDE_FILTER)
    #                    INPUT EXAMPLE (2):   manufacturer_name = 'Diageo'
    #     :return: a filtered Scene Item Facts data frame.
    #     """
    #     if not filters:
    #         return df['pk'].apply(bool)
    #     if self.facings_field in df.keys():
    #         filter_condition = (df[self.facings_field] > 0)
    #     else:
    #         filter_condition = None
    #     for field in filters.keys():
    #         if field in df.keys():
    #             if isinstance(filters[field], tuple):
    #                 value, exclude_or_include = filters[field]
    #             else:
    #                 value, exclude_or_include = filters[field], self.INCLUDE_FILTER
    #             if not value:
    #                 continue
    #             if not isinstance(value, list):
    #                 value = [value]
    #             if exclude_or_include == self.INCLUDE_FILTER:
    #                 condition = (df[field].isin(value))
    #             elif exclude_or_include == self.EXCLUDE_FILTER:
    #                 condition = (~df[field].isin(value))
    #             elif exclude_or_include == self.CONTAIN_FILTER:
    #                 condition = (df[field].str.contains(value[0], regex=False))
    #                 for v in value[1:]:
    #                     condition |= df[field].str.contains(v, regex=False)
    #             else:
    #                 continue
    #             if filter_condition is None:
    #                 filter_condition = condition
    #             else:
    #                 filter_condition &= condition
    #         else:
    #             Log.warning('field {} is not in the Data Frame'.format(field))
    #
    #     return filter_condition
