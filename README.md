


from functools import reduce

oldColumns = data.schema.names
newColumns = ["name", "age"]

df = reduce(lambda data, idx: data.withColumnRenamed(oldColumns[idx], newColumns[idx]), xrange(len(oldColumns)), data)
df.printSchema()
df.show()


df.withColumn("test3",expr("to_date(value, format)")).show()
from pyspark.sql.functions import col
from pyspark.sql.functions import *
from pyspark.sql import *
from ts_glb_lib import tabAction as ta
###*******************************************************************************************************


def data_provider_lookup(df_delta, df_dp):
    delta_columns = [col('df_delta.' + column) for column in df_delta.columns]
    data_provider_columns = [col('df_dp.data_provider_key'), col('df_dp.data_provider_abbr')]
    join_condition = [df_delta.source_data_provider_nm == df_dp.data_provider_abbr]
    join_type = 'left_outer'
    delta_dp_lkp = ta.join_function(df_delta, df_dp, join_condition, join_type, delta_columns,
                                        data_provider_columns)
    delta_dp_lkp = delta_dp_lkp.withColumn("data_provider_flag", when(df_dp.data_provider_key.isNull(), 1).otherwise(0))
    return delta_dp_lkp

###******************************************************************************************************

def addDataServiceProviderKey(df_delta, df_d, df_di):
    df_l = df_d.join(df_di, df_d.dataset_key == df_di.dataset_key, 'inner').select(['df_d.' + column for column in df_d.columns] + ['df_di.original_file_nm', 'df_di.dataset_instance_key', 'df_di.dataset_receipt_dttm']).alias('df_l')
    df = df_delta.join(df_l, (df_l.original_file_nm == df_delta.source_original_file_nm) & \
                       (df_l.dataset_nm == df_delta.source_dataset_nm) & \
                       (df_delta.data_provider_key == df_l.data_provider_key), 'left') \
        .select(['df_delta.' + column for column in df_delta.columns] + \
                ['df_l.data_provider_service_key', 'df_l.dataset_instance_key','df_l.dataset_receipt_dttm', 'df_l.dataset_key'])
    delta_dp_lkp = df.withColumn("data_provider_service_key_flag",
                                 when(df_l.data_provider_service_key.isNull(), 1).otherwise(0))
    delta_dp_lkp = delta_dp_lkp.withColumn("dataset_instance_key_flag",
                                           when(df_l.dataset_instance_key.isNull(), 1).otherwise(0))
    delta_dp_lkp = delta_dp_lkp.withColumn("dataset_receipt_dttm_flag",
                                           when(df_l.dataset_receipt_dttm.isNull(), 1).otherwise(0))
    delta_dp_lkp = delta_dp_lkp.withColumn("dataset_key_flag",
                                           when(df_l.dataset_key.isNull(), 1).otherwise(0))
    return delta_dp_lkp
###******************************************************************************************************
# derive asset unit based refinery unit name . 
##****************************************************************************************************
def icis_asset_unit_lookup(df_delta, df_acr,df_aucr,df_au,df_auv,df_vt,df_ost):
    ###deriving asset_unit_key for SOURCE_UNIT_NMis 'All Units'
    df_all_units = df_delta.filter("source_unit_nm like 'All Units'").alias('df_all_units')
    ###step 1 : derive the asset_key using the source_unit_nm
    df_all_units_columns = [col('df_all_units.' + column) for column in df_all_units.columns]
    df_acr_columns = [col('df_acr.asset_key')]
    join_condition = [df_acr.provider_asset_nm == df_all_units.source_refinery_nm,
                      df_acr.data_provider_key == df_all_units.data_provider_key]
    join_type = 'left_outer'
    df_as = ta.join_function(df_all_units, df_acr, join_condition, join_type, df_all_units_columns,
                                    df_acr_columns).alias('df_as')
    ### step2 :find all the asset unit_keys
    df_auv_ost = df_auv.join(df_ost, (df_auv.operational_status_type_key == df_ost.operational_status_type_key) & \
                                  (trim(df_ost.operational_status_group_cd).isin('Operational')) &
                                  (df_auv.version_active_ind.isin('Y')), 'inner') \
        .select(['df_auv.asset_unit_close_dt', 'df_auv.asset_unit_start_dt','df_auv.version_type_key','df_auv.asset_unit_key']).alias('df_auv_ost')
    
    df_auv_ost_vt = df_auv_ost.join(df_vt, (df_auv_ost.version_type_key == df_vt.version_type_key) & \
                             (df_vt.version_type_cd.isin('B')), 'inner') \
        .select(['df_auv_ost.asset_unit_close_dt', 'df_auv_ost.asset_unit_start_dt','df_auv_ost.asset_unit_key']).alias('df_auv_ost_vt')
    df_au_jn = df_auv_ost_vt.join(df_au, (df_auv_ost_vt.asset_unit_key == df_au.asset_unit_key) & \
                                    (df_au.placeholder_ind.isin('N')), 'inner') \
        .select(['df_auv_ost_vt.asset_unit_close_dt', 'df_auv_ost_vt.asset_unit_start_dt', 'df_au.asset_key','df_au.asset_unit_key']).alias('df_au_jn')
    df_aukey_allunits = df_as.join(df_au_jn, (df_au_jn.asset_key == df_as.asset_key) & \
                             (df_au_jn.asset_unit_start_dt <= df_as.event_start_dt) & \
                             (df_au_jn.asset_unit_close_dt >= df_as.event_end_dt), 'leftouter') \
        .select(['df_au_jn.asset_unit_key']+[col('df_as.' + column) for column in df_as.columns]).alias('df_aukey_allunits')
    df_aukey_allunits = df_aukey_allunits.drop('asset_key').alias('df_aukey_allunits')
    #df_aukey_allunits = df_aukey_allunits.withColumnRenamed("asset_unit_nm", "source_refinery_unit_nm").alias('df_aukey_allunits')

    df_aukey_allunits = df_aukey_allunits.drop('asset_key').alias('df_aukey_allunits')
    ###deriving asset_unit_key for source_unit_nm is not 'All Units'(other names)
    df_not_all_units = df_delta.filter("source_unit_nm not like 'All Units'").alias('df_not_all_units')
    df_not_all_units_columns = [col('df_not_all_units.' + column) for column in df_not_all_units.columns]
    df_aucr_columns = [col('df_aucr.asset_unit_key')]
    join_condition = [df_aucr.provider_asset_unit_nm == df_not_all_units.source_unit_nm,
                      df_aucr.provider_asset_nm == df_not_all_units.source_refinery_nm,
                      df_aucr.data_provider_key == df_not_all_units.data_provider_key]
    join_type = "left_outer"
    df_aukey_not_allunits = ta.join_function(df_not_all_units, df_aucr, join_condition, join_type,df_aucr_columns,df_not_all_units_columns ).alias('df_aukey_not_allunits')

    #df_aukey_not_allunits = df_aukey_not_allunits.withColumnRenamed("source_unit_nm", "source_refinery_unit_nm").alias('df_aukey_not_allunits')
    df_aukey_not_allunits = df_aukey_not_allunits.select([col('df_aukey_not_allunits.' + column) for column in df_aukey_allunits.columns]).alias('df_aukey_not_allunits')


    #union both asset unit keys
   
    df_delta = df_aukey_allunits.union(df_aukey_not_allunits).alias('df_delta')
    return df_delta
   
###******************************************************************************************************

# deriving asset unit event key and slip number 
def icis_asset_unit_event_key_lookup(df_delta, df_au_event_ext_id, df_vt, df_aue,df_aet, df_auev, spark, gv_curated_db_name, curated_table_name):

    #
    # Processing consideration  case 2
        # Processing consideration 3.2 -
    # If the provider 'doesn't' send an event_id 'or' the provider's id doesn't have a valid match at..
    # the external_event_id table, then take the unit_key and check at the event_ver table if there is an existing event for this..
    # unit with the source event start and end dates between existing event's start dt minus red zone and event end plus the red zone days..
    # If exactly one match found, use the event_key, else set it to null

    #df_delta_match = df_delta.filter('asset_unit_event_key is not null')\
    #                         .alias('df_delta_match')
   
    #df_delta_noMatch = df_delta.filter('asset_unit_event_key is null')\
    #                           .drop('asset_unit_event_key')\
    #                           .alias('df_delta_noMatch')
    
    df_auevc = df_aue.join(df_auev, df_aue.asset_unit_event_key == df_auev.asset_unit_event_key, 'inner')\
                     .join(df_vt, ( (df_vt.version_type_key == df_auev.version_type_key) & \
                                   (df_vt.version_type_cd == 'B')), 'inner')\
                     .join(df_aet, df_aet.asset_event_type_key == df_auev.asset_event_type_key, 'inner') \
                     .select(df_aue.asset_unit_key, df_aue.asset_unit_event_key, df_auev.data_provider_key, df_aet.event_red_zone_days, df_auev.event_start_dt, df_auev.event_end_dt).alias('df_auevc')
    df_auevc = df_auevc.withColumn('event_red_zone_days', coalesce('event_red_zone_days', lit(0)))
    df_auevc = df_auevc.withColumn('new_event_start_dt', expr("date_add(event_start_dt, - event_red_zone_days)"))\
                                   .withColumn('new_event_end_dt', expr("date_add(event_end_dt,  event_red_zone_days)")).alias('df_auevc')
    
    df_delta_noMatch = df_delta.join(df_auevc, ((df_auevc.asset_unit_key == df_delta.asset_unit_key) & \
                                        (df_delta.event_start_dt >= df_auevc.new_event_start_dt) & \
                                        (df_delta.event_end_dt <= df_auevc.new_event_end_dt)), 'left') \
                                        .select(['df_delta.' + columns for columns in df_delta.columns] + ['df_auevc.asset_unit_event_key'])\
                                        .alias('df_delta_noMatch')
    
    df_cnt = df_delta_noMatch.groupBy(['icis_indep_refinery_maintenance_key','asset_unit_key']).count().alias('df_cnt')
    ''' 
    df_delta_noMatch = df_delta_noMatch.join(df_cnt, df_cnt.icis_indep_refinery_maintenance_key == df_delta_noMatch.icis_indep_refinery_maintenance_key  \
                         , 'inner').\
                           select(['df_delta_noMatch.' + columns for columns in df_delta_noMatch.columns] + ['df_cnt.count'])\
                            .alias('df_delta_noMatch')
     '''
    df_delta_noMatch = df_delta_noMatch.join(df_cnt, (df_cnt.icis_indep_refinery_maintenance_key == df_delta_noMatch.icis_indep_refinery_maintenance_key) & \
                              (df_cnt.asset_unit_key== df_delta_noMatch.asset_unit_key), 'left').\
                                        select(['df_delta_noMatch.' + columns for columns in df_delta_noMatch.columns] + ['df_cnt.count'])\
                                            .alias('df_delta_noMatch')                       
    df_delta_noMatch = df_delta_noMatch.withColumn('asset_unit_event_key',when(col('count') > 1, lit(None)).otherwise(col('asset_unit_event_key')))
    # above set null to those keys( mainintance kye plus asset unit key) have multiple entires  rest all will either null or valid value  
    df_delta_noMatch = df_delta_noMatch.withColumn('rowNum', row_number().over(Window.partitionBy('icis_indep_refinery_maintenance_key','asset_unit_key').orderBy('asset_unit_key')))
    # checked here row number has been given (1) to asset unit key is null 
    # can we change oreder by key column ?
    df_delta_noMatch = df_delta_noMatch.filter("rowNum = 1").alias('df_delta_noMatch')
    
    df_delta_noMatch = df_delta_noMatch.drop('count') 
    df_delta_noMatch = df_delta_noMatch.drop('rowNum')
    df_delta_noMatch_null = df_delta_noMatch.filter("asset_unit_event_key is null")
    df_delta_noMatch_not_null  = df_delta_noMatch.filter("asset_unit_event_key is not null")

    max_num = spark.sql("SELECT COALESCE(MAX({0}),0) FROM {1}.{2} where pipeline='ICIS_INDEP_REFINERY_MAINTENANCE'".format(\
        'asset_unit_event_slip_num', gv_curated_db_name, curated_table_name))
    slip_num_offset = df_delta_noMatch_null.select('data_provider_key').distinct().collect()[0][0] * 10000000    
    max_num = max_num.collect()[0][0]
    df_delta_noMatch_null = df_delta_noMatch_null.withColumn('rowNum',row_number().over(Window.orderBy(df_delta.columns[0])))   
    df_delta_noMatch_null = df_delta_noMatch_null.withColumn('asset_unit_event_slip_num', col('rowNum') + max_num + slip_num_offset)
 
    df_delta_noMatch_null = df_delta_noMatch_null.drop('rowNum')

    #df_delta = df_delta_match.union(df_delta_noMatch_not_null)
    df_delta = df_delta_noMatch_not_null.withColumn('asset_unit_event_slip_num', lit(None)).alias('df_delta')
    df_delta = df_delta.union(df_delta_noMatch_null).alias('df_delta')

    return df_delta

#ASSET_EVENT_TYPE_KEY derivation 
def icis_asset_unit_event_key_lookup_old_notnotneed(df_delta, df_au_event_ext_id, df_vt, df_au_event, df_aet, df_auev, spark, sql_red_zone, sql_au_event_key,gv_curated_db_name,curated_table_name):

    #Processing consideration case 1
    df_case1 = df_delta.filter("(source_event_id is not null)").alias('df_case1')
    #df_tmp = df_au_event_ext_id.join(df_au_event,'asset_unit_event_key').alias('df_tmp')
    delta_columns = [col('df_case1.' + column) for column in df_case1.columns]
    #tmp_columns = [col('df_tmp.asset_unit_event_key'),col('df_tmp.provider_asset_unit_event_id'),col('df_tmp.data_provider_key')]
    ext_columns = [col('df_au_event_ext_id.asset_unit_event_key')]
    join_type = "left_outer"
    join_condition = [df_case1.source_event_id == df_au_event_ext_id.provider_asset_unit_event_id,
                      df_case1.data_provider_key == df_au_event_ext_id.data_provider_key]
    df_auek_lkp = ta.join_function(df_case1, df_au_event_ext_id, join_condition, join_type, delta_columns, ext_columns).alias('df_auek_lkp')
    join_condition1 = [df_auek_lkp.asset_unit_event_key == df_au_event.asset_unit_event_key]
    columns1 = [col('df_auek_lkp.' + column) for column in df_auek_lkp.columns]
    columns2 = [col('df_au_event.asset_unit_event_key').alias('asset_unit_event_key1')]
    df_tmp = ta.join_function(df_auek_lkp, df_au_event, join_condition1, join_type, columns1,columns2).alias('df_tmp')
#    df_auek_lkp.join(df_au_event, 'asset_unit_event_key').alias('df_tmp')
    max_num = spark.sql("SELECT COALESCE(MAX({0}),0) FROM {1}.{2}".format(
        'asset_unit_event_slip_num',gv_curated_db_name,curated_table_name))
    max_num = max_num.collect()[0][0]
    df_tmp = df_tmp.withColumn('rowNum', row_number().over(Window.orderBy(df_tmp.columns[0])))
    df_tmp = df_tmp.withColumn('asset_unit_event_slip_num', col('rowNum') + max_num)

    df_tmp = df_tmp.drop('asset_unit_event_key1')

    # Processing consideration  case 2
    df_case2 = df_delta.filter("(source_event_id is null)").alias('df_case2')
    df_case2.createOrReplaceTempView("curated")
    df_au_event_ext_id.createOrReplaceTempView("au_event_ext_id")
    df_au_event.createOrReplaceTempView("au_event")
    df_aet.createOrReplaceTempView("asset_event_type")
    df_auev.createOrReplaceTempView("au_event_version")
    df_vt.createOrReplaceTempView("version_type")

    df_tmp1 = spark.sql(sql_red_zone).alias('df_tmp1')
    df_tmp1.createOrReplaceTempView("curated_asset_unit_event_key")

    df_au_event_key = spark.sql(sql_au_event_key).alias('df_au_event_key')
    df_au_event_key = df_au_event_key.withColumn('asset_unit_event_key', col('asset_unit_event_key_tmp'))
    max_num = spark.sql("SELECT COALESCE(MAX({0}),0) FROM {1}.{2}".format(
        'asset_unit_event_slip_num',gv_curated_db_name,curated_table_name))
    max_num = max_num.collect()[0][0]
    df_au_event_key = df_au_event_key.withColumn('rowNum',
                                                 row_number().over(Window.orderBy(df_au_event_key.columns[0])))
    df_au_event_key = df_au_event_key.withColumn('asset_unit_event_slip_num', col('rowNum') + max_num)

    df_au_event_key = df_au_event_key.drop('version_type_key', 'asset_event_type_key','ref_event_start_dt', 'ref_event_end_dt', 'event_red_zone_days', 'red_zone_start_flg', 'red_zone_end_flg', 'flag', 'num_events', 'asset_unit_event_key_tmp')

    df_tmp = df_tmp.union(df_au_event_key)
    return df_tmp
##**************************************************************************************************
def icis_asset_event_type_key_lookup(df_delta, df_aetcr):
    delta_columns = [col('df_delta.' + column) for column in df_delta.columns]
    aect_columns = [col('df_aetcr.asset_event_type_key')]
    join_condition = [df_delta.source_remarks_txt == df_aetcr.provider_asset_event_type_cd,
                      df_delta.data_provider_key == df_aetcr.data_provider_key]
    join_type = "left_outer"
    df_ect_lkp = ta.join_function(df_delta, df_aetcr, join_condition, join_type, delta_columns, aect_columns)
    return df_ect_lkp
##***********************************************************************************   
## ******* derive precesion key *************************
def icis_planning_precision_type_lookup(df_delta, df_pptcr):
    delta_columns = [col('df_delta.' + column) for column in df_delta.columns]
    ppt_columns = [col('df_pptcr.planning_precision_type_key')]
    join_condition = [upper(df_delta.source_event_precision_cd) == upper(df_pptcr.provider_planning_precision_type_cd),
                      df_delta.data_provider_key == df_pptcr.data_provider_key]
    join_type = "left_outer"
    df_ppt_lkp = ta.join_function(df_delta, df_pptcr, join_condition, join_type, delta_columns, ppt_columns)
    return df_ppt_lkp
###******************************************************************************************************
def icis_offline_capacity_original_uom_key_lookup(df_delta, df_mucr, df_d):
    delta_columns = [col('df_delta.' + column) for column in df_delta.columns]
    dataset_columns = [col('df_d.default_measure_unit_key')]
    mucr_columns = [col('df_mucr.measure_unit_key')]
    join_condition = [df_delta.dataset_key == df_d.dataset_key]
    join_type = "left_outer"
    df_ck_lkp = ta.join_function(df_delta, df_d, join_condition, join_type, delta_columns, dataset_columns).alias('df_ck_lkp')
    cols = [col('df_ck_lkp.' + column) for column in df_ck_lkp.columns]
    join_condition1 = [df_ck_lkp.source_capacity_measure_unit_cd == df_mucr.provider_measure_unit_cd,
                      df_ck_lkp.data_provider_key == df_mucr.data_provider_key]
    df_mu_lkp = ta.join_function(df_ck_lkp, df_mucr, join_condition1, join_type, cols, mucr_columns)
    # df_ck_lkp = df_ck_lkp.withColumn('capacity_original_uom_key', when((col('source_measure_unit_cd').isNull(), col('default_measure_unit_key')).otherwise(col('measure_unit_key'))))
    df_mu_lkp = df_mu_lkp.withColumn('offline_capacity_original_uom_key',
                                     when(df_mu_lkp.source_capacity_measure_unit_cd.isNull(), col('default_measure_unit_key')).otherwise(
                                         col('measure_unit_key')))
    return df_mu_lkp

###****************************************************************************************************************

def icis_asset_unit_ver_lookup(df_delta, df_auv, df_vt):

    df_l = df_auv.join(df_vt, (df_auv.version_type_key == df_vt.version_type_key) & \
                       (df_vt.version_type_cd.isin('B')), 'inner').select(
                          ['df_auv.asset_unit_key', 'df_auv.asset_unit_subtype_key']).alias('df_l')
    delta_columns = [col('df_delta.' + column) for column in df_delta.columns]
    df_l_columns = [col('df_l.asset_unit_subtype_key')]
    join_type = "left_outer"
    join_condition = [df_delta.asset_unit_key == df_l.asset_unit_key]
    df_lkp = ta.join_function(df_delta, df_l, join_condition, join_type, delta_columns, df_l_columns)

    return df_lkp

###****************************************************************************************************************


def icis_offline_capacity_key_lookup(df_delta, df_asset_unit_subtype):
    delta_columns = [col('df_delta.' + column) for column in df_delta.columns]
    df_l_columns = [col('df_asset_unit_subtype.universal_measure_unit_key'), col('df_asset_unit_subtype.platform_measure_unit_key')]
    join_type = "left_outer"
    join_condition = [df_delta.asset_unit_subtype_key == df_asset_unit_subtype.asset_unit_subtype_key]
    df_lkp = ta.join_function(df_delta, df_asset_unit_subtype, join_condition, join_type, delta_columns, df_l_columns).alias('df_lkp')
    df_lkp = df_lkp.withColumnRenamed("universal_measure_unit_key", "offline_capacity_universal_uom_key")
    df_lkp = df_lkp.withColumnRenamed("platform_measure_unit_key", "offline_capacity_platform_uom_key")

    return df_lkp;


###****************************************************************************************************************

def uomNormalizationUniversalQty(df_delta, df_measure_u, df_measure_uc, df_asset_uom):
    # Condition 2.1: When the measure units are equal
    df_case1, df_null_case, df_other_case = sameMeasureUnitCodeUniversalKey(df_delta)
    df_other_case = df_other_case.alias('df_other_case')
    # Condition 2.2: When the measure units are not equal but are of the same type
    df_case2, df_case3 = sameTypeMeasureUnitCodeUniversalKey(df_other_case, df_measure_u, df_measure_uc)

    # Send df_case2_err to notification class
    # to be done

    df_case1 = df_case1.alias('df_case1')
    df_case1 = df_case1.unionAll(df_null_case).alias('df_case1')
    df_case3 = df_case3.alias('df_case3')
    # Condition 2.3: When the measure units are not equal and are of the different type
    df_case3 = differentMeasureUnitCodeUniversalKey(df_case3, df_asset_uom)

    # df_case2 = df_case2.unionAll(df_case2_err)
    # df_case3 = df_case3.unionAll(df_case3_err)
    # Send df_case3_err to notification class
    df_not_equals = df_case2.unionAll(df_case3)

    df_not_equals = df_not_equals.drop('original_mu_type_key', 'universal_mu_type_key', 'conversion_numerator_fctr',
                                       'conversion_denominator_fctr', 'conversion_additive_fctr')
    df_final = df_case1.unionAll(df_not_equals)
    df_final = df_final.withColumn("offline_capacity_universal_qty_flag",
                                   when(df_final.offline_capacity_universal_qty.isNull(), 1).otherwise(0))
    return df_final


def uomNormalizationPlatformQty(df_delta, df_measure_u, df_measure_uc, df_asset_uom):
    # Condition 2.1: When the measure units are equal
    df_case1, df_null_case, df_other_case = sameMeasureUnitCodePlatformKey(df_delta)
    df_other_case = df_other_case.alias('df_other_case')
    # Condition 2.2: When the measure units are not equal but are of the same type
    df_case2, df_case3 = sameTypeMeasureUnitCodePlatformKey(df_other_case, df_measure_u, df_measure_uc)

    # Send df_case2_err to notification class
    # to be done
    df_case1 = df_case1.alias('df_case1')
    df_case1 = df_case1.unionAll(df_null_case).alias('df_case1')
    df_case3 = df_case3.alias('df_case3')
    # Condition 2.3: When the measure units are not equal and are of the different type
    df_case3 = differentMeasureUnitCodePlatformKey(df_case3, df_asset_uom)

    # df_case2 = df_case2.unionAll(df_case2_err)
    # df_case3 = df_case3.unionAll(df_case3_err)
    # Send df_case3_err to notification class
    df_not_equals = df_case2.unionAll(df_case3)
    df_not_equals = df_not_equals.drop('original_mu_type_key', 'universal_mu_type_key', 'platform_mu_type_key',
                                       'conversion_numerator_fctr', 'conversion_denominator_fctr',
                                       'conversion_additive_fctr')
    df_final = df_case1.unionAll(df_not_equals)
    df_final = df_final.withColumn("offline_capacity_platform_qty_flag",
                                   when(df_final.offline_capacity_platform_qty.isNull(), 1).otherwise(0))
    return df_final


def sameMeasureUnitCodeUniversalKey(df_delta):
    # Filter Delta table to get dataframe where capacity_original_uom_key = capacity_universal_uom_key
    
    df_null_case = df_delta.filter("(offline_capacity_original_uom_key is null) or (offline_capacity_universal_uom_key is null)")
    df_null_case = df_null_case.withColumn('offline_capacity_universal_qty', lit(None))
    df_delta = df_delta.filter("(offline_capacity_original_uom_key is not null ) AND (offline_capacity_universal_uom_key is not null )")
    df_case1 = df_delta.filter(col("offline_capacity_original_uom_key") == col("offline_capacity_universal_uom_key")).alias('df_case1')
    print("sameMeasureUnitCodeUniversalKey-- equals")
    # print(df_case1.count())
    # Filter Delta table to get dataframe where capacity_original_uom_key != capacity_universal_uom_key
    df_other = df_delta.filter(col("offline_capacity_original_uom_key") != col("offline_capacity_universal_uom_key")).alias('df_other')
    # print(df_other.count())
    # set capacity_universal_01_qty as capacity_original_01_qty
    df_case1 = df_case1.withColumn('offline_capacity_universal_qty', df_case1.offline_capacity_original_qty).alias('df_case1')
    return df_case1, df_null_case, df_other


def sameMeasureUnitCodePlatformKey(df_delta):
    df_null_case = df_delta.filter("(offline_capacity_original_uom_key is null) or (offline_capacity_platform_uom_key is null)")
    df_null_case = df_null_case.withColumn('offline_capacity_platform_qty', lit(None))
    df_delta = df_delta.filter("(offline_capacity_original_uom_key is not null ) AND (offline_capacity_platform_uom_key is not null )")
    # Filter Delta table to get dataframe where capacity_original_uom_key = capacity_platform_uom_key
    df_case1 = df_delta.filter(col("offline_capacity_original_uom_key") == col("offline_capacity_platform_uom_key")).alias('df_case1')
    print("sameMeasureUnitCodePFKey-- equals")
    # print(df_case1.count())
    # Filter Delta table to get dataframe where capacity_original_uom_key != capacity_platform_uom_key
    df_other = df_delta.filter(col("offline_capacity_original_uom_key") != col("offline_capacity_platform_uom_key")).alias('df_other')
    # print(df_other.count())
    # set capacity_platform_01_qty as capacity_original_01_qty
    df_case1 = df_case1.withColumn('offline_capacity_platform_qty', df_case1.offline_capacity_original_qty).alias('df_case1')
    return df_case1, df_null_case, df_other


def sameTypeMeasureUnitCodeUniversalKey(df_other_case, df_measure_u, df_measure_uc):
    ##Case 2.1: Join between capacity_original_uom_key and df_measure_u.measure_unit_key
    delta_columns = [col('df_other_case.' + column) for column in df_other_case.columns]
    ck_columns = [col('df_measure_u.measure_unit_type_key').alias('original_mu_type_key')]
    join_type = "left_outer"
    join_condition = [df_other_case.offline_capacity_original_uom_key == df_measure_u.measure_unit_key]

    df_ck_lkp = ta.join_function(df_other_case, df_measure_u, join_condition, join_type, delta_columns,
                                     ck_columns).alias('df_ck_lkp')

    # Case 2.2: Join between capacity_universal_uom_key and df_measure_u.measure_unit_key
    delta_columns = [col('df_ck_lkp.' + column) for column in df_ck_lkp.columns]
    ck_columns = [col('df_measure_u.measure_unit_type_key').alias('universal_mu_type_key')]
    join_type = "left_outer"
    join_condition = [df_ck_lkp.offline_capacity_universal_uom_key == df_measure_u.measure_unit_key]
    df_ck_lkp1 = ta.join_function(df_ck_lkp, df_measure_u, join_condition, join_type, delta_columns,
                                      ck_columns).alias('df_ck_lkp1')
    # Filter for same measure unit type key
    df_case2 = df_ck_lkp1.filter(col("original_mu_type_key") == col("universal_mu_type_key")).alias('df_case2')
    print("df_case2 equals case.....")
    # print(df_case2.count())
    # Dataframe where measure unit type key does'nt match
    df_case3 = df_ck_lkp1.filter(col("original_mu_type_key") != col("universal_mu_type_key")).alias('df_case3')
    delta_columns = [col('df_case2.' + column) for column in df_case2.columns]
    ck_columns = [col('df_measure_uc.conversion_numerator_fctr'), col('df_measure_uc.conversion_denominator_fctr'),
                  col('df_measure_uc.conversion_additive_fctr')]
    join_type = "left_outer"
    join_condition = [df_case2.offline_capacity_original_uom_key == df_measure_uc.from_measure_unit_key, \
                      df_case2.offline_capacity_universal_uom_key == df_measure_uc.to_measure_unit_key]
    df_result_case2 = ta.join_function(df_case2, df_measure_uc, join_condition, join_type, delta_columns,
                                           ck_columns).alias('df_result_case2')
    # df_case2_err = df_result_case2.filter(
    # "(conversion_numerator_fctr is null) or (conversion_denominator_fctr is null) or (conversion_additive_fctr is null)").alias(
    # 'df_case2_err')
    # df_case2_err = df_case2_err.withColumn('capacity_universal_01_qty', lit(None))
    expression = col('offline_capacity_original_qty') * col('conversion_numerator_fctr') / col(
        'conversion_denominator_fctr') + col('conversion_additive_fctr')
    df_result_case2 = df_result_case2.withColumn('offline_capacity_universal_qty', expression).alias('df_result_case2')
    print("df_result_case2 equals case.....")
    # print(df_result_case2.count())
    print("df_case3 not equals case.....")
    # print(df_case3.count())
    # df_case2_err = df_case2_err.withColumn('capacity_universal_01_qty', lit(None))
    return df_result_case2, df_case3


def sameTypeMeasureUnitCodePlatformKey(df_other_case, df_measure_u, df_measure_uc):
    # Case 2.1: Join between capacity_original_uom_key and df_measure_u.measure_unit_key
    delta_columns = [col('df_other_case.' + column) for column in df_other_case.columns]
    ck_columns = [col('df_measure_u.measure_unit_type_key').alias('original_mu_type_key')]
    join_type = "left_outer"
    join_condition = [df_other_case.offline_capacity_original_uom_key == df_measure_u.measure_unit_key]

    df_ck_lkp = ta.join_function(df_other_case, df_measure_u, join_condition, join_type, delta_columns,
                                     ck_columns).alias('df_ck_lkp')

    # Case 2.2: Join between capacity_platform_uom_key and df_measure_u.measure_unit_key
    delta_columns = [col('df_ck_lkp.' + column) for column in df_ck_lkp.columns]
    ck_columns = [col('df_measure_u.measure_unit_type_key').alias('platform_mu_type_key')]
    join_type = "left_outer"
    join_condition = [df_ck_lkp.offline_capacity_platform_uom_key == df_measure_u.measure_unit_key]
    df_ck_lkp1 = ta.join_function(df_ck_lkp, df_measure_u, join_condition, join_type, delta_columns,
                                      ck_columns).alias('df_ck_lkp1')
    # Filter for same measure unit type key
    df_case2 = df_ck_lkp1.filter(col("original_mu_type_key") == col("platform_mu_type_key")).alias('df_case2')
    print("df_case2 equals case.....")
    # print(df_case2.count())
    # Dataframe where measure unit type key does'nt match
    df_case3 = df_ck_lkp1.filter(col("original_mu_type_key") != col("platform_mu_type_key")).alias('df_case3')
    delta_columns = [col('df_case2.' + column) for column in df_case2.columns]
    ck_columns = [col('df_measure_uc.conversion_numerator_fctr'), col('df_measure_uc.conversion_denominator_fctr'),
                  col('df_measure_uc.conversion_additive_fctr')]
    join_type = "left_outer"
    join_condition = [df_case2.offline_capacity_original_uom_key == df_measure_uc.from_measure_unit_key, \
                      df_case2.offline_capacity_universal_uom_key == df_measure_uc.to_measure_unit_key]
    df_result_case2 = ta.join_function(df_case2, df_measure_uc, join_condition, join_type, delta_columns,
                                           ck_columns).alias('df_result_case2')
    # df_case2_err = df_result_case2.filter(
    # "(conversion_numerator_fctr is null) or (conversion_denominator_fctr is null) or (conversion_additive_fctr is null)").alias(
    # 'df_case2_err')
    # df_case2_err = df_case2_err.withColumn('capacity_platform_01_qty', lit(None))
    expression = col('offline_capacity_original_qty') * col('conversion_numerator_fctr') / col(
        'conversion_denominator_fctr') + col('conversion_additive_fctr')
    df_result_case2 = df_result_case2.withColumn('offline_capacity_platform_qty', expression).alias('df_result_case2')
    print("df_result_case2 equals case.....")
    # print(df_result_case2.count())
    print("df_case3 not equals case.....")
    # print(df_case3.count())
    return df_result_case2, df_case3


def differentMeasureUnitCodeUniversalKey(df_case3, df_asset_uom):
    delta_columns = [col('df_case3.' + column) for column in df_case3.columns]
    ck_columns = [col('df_asset_uom.conversion_numerator_fctr'), col('df_asset_uom.conversion_denominator_fctr'),
                  col('df_asset_uom.conversion_additive_fctr')]
    join_type = "left_outer"
    join_condition = [df_case3.data_provider_key == df_asset_uom.data_provider_key, \
                      df_case3.asset_unit_subtype_key == df_asset_uom.asset_unit_subtype_key, \
                      df_case3.offline_capacity_original_uom_key == df_asset_uom.from_measure_unit_key, \
                      df_case3.offline_capacity_universal_uom_key == df_asset_uom.to_measure_unit_key]
    df_case3 = ta.join_function(df_case3, df_asset_uom, join_condition, join_type, delta_columns, ck_columns).alias(
        'df_case3')
    # df_case3_err = df_case3.filter(
    # "(conversion_numerator_fctr is null) or (conversion_denominator_fctr is null) or (conversion_additive_fctr is null)").alias(
    # 'df_case3_err')

    expr_asset_uom = col('offline_capacity_original_qty') * col('conversion_numerator_fctr') / col(
        'conversion_denominator_fctr') + col(
        'conversion_additive_fctr')
    # df_case3_err = df_case3_err.withColumn('capacity_universal_01_qty', lit(None))
    df_case3 = df_case3.withColumn('offline_capacity_universal_qty', expr_asset_uom).alias('df_case3')
    print("df_case3 differentMeasureUnitCodeUniversalKey case.....")
    # print(df_case3.count())
    return df_case3


def differentMeasureUnitCodePlatformKey(df_case3, df_asset_uom):
    delta_columns = [col('df_case3.' + column) for column in df_case3.columns]
    ck_columns = [col('df_asset_uom.conversion_numerator_fctr'), col('df_asset_uom.conversion_denominator_fctr'),
                  col('df_asset_uom.conversion_additive_fctr')]
    join_type = "left_outer"
    join_condition = [df_case3.data_provider_key == df_asset_uom.data_provider_key, \
                      df_case3.asset_unit_subtype_key == df_asset_uom.asset_unit_subtype_key, \
                      df_case3.offline_capacity_original_uom_key == df_asset_uom.from_measure_unit_key, \
                      df_case3.offline_capacity_platform_uom_key == df_asset_uom.to_measure_unit_key]
    df_case3 = ta.join_function(df_case3, df_asset_uom, join_condition, join_type, delta_columns, ck_columns).alias(
        'df_case3')
    # df_case3_err = df_case3.filter(
    # "(conversion_numerator_fctr is null) or (conversion_denominator_fctr is null) or (conversion_additive_fctr is null)").alias(
    # 'df_case3_err')
    # df_case3_err = df_case3_err.withColumn('capacity_platform_01_qty', lit(None))
    expr_asset_uom = col('offline_capacity_original_qty') * col('conversion_numerator_fctr') / col(
        'conversion_denominator_fctr') + col(
        'conversion_additive_fctr')
    df_case3 = df_case3.withColumn('offline_capacity_platform_qty', expr_asset_uom).alias('df_case3')
    print("df_case3 differentMeasureUnitCodePlatformKey case.....")
    # print(df_case3.count())
    return df_case3


#*************notification lookup ***********************************************************************************************
def notification_lookup(df_notify, df_ntf_class):
    notify_cols = [col('df_notify.' + column) for column in df_notify.columns]
    lookup_cols = [col('df_ntf_class.notification_class_key')]
    join_type = "left_outer"
    join_condition = [df_notify.notification_class_cd == df_ntf_class.notification_class_cd]
    df_res = ta.join_function(df_notify, df_ntf_class, join_condition, join_type, notify_cols, lookup_cols).alias('df_res')
    return df_res



