import datetime
import json
from operator import itemgetter

import pandas as pd

from Trax.Algo.Geometry.Masking.MaskingConsts import TIME_SEVERITY
from Trax.Algo.Geometry.SceneAnalysis.DBHelper import SceneDBHelper
from Trax.Cloud.Services.Connector.Factory import BigDataFactory
from Trax.DB.Mongo.Connector import MongoConnector
from Trax.Data.Orm.OrmCore import OrmSession
from Trax.Utils.Classes.With import WithAgent
from Trax.Utils.Conf.Configuration import Config
from Trax.Utils.Conventions.Log import Severities
from Trax.Utils.Logging.Logger import Log

MASKING_ENGINE_RESPONSE_COLLECTION = 'masking_engine_response'
PLANOGRAM_MASKING_ENGINE_RESPONSE_COLLECTION = 'planogram_masking_engine_response'
MASKING_DETECTOR_RESPONSE_COLLECTION = 'masking_detector_response'


def get_mongo_connector():
    return MongoConnector(MongoConnector.MASKING)


def get_masks_from_rds_gcp(project, probe_list, session=None):
    """
    get_masks_from_rds equivalent function that can run on cgp cloud.
    this function is needed to avoid using JSON_ARRAYAGG operator that is not supported on GCP mysql
    """
    masks = get_masks_from_rds_flat(project, probe_list, session)
    pipeline_id = 'just'
    version = 'MaskingEngine_SQL'

    records = []
    for _id, (probe_id, probe_masks) in enumerate(masks.groupby('probe_id')):
        creation_time = probe_masks.creation_time.max()
        response = probe_masks[['probe_tag_id', 'x1', 'y1', 'x2', 'y2', 'x', 'y']].to_dict('list')
        records.append({
            '_id': _id+1,
            'creation_time': creation_time,
            'pipeline_id': pipeline_id,
            'probe_id': probe_id,
            'project_name': project,
            'response': response,
            'version': version
        })

    masks_mongo_style = pd.DataFrame.from_records(records)
    return masks_mongo_style


def get_masks(project, probe_ids, limit=None, detection_mode=False, unique=False, orm_session=None, allow_athena=False):
    if detection_mode:
        raise Exception("MaskingEngine detector mode is not supported")

    try:
        probe_ids = set(probe_ids)
    except TypeError:
        probe_ids = set([probe_ids])

    with WithAgent(orm_session, OrmSession, project) as session:
        # get from the project rds
        if Config.get_cloud() == Config.GCP:
            masks_from_rds = get_masks_from_rds_gcp(project, probe_ids, session=session)
        else:
            masks_from_rds = get_masks_from_rds(project, probe_ids, session=session)

        if masks_from_rds.empty:
            rds_probes = set([])
        else:
            rds_probes = set(masks_from_rds.probe_id)

        probes_without_masks = probe_ids - rds_probes
        if len(probes_without_masks) > 0:
            Log.info("Missing masks from RDS",
                     extra={'missing': len(probes_without_masks), 'searched': len(probe_ids)})

        # get from the project mongo as fallback
        probe_ids = probes_without_masks
        if len(probe_ids) > 0:
            probe_masks = get_masks_from_mongo(project, probe_ids)
            if probe_masks.empty:
                mongo_probes = set([])
            else:
                mongo_probes = set(probe_masks.probe_id)
        else:
            mongo_probes = set([])
            probe_masks = pd.DataFrame(columns=masks_from_rds.columns)

        probes_without_masks = probe_ids - mongo_probes
        if len(probes_without_masks) > 0:
            Log.warning("Missing masks from Mongo",
                        extra={'missing': len(probes_without_masks), 'searched': len(probe_ids)})

        # get from the project athena as fallback
        probe_ids = probes_without_masks
        if allow_athena and len(probe_ids) > 0:
            probe_masks_athena = get_masks_archive(project, probe_ids, session)
            athena_probes = set(probe_masks_athena.probe_id)
            if not athena_probes:
                Log.warning("No masks were found in athena (waste of resources)")
        else:
            probe_masks_athena = pd.DataFrame(columns=masks_from_rds.columns)
            athena_probes = set([])

        probes_without_masks = probe_ids - athena_probes
        if allow_athena and len(probes_without_masks) > 0:
            Log.error("Missing masks from Athena",
                      extra={'missing': len(probes_without_masks), 'searched': len(probe_ids)})

        probe_masks = pd.concat([masks_from_rds, probe_masks, probe_masks_athena])

    return probe_masks


def get_masks_from_mongo(project, probe_ids=None, limit=None, detection_mode=False, unique=False):
    key = {}
    if project is not None:
        key.update({"project_name": project})
    if probe_ids is not None:
        try:
            key.update({"probe_id": {"$in": list(probe_ids)}})
        except Exception as e:
            Log.error("failed updating key\n{}".format(e))
            key.update({"probe_id": probe_ids})
    with get_mongo_connector() as m:
        if detection_mode:
            col = m.db[MASKING_DETECTOR_RESPONSE_COLLECTION]
        else:
            col = m.db[MASKING_ENGINE_RESPONSE_COLLECTION]

        if unique:
            aggregate_pipeline = [
                {"$match": key},
                {"$sort": {"creation_time": 1}},
                {"$group": {
                    "_id": {"project_name": "$project_name", "probe_id": "$probe_id"},
                    # "cnt": {"$sum": 1},
                    # "first": {"$first": "$$ROOT"},
                    "last": {"$last": "$$ROOT"}}},
                {"$project": {"_id": 0, "last": 1}}
            ]
            if limit is not None:
                aggregate_pipeline.append({"$limit": limit})
            agg_res = col.aggregate(aggregate_pipeline)
            res = map(itemgetter("last"), agg_res)
        else:
            res = col.find(key).sort([["_id", -1]])
            if limit is not None:
                res = res.limit(limit)

        masks = pd.DataFrame(list(res))
    return masks


def save_response_to_sql(project, detections, db_helper=None, orm_session=None):
    if db_helper is None:
        db_helper = SceneDBHelper()
    db_helper.write_masking_engine_masks(project, detections, orm_session=orm_session)


def save_response_to_mongo(mongo_results_connector,
                           project,
                           probe_id,
                           detections,
                           pipeline_id,
                           detector_mode=False,
                           planogram_process=False):
    start_time = Log.get_current_time()
    if detections.empty:
        response = {}
    else:
        if detector_mode:
            response = detections[['x1', 'y1', 'x2', 'y2']].to_dict('list')
        else:
            response = detections[
                ['probe_tag_id', 'x', 'y', 'x1', 'y1', 'x2', 'y2']].to_dict('list')
    version = Config.get_version()
    creation_time = datetime.datetime.utcnow()
    save_data = {'project_name': project,
                 'version': version,
                 'creation_time': creation_time,
                 'pipeline_id': pipeline_id,
                 'response': response}
    if planogram_process:
        collection_name = PLANOGRAM_MASKING_ENGINE_RESPONSE_COLLECTION
        save_data.update({'planogram_image_id': probe_id})
    else:
        collection_name = MASKING_DETECTOR_RESPONSE_COLLECTION if detector_mode else MASKING_ENGINE_RESPONSE_COLLECTION
        save_data.update({'probe_id': probe_id})

    res = False
    for ind in range(3):
        try:
            db = mongo_results_connector.db[collection_name]
            res = db.insert_one(save_data)
            if res:
                break
        except Exception as e:
            Log.warning("save_response failed\n{}".format(e))
            res = False

    if not res:
        raise Exception("save_response error")

    Log.time("Response Saved", start_time=start_time, severity=TIME_SEVERITY)


def jsonify_response(val):
    columns = ["probe_tag_id", "bx1", "x1", "bx2", "x2", "by1", "y1", "by2", "y2", "x", "y"]
    val = val.replace("=", ":")
    for col in columns:
        val = val.replace('{}:'.format(col), '"{}":'.format(col))
    return json.loads(val)


def get_masks_from_rds_flat(project, probe_list, session=None):
    if not session:
        session = OrmSession(project)

    probe_list_str = "AND mpip.probe_fk IN (" + ",".join([str(val) for val in probe_list]) + ")"
    query = """
    SELECT
        mpip.creation_time AS creation_time,
        mpip.probe_fk AS probe_id,
        mpipm.match_product_in_probe_fk AS probe_tag_id,
        mpipm.mask_left AS x1,
        mpipm.mask_right AS x2,
        mpipm.mask_top AS y1,
        mpipm.mask_bottom AS y2,
        ROUND(mpip.rect_x + mpip.rect_width*0.5, 0) AS x,
        ROUND(mpip.rect_y + mpip.rect_height*0.5, 0) AS y
    FROM
        probedata.match_product_in_probe_masks AS mpipm
    INNER JOIN probedata.match_product_in_probe AS mpip 
        ON mpip.pk = mpipm.match_product_in_probe_fk
    WHERE
        1=1
        {probe_list_str}
    """.format(**{"project": project, "probe_list_str": probe_list_str})

    res = session.execute(query)
    masks_from_rds = pd.DataFrame(list(res), columns=res.keys())
    masks_from_rds[['x1', 'x2', 'y1', 'y2']] = masks_from_rds[['x1', 'x2', 'y1', 'y2']].astype(float)
    masks_from_rds[['x', 'y']] = masks_from_rds[['x', 'y']].astype(int)

    return masks_from_rds


def get_masks_from_rds(project, probe_list, session=None):
    if not session:
        session = OrmSession(project)

    probe_list_str = "and mpip.probe_fk in (" + ",".join([str(val) for val in probe_list]) + ")"
    query = """
    select
        1 as _id,
        mpip.creation_time as creation_time,
        'just' as pipeline_id,
        mpip.probe_fk as probe_id,
        '{project}' as project_name,
        JSON_OBJECT("probe_tag_id",
        JSON_ARRAYAGG(mpipm.match_product_in_probe_fk),
        "x1",
        JSON_ARRAYAGG(mpipm.mask_left),
        "x2",
        JSON_ARRAYAGG(mpipm.mask_right),
        "y1",
        JSON_ARRAYAGG(mpipm.mask_top),
        "y2",
        JSON_ARRAYAGG(mpipm.mask_bottom),
        "x",
        JSON_ARRAYAGG(ROUND(mpip.rect_x + mpip.rect_width*0.5, 0)),
        "y",
        JSON_ARRAYAGG(ROUND(mpip.rect_y + mpip.rect_height*0.5, 0))) as response,
        'MaskingEngine_SQL' as version
    from
        probedata.match_product_in_probe_masks mpipm
    inner join probedata.match_product_in_probe mpip on
        mpip.pk = mpipm.match_product_in_probe_fk
    where
        1=1
        {probe_list_str}
    GROUP by
        mpip.probe_fk
    """.format(**{"project": project, "probe_list_str": probe_list_str})

    res = session.execute(query)
    masks_from_rds = pd.DataFrame(list(res), columns=res.keys())
    masks_from_rds["response"] = masks_from_rds["response"].apply(jsonify_response)

    return masks_from_rds


def get_masks_archive(project, athena_query_probes, session=None):
    if not session:
        session = OrmSession(project)
    Log.info("Accessing cloud archive to extract masks for {} probes".format(len(athena_query_probes)))
    probes_list_str = ','.join([str(val) for val in athena_query_probes])
    processing_info_query = """
                            select 	
                                distinct	
                                DATE(completion_time) as date,
                                LAST_DAY(completion_time) = DATE(completion_time) as is_last_day_of_month
                            from 
                                probedata.user_processing_info
                            where probe_fk in ({})
                        """.format(probes_list_str)
    res = session.execute(processing_info_query)
    partitions_df = pd.DataFrame(list(res), columns=res.keys())
    partitions_df["date"] = pd.to_datetime(partitions_df["date"])

    # handle the case of end of month probe upload
    sub_df = partitions_df[partitions_df["is_last_day_of_month"] == 1].copy()
    sub_df["date"] = sub_df["date"].apply(lambda d: d + datetime.timedelta(days=7))
    partitions_df = partitions_df.append(sub_df)

    partitions_df["month"] = partitions_df["date"].dt.month
    partitions_df["year"] = partitions_df["date"].dt.year
    partition_str = "(" + " or ".join(
        partitions_df[["month", "year"]].drop_duplicates().apply(
            lambda row: "(month={} and year={})".format(row["month"], row["year"]), axis=1)) + ")"

    athena_query = """SELECT 
                            _id, creation_time, pipeline_id, probe_id, project_name, response, version
                        FROM 
                            mongo_archives_prod.masking_engine_response_view 
                        where 1=1
                        and project_name = '{}'
                        and probe_id in ({})
                        and {};""".format(project, probes_list_str, partition_str)
    with Log.Timer('BigDataFactory masking extraction execution time', Severities.INFO,
                   extra={'project_name': project}):
        athena_connector = BigDataFactory.get_big_data_connector(region='us-east-1', schema_name='mongo_archives_prod')
        athena_connector.connect()
        cursor = athena_connector.execute_query(athena_query)

    columns = [col[0] for col in cursor.description]
    probe_masks_athena = pd.DataFrame.from_records(cursor, columns=columns).drop_duplicates()
    probe_masks_athena["response"] = probe_masks_athena["response"].apply(jsonify_response)
    return probe_masks_athena


def get_stitching_data(project, scene_ids, session=None):
    if not session:
        session = OrmSession(project)
    query = SceneDBHelper().get_stitching_data_query(scene_ids)
    res = session.execute(query)
    stitching_data = pd.DataFrame(list(res), columns=res.keys())
    return stitching_data


def retrieve_maskings_flat(project, scene_ids):
    session = OrmSession(project)
    stitching_data = get_stitching_data(project, scene_ids, session)

    scene_probes = set(stitching_data.probe_id.tolist())
    if Config.get_cloud() == Config.GCP:
        probe_maskings = get_masks_from_rds_gcp(project, scene_probes, session=session)
    else:
        probe_maskings = get_masks_from_rds(project, scene_probes, session=session)
    for col in ['x1', 'x2', 'y1', 'y2']:
        probe_maskings[col] = probe_maskings[col].astype(float)
    return probe_maskings, stitching_data


def retrieve_maskings(project, scene_ids):
    session = OrmSession(project)
    stitching_data = get_stitching_data(project, scene_ids, session)

    scene_probes = set(stitching_data.probe_id.tolist())

    # get from the project rds
    if Config.get_cloud() == Config.GCP:
        masks_from_rds = get_masks_from_rds_gcp(project, scene_probes, session=session)
    else:
        masks_from_rds = get_masks_from_rds(project, scene_probes, session=session)

    if masks_from_rds.empty:
        rds_probes = set([])
    else:
        rds_probes = set(masks_from_rds.probe_id)
    Log.info("Missing {} out of {} probes from RDS".format(len(scene_probes - rds_probes), len(scene_probes)))
    mongo_query_probes = scene_probes - rds_probes
    scene_probes = scene_probes - rds_probes
    if len(mongo_query_probes) > 0:
        probe_masks = get_masks(project, mongo_query_probes)
        if probe_masks.empty:
            mongo_probes = set([])
        else:
            mongo_probes = set(probe_masks.probe_id)
    else:
        mongo_probes = set([])
        probe_masks = pd.DataFrame(columns=masks_from_rds.columns)

    Log.info("Missing {} out of {} probes from Mongo".format(len(scene_probes - mongo_probes), len(scene_probes)))

    athena_query_probes = scene_probes - mongo_probes
    scene_probes = scene_probes - mongo_probes
    if len(athena_query_probes) > 0:
        try:
            probe_masks_athena = get_masks_archive(project, athena_query_probes, session)
            athena_probes = set(probe_masks_athena.probe_id)
        except:
            Log.warning('Unable to connect to AWS Athena')
            probe_masks_athena = pd.DataFrame(columns=masks_from_rds.columns)
            athena_probes = set([])
    else:
        probe_masks_athena = pd.DataFrame(columns=masks_from_rds.columns)
        athena_probes = set([])

    if len(scene_probes - athena_probes) > 0:
        Log.warning("{} Probes are missing masks in all sources: {}".format(len(scene_probes - athena_probes),
                                                                            scene_probes - athena_probes))
    probe_masks = pd.concat([masks_from_rds, probe_masks, probe_masks_athena])

    return stitching_data.merge(probe_masks, on='probe_id', how='inner')
