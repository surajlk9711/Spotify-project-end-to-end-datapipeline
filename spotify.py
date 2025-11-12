import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# Script generated for node artist
artist_node1762517348777 = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": "'", 
"withHeader": True, "separator": ","}, connection_type="s3", format="csv", connection_options={"paths": ["s3://project-spotify-datewithdata-new/staging-new/artists.csv"], "recurse": True}, transformation_ctx="artist_node1762517348777")

# Script generated for node album
album_node1762517350514 = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": "'", "withHeader": True, "separator": ","}, connection_type="s3", format="csv", connection_options={"paths": ["s3://project-spotify-datewithdata-new/staging-new/albums.csv"], "recurse": True}, transformation_ctx="album_node1762517350514")

# Script generated for node tracks
tracks_node1762517351078 = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": "'", "withHeader": True, "separator": ","}, connection_type="s3", format="csv", connection_options={"paths": ["s3://project-spotify-datewithdata-new/staging-new/track.csv"], "recurse": True}, transformation_ctx="tracks_node1762517351078")

# Script generated for node Join album & artist
Joinalbumartist_node1762517583015 = Join.apply(frame1=album_node1762517350514, frame2=artist_node1762517348777, keys1=["artist_id"], keys2=["id"], transformation_ctx="Joinalbumartist_node1762517583015")

# Script generated for node Join with tracks
Joinwithtracks_node1762517783709 = Join.apply(frame1=tracks_node1762517351078, frame2=Joinalbumartist_node1762517583015, keys1=["track_id"], keys2=["track_id"], transformation_ctx="Joinwithtracks_node1762517783709")

# Script generated for node Drop Fields
DropFields_node1762517903778 = DropFields.apply(frame=Joinwithtracks_node1762517783709, paths=["`.track_id`", "id"], transformation_ctx="DropFields_node1762517903778")

# Script generated for node Destination
EvaluateDataQuality().process_rows(frame=DropFields_node1762517903778, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1762517339703", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
Destination_node1762517991769 = glueContext.write_dynamic_frame.from_options(frame=DropFields_node1762517903778, connection_type="s3", format="glueparquet", connection_options={"path": "s3://project-spotify-datewithdata-new/datawarehouse-new/", "partitionKeys": []}, format_options={"compression": "snappy"}, transformation_ctx="Destination_node1762517991769")

job.commit()