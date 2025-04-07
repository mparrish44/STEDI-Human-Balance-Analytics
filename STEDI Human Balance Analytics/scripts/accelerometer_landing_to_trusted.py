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

# Script generated for node Accelerometer Landing
AccelerometerLanding_node1740019022685 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="accelerometer_landing", transformation_ctx="AccelerometerLanding_node1740019022685")

# Script generated for node Customer to Trusted
CustomertoTrusted_node1740120433970 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_trusted", transformation_ctx="CustomertoTrusted_node1740120433970")

# Script generated for node Join
Join_node1740120519397 = Join.apply(frame1=AccelerometerLanding_node1740019022685, frame2=CustomertoTrusted_node1740120433970, keys1=["user"], keys2=["email"], transformation_ctx="Join_node1740120519397")

# Script generated for node Accelerometer Trusted
EvaluateDataQuality().process_rows(frame=Join_node1740120519397, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1740119869999", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AccelerometerTrusted_node1740120673962 = glueContext.getSink(path="s3://mp-d609/accelerometer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AccelerometerTrusted_node1740120673962")
AccelerometerTrusted_node1740120673962.setCatalogInfo(catalogDatabase="stedi",catalogTableName="accelerometer_trusted")
AccelerometerTrusted_node1740120673962.setFormat("json")
AccelerometerTrusted_node1740120673962.writeFrame(Join_node1740120519397)
job.commit()