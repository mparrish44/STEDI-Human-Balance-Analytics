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

# Script generated for node Customer Trusted to Curated
CustomerTrustedtoCurated_node1740031611291 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://mp-d609/customer/trusted/"], "recurse": True}, transformation_ctx="CustomerTrustedtoCurated_node1740031611291")

# Script generated for node Accelerated Trusted to Curated
AcceleratedTrustedtoCurated_node1740031761042 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://mp-d609/accelerometer/trusted/"], "recurse": True}, transformation_ctx="AcceleratedTrustedtoCurated_node1740031761042")

# Script generated for node Join
Join_node1740032190553 = Join.apply(frame1=CustomerTrustedtoCurated_node1740031611291, frame2=AcceleratedTrustedtoCurated_node1740031761042, keys1=["email"], keys2=["user"], transformation_ctx="Join_node1740032190553")

# Script generated for node Customer Curated
EvaluateDataQuality().process_rows(frame=Join_node1740032190553, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1740030492954", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
CustomerCurated_node1740032348339 = glueContext.getSink(path="s3://mp-d609/customer/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="CustomerCurated_node1740032348339")
CustomerCurated_node1740032348339.setCatalogInfo(catalogDatabase="stedi",catalogTableName="customer_curated")
CustomerCurated_node1740032348339.setFormat("json")
CustomerCurated_node1740032348339.writeFrame(Join_node1740032190553)
job.commit()