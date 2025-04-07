import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from awsglue.dynamicframe import DynamicFrame

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

# Script generated for node Step Trainer Trusted to Curated
StepTrainerTrustedtoCurated_node1740051997352 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://mp-d609/step_trainer/trusted/"], "recurse": True}, transformation_ctx="StepTrainerTrustedtoCurated_node1740051997352")

# Script generated for node Accelerometer Trusted to Curated
AccelerometerTrustedtoCurated_node1740051993210 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://mp-d609/accelerometer/trusted/"], "recurse": True}, transformation_ctx="AccelerometerTrustedtoCurated_node1740051993210")

# Script generated for node Join
StepTrainerTrustedtoCurated_node1740051997352DF = StepTrainerTrustedtoCurated_node1740051997352.toDF()
AccelerometerTrustedtoCurated_node1740051993210DF = AccelerometerTrustedtoCurated_node1740051993210.toDF()
Join_node1740053904103 = DynamicFrame.fromDF(StepTrainerTrustedtoCurated_node1740051997352DF.join(AccelerometerTrustedtoCurated_node1740051993210DF, (StepTrainerTrustedtoCurated_node1740051997352DF['sensorreadingtime'] == AccelerometerTrustedtoCurated_node1740051993210DF['timestamp']), "left"), glueContext, "Join_node1740053904103")

# Script generated for node Machine Learning Curated
EvaluateDataQuality().process_rows(frame=Join_node1740053904103, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1740049730866", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
MachineLearningCurated_node1740052597008 = glueContext.getSink(path="s3://mp-d609/step_trainer/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="MachineLearningCurated_node1740052597008")
MachineLearningCurated_node1740052597008.setCatalogInfo(catalogDatabase="stedi",catalogTableName="machine_learning_curated")
MachineLearningCurated_node1740052597008.setFormat("json")
MachineLearningCurated_node1740052597008.writeFrame(Join_node1740053904103)
job.commit()