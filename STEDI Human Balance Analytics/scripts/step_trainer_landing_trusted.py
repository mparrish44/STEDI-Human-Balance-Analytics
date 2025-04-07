import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import col

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Load Customer Curated Data
CustomerCurated_node = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={"paths": ["s3://mp-d609/customer/curated/"], "recurse": True},
    transformation_ctx="CustomerCurated_node",
)

# Load Step Trainer Landing Data
StepTrainerLanding_node = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={"paths": ["s3://mp-d609/step_trainer/landing/"], "recurse": True},
    transformation_ctx="StepTrainerLanding_node",
)

# Convert to DataFrames for Spark SQL operations
CustomerCuratedDF = CustomerCurated_node.toDF()
StepTrainerLandingDF = StepTrainerLanding_node.toDF()

# Debugging: Print row counts
print(f"CustomerCuratedDF count: {CustomerCuratedDF.count()}")
print(f"StepTrainerLandingDF count: {StepTrainerLandingDF.count()}")

# Debugging: Print schema
print("CustomerCuratedDF Schema:")
CustomerCuratedDF.printSchema()

print("StepTrainerLandingDF Schema:")
StepTrainerLandingDF.printSchema()

# Ensure Column Names are Properly Referenced
CustomerCuratedDF = CustomerCuratedDF.withColumnRenamed("serialNumber", "customer_serialNumber")
StepTrainerLandingDF = StepTrainerLandingDF.withColumnRenamed("serialNumber", "step_serialNumber")

# Perform INNER JOIN with DISTINCT to remove duplicate rows
StepTrainerTrustedDF = StepTrainerLandingDF.join(
    CustomerCuratedDF,
    StepTrainerLandingDF["step_serialNumber"] == CustomerCuratedDF["customer_serialNumber"],
    "inner"
).select(
    col("sensorReadingTime"), 
    col("step_serialNumber").alias("serialNumber"), 
    col("distanceFromObject")
).distinct()

# Handle cases where the join returns empty
if StepTrainerTrustedDF.count() == 0:
    print("Join resulted in 0 rows. Skipping write operation.")
    job.commit()
    sys.exit(0)

# Convert back to DynamicFrame
StepTrainerTrustedDF = DynamicFrame.fromDF(StepTrainerTrustedDF, glueContext, "StepTrainerTrustedDF")

# Write Output to S3 in JSON format (No Compression)
glueContext.write_dynamic_frame.from_options(
    frame=StepTrainerTrustedDF,
    connection_type="s3",
    format="json",
    connection_options={"path": "s3://mp-d609/step_trainer/trusted/"},
    transformation_ctx="StepTrainerTrusted_node",
)

job.commit()
