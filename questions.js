const quizData = [
    {
        "question": "A Spark application developer wants to identify which operations cause shuffling, leading to a\nnew stage in the Spark execution plan.\nWhich operation results in a shuffle and a new stage?",
        "options": [
            "A. DataFrame.groupBy().agg()",
            "B. DataFrame.filter()",
            "C. DataFrame.withColumn()",
            "D. DataFrame.select()"
        ],
        "answer": [
            "A"
        ],
        "explanation": "Operations that trigger data movement across partitions (like groupBy, join, repartition) result\nin a shuffle and a new stage.\nFrom Spark documentation:\n\"groupBy and aggregation cause data to be shuffled across partitions to combine rows with\nthe same key.\" Option A (groupBy + agg) → causes shuffle.\nOptions B, C, and D (filter, withColumn, select) → transformations that do not require\nshuffling; they are narrow dependencies.\nFinal answer: A"
    },
    {
        "question": "A data scientist has been investigating user profile data to build features for their model. After\nsome exploratory data analysis, the data scientist identified that some records in the user\nprofiles contain NULL values in too many fields to be useful.\nThe schema of the user profile table looks like this:\nuser_id STRING,\nusername STRING,\ndate_of_birth DATE,\ncountry STRING,\ncreated_at TIMESTAMP\nThe data scientist decided that if any record contains a NULL value in any field, they want to\nremove that record from the output before further processing.\nWhich block of Spark code can be used to achieve these requirements?",
        "options": [
            "A. filtered_users = raw_users.na.drop(\"any\")",
            "B. filtered_users = raw_users.na.drop(\"all\")",
            "C. filtered_users = raw_users.dropna(how=\"any\")",
            "D. filtered_users = raw_users.dropna(how=\"all\")"
        ],
        "answer": [
            "C"
        ],
        "explanation": "In Spark's DataFrame API, the dropna() (or equivalently, DataFrameNaFunctions.drop())\nmethod removes rows containing null values.\nBehavior:\nhow=\"any\" → drops rows where any column has a null value.\nhow=\"all\" → drops rows where all columns are null.\n\n\nSince the data scientist wants to drop records with any null field, the correct parameter is\nhow=\"any\".\nCorrect syntax:\nfiltered_users = raw_users.dropna(how=\"any\")\nThis will remove all records that have at least one null value in any column.\nWhy the other options are incorrect:\nA: Uses na.drop(\"any\") but missing parentheses context (works only as\nraw_users.na.drop(\"any\"), which is equivalent to option C).\nB/D: how=\"all\" only removes rows where all values are null - too strict for this use case.\nReference:\nPySpark DataFrame API - DataFrameNaFunctions.drop() and DataFrame.dropna().\nDatabricks Exam Guide (June 2025): Section \"Developing Apache Spark\nDataFrame/DataSet API Applications\" - covers handling missing data and DataFrame\ncleaning operations."
    },
    {
        "question": "An engineer has two DataFrames: df1 (small) and df2 (large). A broadcast join is used:\nfrom pyspark.sql.functions import broadcast\nresult = df2.join(broadcast(df1), on='id', how='inner')\nWhat is the purpose of using broadcast() in this scenario?\nOptions:",
        "options": [
            "A. It filters the id values before performing the join.",
            "B. It increases the partition size for df1 and df2.",
            "C. It reduces the number of shuffle operations by replicating the smaller DataFrame to all\nnodes.",
            "D. It ensures that the join happens only when the id values are identical."
        ],
        "answer": [
            "C"
        ],
        "explanation": "broadcast(df1) tells Spark to send the small DataFrame (df1) to all worker nodes.\nThis eliminates the need for shuffling df1 during the join.\nBroadcast joins are optimized for scenarios with one large and one small table."
    },
    {
        "question": "A developer runs:\ndf.write.partitionBy(\"color\", \"fruit\").parquet(\"/path/to/output\")\nWhat is the result?\nOptions:",
        "options": [
            "A. It stores all data in a single Parquet file.",
            "B. It throws an error if there are null values in either partition column.",
            "C. It appends new partitions to an existing Parquet file.",
            "D. It creates separate directories for each unique combination of color and fruit."
        ],
        "answer": [
            "D"
        ],
        "explanation": "The partitionBy() method in Spark organizes output into subdirectories based on unique\ncombinations of the specified columns:\ne.g.\n/path/to/output/color=red/fruit=apple/part-0000.parquet\n/path/to/output/color=green/fruit=banana/part-0001.parquet\nThis improves query performance via partition pruning.\nIt does not consolidate into a single file.\nNull values are allowed in partitions.\nIt does not \"append\" unless .mode(\"append\") is used."
    },
    {
        "question": "A data scientist is analyzing a large dataset and has written a PySpark script that includes\nseveral transformations and actions on a DataFrame. The script ends with a collect() action\nto retrieve the results.\nHow does Apache Spark™'s execution hierarchy process the operations when the data\nscientist runs this script?",
        "options": [
            "A. The script is first divided into multiple applications, then each application is split into jobs,\nstages, and finally tasks.",
            "B. The entire script is treated as a single job, which is then divided into multiple stages, and\neach stage is further divided into tasks based on data partitions.",
            "C. The collect() action triggers a job, which is divided into stages at shuffle boundaries, and\neach stage is split into tasks that operate on individual data partitions.",
            "D. Spark creates a single task for each transformation and action in the script, and these\ntasks are grouped into stages and jobs based on their dependencies."
        ],
        "answer": [
            "C"
        ],
        "explanation": "In Apache Spark, the execution hierarchy is structured as follows:\nApplication: The highest-level unit, representing the user program built on Spark.\nJob: Triggered by an action (e.g., collect(), count()). Each action corresponds to a job.\nStage: A job is divided into stages based on shuffle boundaries. Each stage contains tasks\nthat can be executed in parallel.\nTask: The smallest unit of work, representing a single operation applied to a partition of the\ndata.\nWhen the collect() action is invoked, Spark initiates a job. This job is then divided into stages\nat points where data shuffling is required (i.e., wide transformations). Each stage comprises\ntasks that are distributed across the cluster's executors, operating on individual data\npartitions.\nThis hierarchical execution model allows Spark to efficiently process large-scale data by\nparallelizing tasks and optimizing resource utilization."
    },
    {
        "question": "A data engineer has written the following code to join two DataFrames df1 and df2:\ndf1 = spark.read.csv(\"sales_data.csv\")\n\n\ndf2 = spark.read.csv(\"product_data.csv\")\ndf_joined = df1.join(df2, df1.product_id == df2.product_id)\nThe DataFrame df1 contains ~10 GB of sales data, and df2 contains ~8 MB of product data.\nWhich join strategy will Spark use?",
        "options": [
            "A. Shuffle join, as the size difference between df1 and df2 is too large for a broadcast join to\nwork efficiently.",
            "B. Shuffle join, because AQE is not enabled, and Spark uses a static query plan.",
            "C. Shuffle join because no broadcast hints were provided.",
            "D. Broadcast join, as df2 is smaller than the default broadcast threshold."
        ],
        "answer": [
            "D"
        ],
        "explanation": "Spark automatically uses a broadcast hash join when one side of the join is small enough to\nfit within the broadcast threshold.\nDefault threshold:\nspark.sql.autoBroadcastJoinThreshold = 10MB (as of Spark 3.5)\nSince df2 is 8 MB, Spark automatically broadcasts it to all executors. This avoids a shuffle on\nthe large dataset (df1) and speeds up the join.\nWhy the other options are incorrect:\nA: 8 MB < 10 MB threshold → broadcast join is efficient.\nB: AQE is not required for broadcast joins; it's a static optimization.\nC: Broadcast hints are optional - Spark infers automatically.\nReference:\nDatabricks Exam Guide (June 2025): Section \"Troubleshooting and Tuning Apache Spark\nDataFrame API Applications\" - broadcast joins and optimization.\nSpark SQL Join Strategies - Broadcast hash join and shuffle join thresholds."
    },
    {
        "question": "A data engineer is reviewing a Spark application that applies several transformations to a\nDataFrame but notices that the job does not start executing immediately.\nWhich two characteristics of Apache Spark's execution model explain this behavior? (Choose\n2 answers)",
        "options": [
            "A. Transformations are executed immediately to build the lineage graph.",
            "B. The Spark engine optimizes the execution plan during the transformations, causing\ndelays.",
            "C. Transformations are evaluated lazily.",
            "D. The Spark engine requires manual intervention to start executing transformations.",
            "E. Only actions trigger the execution of the transformation pipeline."
        ],
        "answer": [
            "C",
            "E"
        ],
        "explanation": "Apache Spark follows a lazy evaluation model, meaning transformations (like filter(), select(),\nmap()) are not executed immediately. Instead, they build a logical plan (lineage graph) that\nrepresents the sequence of operations to be applied.\nExecution only begins when an action (e.g., count(), collect(), save(), show()) is called. At that\n\n\npoint, Spark's engine:\nOptimizes the logical plan into a physical plan.\nDivides it into stages and tasks.\nExecutes them across the cluster.\nThis design helps Spark optimize execution paths and avoid unnecessary computations.\nWhy the other options are incorrect:\nA: Transformations do not execute immediately; they are deferred.\nB: Optimization happens during job execution (after an action), not during transformations.\nD: Execution starts automatically once an action is triggered, no manual intervention needed.\nReference:\nDatabricks Exam Guide (June 2025): Section \"Apache Spark Architecture and Components\"\n- covers lazy evaluation, actions vs. transformations, and execution hierarchy.\nSpark 3.5 Documentation - Lazy Evaluation model and DAG scheduling."
    },
    {
        "question": "What is the behavior of the function date_sub(start, days) if a negative value is passed into\nthe days parameter?",
        "options": [
            "A. The number of days specified will be added to the start date.",
            "B. An error message of an invalid parameter will be returned.",
            "C. The same start date will be returned.",
            "D. The number of days specified will be removed from the start date."
        ],
        "answer": [
            "A"
        ],
        "explanation": "In Spark SQL, the function date_sub(startDate, days) returns the date that is days before\nstartDate.\nIf the days parameter is negative, Spark interprets it as subtracting a negative number, which\neffectively adds days to the date.\nExample:\nfrom pyspark.sql.functions import date_sub, lit\ndf = spark.createDataFrame([( \"2024-10-01\", )], [\"dt\"])\ndf.select(date_sub(\"dt\", -5).alias(\"new_date\")).show()\nOutput:\n+----------+\n| new_date |\n+----------+\n|2024-10-06|\n+----------+\nWhy the other options are incorrect:\nB: No error occurs; negative values are supported.\nC: The start date changes if days ≠ 0.\nD: Subtracting days would move the date backward, not forward.\nReference:\nSpark SQL Functions - date_sub(startDate, days) and date_add(startDate, days) behavior.\nDatabricks Exam Guide (June 2025): Section \"Using Spark SQL\" - working with date and\n\n\ntimestamp functions."
    },
    {
        "question": "A data scientist at a large e-commerce company needs to process and analyze 2 TB of daily\ncustomer transaction data. The company wants to implement real-time fraud detection and\npersonalized product recommendations.\nCurrently, the company uses a traditional relational database system, which struggles with\nthe increasing data volume and velocity.\nWhich feature of Apache Spark effectively addresses this challenge?",
        "options": [
            "A. Ability to process small datasets efficiently",
            "B. In-memory computation and parallel processing capabilities",
            "C. Support for SQL queries on structured data",
            "D. Built-in machine learning libraries"
        ],
        "answer": [
            "B"
        ],
        "explanation": "Apache Spark was designed for big data and high-velocity workloads. Its core strength lies in\nits in-memory computation and parallel distributed processing model.\nThese features allow Spark to:\nProcess large-scale datasets quickly across many nodes.\nSupport real-time and near-real-time analytics for tasks like fraud detection and\nrecommendations.\nMinimize disk I/O through caching and memory persistence.\nThus, the key advantage in this use case is Spark's ability to handle large data volumes\nefficiently using distributed, in-memory computation.\nWhy the other options are incorrect:\nA: Spark is optimized for large, not small, datasets.\nC: SQL support is useful but doesn't solve the scalability issue.\nD: MLlib supports machine learning but relies on Spark's parallel computation for speed.\nReference:\nDatabricks Exam Guide (June 2025): Section \"Apache Spark Architecture and Components\"\n- identifies Spark's advantages: in-memory processing, distributed computation, and\nscalability.\nApache Spark 3.5 Overview - Key design goals and cluster computation model."
    },
    {
        "question": "A Spark application is experiencing performance issues in client mode due to the driver being\nresource-constrained.\nHow should this issue be resolved?",
        "options": [
            "A. Switch the deployment mode to cluster mode.",
            "B. Add more executor instances to the cluster.",
            "C. Increase the driver memory on the client machine.",
            "D. Switch the deployment mode to local mode."
        ],
        "answer": [
            "A"
        ],
        "explanation": "In client mode, the driver runs on the same machine that submitted the job (often a\ndeveloper's workstation). If the driver has insufficient memory or CPU, it becomes a\nbottleneck.\nSolution: Run the job in cluster mode.\nIn cluster mode, the driver runs inside the cluster on a worker node, benefiting from\ndistributed cluster resources and improved performance for large workloads.\nWhy the other options are incorrect:\nB: Executors handle tasks, not driver overhead.\nC: May help temporarily but doesn't scale; cluster mode is best practice.\nD: Local mode runs everything on one JVM - worse for large workloads.\nReference:\nDatabricks Exam Guide (June 2025): Section \"Using Spark Connect to Deploy Applications\" -\nexplains client vs. cluster deployment modes.\nSpark Deployment Overview - driver behavior and resource management."
    },
    {
        "question": "A Spark application suffers from too many small tasks due to excessive partitioning. How can\nthis be fixed without a full shuffle?\nOptions:",
        "options": [
            "A. Use the distinct() transformation to combine similar partitions",
            "B. Use the coalesce() transformation with a lower number of partitions",
            "C. Use the sortBy() transformation to reorganize the data",
            "D. Use the repartition() transformation with a lower number of partitions"
        ],
        "answer": [
            "B"
        ],
        "explanation": "coalesce(n) reduces the number of partitions without triggering a full shuffle, unlike\nrepartition().\nThis is ideal when reducing partition count, especially during write operations."
    },
    {
        "question": "A developer needs to produce a Python dictionary using data stored in a small Parquet table,\nwhich looks like this:\n\n\n \nThe resulting Python dictionary must contain a mapping of region -> region id containing the\nsmallest 3 region_id values.\nWhich code fragment meets the requirements?\nA)\n \nB)\n \nC)\n\n\n \nD)\n \nThe resulting Python dictionary must contain a mapping of region -> region_id for the\nsmallest 3 region_id values.\nWhich code fragment meets the requirements?",
        "options": [
            "A. regions = dict(\nregions_df\n.select('region', 'region_id')\n.sort('region_id')\n.take(3)\n)",
            "B. regions = dict(\nregions_df\n.select('region_id', 'region')\n.sort('region_id')\n.take(3)\n)",
            "C. regions = dict(\nregions_df\n.select('region_id', 'region')\n.limit(3)\n\n\n.collect()\n)",
            "D. regions = dict(\nregions_df\n.select('region', 'region_id')\n.sort(desc('region_id'))\n.take(3)\n)"
        ],
        "answer": [
            "A"
        ],
        "explanation": "The question requires creating a dictionary where keys are region values and values are the\ncorresponding region_id integers. Furthermore, it asks to retrieve only the smallest 3\nregion_id values.\nKey observations:\n.select('region', 'region_id') puts the column order as expected by dict() - where the first\ncolumn becomes the key and the second the value.\n.sort('region_id') ensures sorting in ascending order so the smallest IDs are first.\n.take(3) retrieves exactly 3 rows.\nWrapping the result in dict(...) correctly builds the required Python dictionary: { 'AFRICA': 0,\n'AMERICA': 1, 'ASIA': 2 }.\nIncorrect options:\nOption B flips the order to region_id first, resulting in a dictionary with integer keys - not what's\nasked.\nOption C uses .limit(3) without sorting, which leads to non-deterministic rows based on\npartition layout.\nOption D sorts in descending order, giving the largest rather than smallest region_ids.\nHence, Option A meets all the requirements precisely."
    },
    {
        "question": "A data engineer is streaming data from Kafka and requires:\nMinimal latency\nExactly-once processing guarantees\nWhich trigger mode should be used?",
        "options": [
            "A. .trigger(processingTime='1 second')",
            "B. .trigger(continuous=True)",
            "C. .trigger(continuous='1 second')",
            "D. .trigger(availableNow=True)"
        ],
        "answer": [
            "A"
        ],
        "explanation": "Exactly-once guarantees in Spark Structured Streaming require micro-batch mode (default),\nnot continuous mode.\nContinuous mode (.trigger(continuous=...)) only supports at-least-once semantics and lacks\nfull fault-tolerance.\ntrigger(availableNow=True) is a batch-style trigger, not suited for low-latency streaming.\n\n\nSo:\nOption A uses micro-batching with a tight trigger interval → minimal latency + exactly-once\nguarantee.\nFinal answer: A"
    },
    {
        "question": "A developer notices that all the post-shuffle partitions in a dataset are smaller than the value\nset for spark.sql.adaptive.maxShuffledHashJoinLocalMapThreshold.\nWhich type of join will Adaptive Query Execution (AQE) choose in this case?",
        "options": [
            "A. A Cartesian join",
            "B. A shuffled hash join",
            "C. A broadcast nested loop join",
            "D. A sort-merge join"
        ],
        "answer": [
            "B"
        ],
        "explanation": "Adaptive Query Execution (AQE) dynamically selects join strategies based on actual data\nsizes at runtime. If the size of post-shuffle partitions is below the threshold set by:\nspark.sql.adaptive.maxShuffledHashJoinLocalMapThreshold\nthen Spark prefers to use a shuffled hash join.\nFrom the Spark documentation:\n\"AQE selects a shuffled hash join when the size of post-shuffle data is small enough to fit\nwithin the configured threshold, avoiding more expensive sort-merge joins.\" Therefore:\nA is wrong - Cartesian joins are only used with no join condition.\nB is correct - this is the optimized join for small partitioned shuffle data under AQE.\nC and D are used under other scenarios but not for this case.\nFinal answer: B"
    },
    {
        "question": "What is the behavior for function date_sub(start, days) if a negative value is passed into the\ndays parameter?",
        "options": [
            "A. The same start date will be returned",
            "B. An error message of an invalid parameter will be returned",
            "C. The number of days specified will be added to the start date",
            "D. The number of days specified will be removed from the start date"
        ],
        "answer": [
            "C"
        ],
        "explanation": "The function date_sub(start, days) subtracts the number of days from the start date. If a\nnegative number is passed, the behavior becomes a date addition.\nExample:\nSELECT date_sub('2024-05-01', -5)\n-- Returns: 2024-05-06\nSo, a negative value effectively adds the absolute number of days to the date."
    },
    {
        "question": "A developer created a DataFrame with columns color, fruit, and taste, and wrote the data to a\nParquet directory using:\ndf.write.partitionBy(\"color\", \"taste\").parquet(\"/path/to/output\")\nWhat is the result of this code?",
        "options": [
            "A. It appends new partitions to an existing Parquet file.",
            "B. It throws an error if there are null values in either partition column.",
            "C. It creates separate directories for each unique combination of color and taste.",
            "D. It stores all data in a single Parquet file."
        ],
        "answer": [
            "C"
        ],
        "explanation": "When writing a DataFrame using .partitionBy() in Spark, the data is physically organized into\ndirectory structures corresponding to unique combinations of the partition columns.\nExample:\n/path/to/output/color=Red/taste=Sweet/part-0001.parquet\n/path/to/output/color=Green/taste=Sour/part-0002.parquet\nThis structure improves query performance by pruning partitions when filtering on these\ncolumns.\nWhy the other options are incorrect:\nA: Appending requires .mode(\"append\"), which isn't used here.\nB: Null values in partition columns are handled; they don't raise errors.\nD: Partitioning prevents storing all data in a single file.\nReference:\nPySpark DataFrameWriter API - partitionBy() and .parquet() methods.\nDatabricks Exam Guide (June 2025): Section \"Using Spark SQL\" - partitioning and writing\noptimized output files."
    },
    {
        "question": "A developer initializes a SparkSession:\n \nspark = SparkSession.builder \\\n.appName(\"Analytics Application\") \\\n.getOrCreate()\nWhich statement describes the spark SparkSession?",
        "options": [
            "A. The getOrCreate() method explicitly destroys any existing SparkSession and creates a\nnew one.",
            "B. A SparkSession is unique for each appName, and calling getOrCreate() with the same\nname will return an existing SparkSession once it has been created.",
            "C. If a SparkSession already exists, this code will return the existing session instead of\ncreating a new one.",
            "D. A new SparkSession is created every time the getOrCreate() method is invoked."
        ],
        "answer": [
            "C"
        ],
        "explanation": "According to the PySpark API documentation:\n\"getOrCreate(): Gets an existing SparkSession or, if there is no existing one, creates a new\none based on the options set in this builder.\" This means Spark maintains a global singleton\nsession within a JVM process. Repeated calls to getOrCreate() return the same session,\nunless explicitly stopped.\nOption A is incorrect: the method does not destroy any session.\nOption B incorrectly ties uniqueness to appName, which does not influence session\nreusability.\nOption D is incorrect: it contradicts the fundamental behavior of getOrCreate().\n(Source: PySpark SparkSession API Docs)"
    },
    {
        "question": "In the code block below, aggDF contains aggregations on a streaming DataFrame:\n \nWhich output mode at line 3 ensures that the entire result table is written to the console\nduring each trigger execution?",
        "options": [
            "A. complete",
            "B. append",
            "C. replace",
            "D. aggregate"
        ],
        "answer": [
            "A"
        ],
        "explanation": "The correct output mode for streaming aggregations that need to output the full updated\nresults at each trigger is \"complete\".\nFrom the official documentation:\n\"complete: The entire updated result table will be output to the sink every time there is a\ntrigger.\" This is ideal for aggregations, such as counts or averages grouped by a key, where\nthe result table changes incrementally over time.\nappend: only outputs newly added rows\nreplace and aggregate: invalid values for output mode"
    },
    {
        "question": "A data scientist at an e-commerce company is working with user data obtained from its\n\n\nsubscriber database and has stored the data in a DataFrame df_user.\nBefore further processing, the data scientist wants to create another DataFrame\ndf_user_non_pii and store only the non-PII columns.\nThe PII columns in df_user are name, email, and birthdate.\nWhich code snippet can be used to meet this requirement?",
        "options": [
            "A. df_user_non_pii = df_user.drop(\"name\", \"email\", \"birthdate\")",
            "B. df_user_non_pii = df_user.dropFields(\"name\", \"email\", \"birthdate\")",
            "C. df_user_non_pii = df_user.select(\"name\", \"email\", \"birthdate\")",
            "D. df_user_non_pii = df_user.remove(\"name\", \"email\", \"birthdate\")"
        ],
        "answer": [
            "A"
        ],
        "explanation": "To exclude sensitive (PII) columns from a DataFrame, the easiest method is to use the\n.drop() function with the list of column names to remove.\nCorrect syntax:\ndf_user_non_pii = df_user.drop(\"name\", \"email\", \"birthdate\")\nThis creates a new DataFrame containing all remaining columns.\nWhy the other options are incorrect:\nB: .dropFields() is not valid for standard DataFrames - it's used for struct fields only.\nC: .select() would keep only PII columns, not remove them.\nD: .remove() does not exist in Spark DataFrame API.\nReference:\nPySpark DataFrame API - drop() method for removing multiple columns.\nDatabricks Exam Guide (June 2025): Section \"Developing Apache Spark\nDataFrame/DataSet API Applications\" - data manipulation, selecting, and dropping columns."
    },
    {
        "question": "What is the risk associated with this operation when converting a large Pandas API on Spark\nDataFrame back to a Pandas DataFrame?",
        "options": [
            "A. The conversion will automatically distribute the data across worker nodes",
            "B. The operation will fail if the Pandas DataFrame exceeds 1000 rows",
            "C. Data will be lost during conversion",
            "D. The operation will load all data into the driver's memory, potentially causing memory\noverflow"
        ],
        "answer": [
            "D"
        ],
        "explanation": "When you convert a large pyspark.pandas (aka Pandas API on Spark) DataFrame to a local\nPandas DataFrame using .toPandas(), Spark collects all partitions to the driver.\nFrom the Spark documentation:\n\"Be careful when converting large datasets to Pandas. The entire dataset will be pulled into\nthe driver's memory.\" Thus, for large datasets, this can cause memory overflow or out-of-\n\n\nmemory errors on the driver.\nFinal answer: D"
    },
    {
        "question": "A data engineer is running a batch processing job on a Spark cluster with the following\nconfiguration:\n10 worker nodes\n16 CPU cores per worker node\n64 GB RAM per node\nThe data engineer wants to allocate four executors per node, each executor using four cores.\nWhat is the total number of CPU cores used by the application?",
        "options": [
            "A. 160",
            "B. 64",
            "C. 80",
            "D. 40"
        ],
        "answer": [
            "A"
        ],
        "explanation": "10 nodes × 4 executors/node = 40 executors.\n40 executors × 4 cores/executor = 160 cores total.\nFinal answer: A (160)"
    },
    {
        "question": "A data engineer needs to join multiple DataFrames and has written the following code:\nfrom pyspark.sql.functions import broadcast\ndata1 = [(1, \"A\"), (2, \"B\")]\ndata2 = [(1, \"X\"), (2, \"Y\")]\ndata3 = [(1, \"M\"), (2, \"N\")]\ndf1 = spark.createDataFrame(data1, [\"id\", \"val1\"])\ndf2 = spark.createDataFrame(data2, [\"id\", \"val2\"])\ndf3 = spark.createDataFrame(data3, [\"id\", \"val3\"])\ndf_joined = df1.join(broadcast(df2), \"id\", \"inner\") \\\n.join(broadcast(df3), \"id\", \"inner\")\nWhat will be the output of this code?",
        "options": [
            "A. The code will work correctly and perform two broadcast joins simultaneously to join df1\n\n\nwith df2, and then the result with df3.",
            "B. The code will fail because only one broadcast join can be performed at a time.",
            "C. The code will fail because the second join condition (df2.id == df3.id) is incorrect.",
            "D. The code will result in an error because broadcast() must be called before the joins, not\ninline."
        ],
        "answer": [
            "A"
        ],
        "explanation": "Spark supports multiple broadcast joins in a single query plan, as long as each broadcasted\nDataFrame is small enough to fit under the configured threshold.\nExecution Plan:\nSpark broadcasts df2 to all executors.\nJoins df1 (big) with broadcasted df2.\nThen broadcasts df3 and performs another join with the intermediate result.\nThe result is efficient and avoids shuffling large data.\nWhy the other options are incorrect:\nB: Multiple broadcast joins are supported in Spark 3.x.\nC: The join condition is correct since all use id as the key.\nD: broadcast() can be used inline; it's valid syntax.\nReference:\nPySpark SQL Functions - broadcast() usage.\nDatabricks Exam Guide (June 2025): Section \"Developing Apache Spark\nDataFrame/DataSet API Applications\" - multiple broadcast join optimization."
    },
    {
        "question": "What is the benefit of Adaptive Query Execution (AQE)?",
        "options": [
            "A. It allows Spark to optimize the query plan before execution but does not adapt during\nruntime.",
            "B. It enables the adjustment of the query plan during runtime, handling skewed data,\noptimizing join strategies, and improving overall query performance.",
            "C. It optimizes query execution by parallelizing tasks and does not adjust strategies based on\nruntime metrics like data skew.",
            "D. It automatically distributes tasks across nodes in the clusters and does not perform\nruntime adjustments to the query plan."
        ],
        "answer": [
            "B"
        ],
        "explanation": "Adaptive Query Execution (AQE) is a powerful optimization framework introduced in Apache\nSpark 3.0 and enabled by default since Spark 3.2. It dynamically adjusts query execution\nplans based on runtime statistics, leading to significant performance improvements. The key\nbenefits of AQE include:\nDynamic Join Strategy Selection: AQE can switch join strategies at runtime. For instance, it\ncan convert a sort-merge join to a broadcast hash join if it detects that one side of the join is\nsmall enough to be broadcasted, thus optimizing the join operation .\nHandling Skewed Data: AQE detects skewed partitions during join operations and splits them\ninto smaller partitions. This approach balances the workload across tasks, preventing\nscenarios where certain tasks take significantly longer due to data skew .\n\n\nCoalescing Post-Shuffle Partitions: AQE dynamically coalesces small shuffle partitions into\nlarger ones based on the actual data size, reducing the overhead of managing numerous\nsmall tasks and improving overall query performance .\nThese runtime optimizations allow Spark to adapt to the actual data characteristics during\nquery execution, leading to more efficient resource utilization and faster query processing\ntimes."
    },
    {
        "question": "A data engineer writes the following code to join two DataFrames df1 and df2:\ndf1 = spark.read.csv(\"sales_data.csv\") # ~10 GB\ndf2 = spark.read.csv(\"product_data.csv\") # ~8 MB\nresult = df1.join(df2, df1.product_id == df2.product_id)\nWhich join strategy will Spark use?",
        "options": [
            "A. Shuffle join, because AQE is not enabled, and Spark uses a static query plan",
            "B. Broadcast join, as df2 is smaller than the default broadcast threshold",
            "C. Shuffle join, as the size difference between df1 and df2 is too large for a broadcast join to\nwork efficiently",
            "D. Shuffle join because no broadcast hints were provided"
        ],
        "answer": [
            "B"
        ],
        "explanation": "The default broadcast join threshold in Spark is:\nspark.sql.autoBroadcastJoinThreshold = 10MB\nSince df2 is only 8 MB (less than 10 MB), Spark will automatically apply a broadcast join\nwithout requiring explicit hints.\nFrom the Spark documentation:\n\"If one side of the join is smaller than the broadcast threshold, Spark will automatically\nbroadcast it to all executors.\" A is incorrect because Spark does support auto broadcast even\nwith static plans.\nB is correct: Spark will automatically broadcast df2.\nC and D are incorrect because Spark's default logic handles this optimization.\nFinal answer: B"
    },
    {
        "question": "A data engineer has noticed that upgrading the Spark version in their applications from Spark\n3.0 to Spark 3.5 has improved the runtime of some scheduled Spark applications.\nLooking further, the data engineer realizes that Adaptive Query Execution (AQE) is now\nenabled.\nWhich operation should AQE be implementing to automatically improve the Spark application\nperformance?",
        "options": [
            "A. Dynamically switching join strategies",
            "B. Collecting persistent table statistics and storing them in the metastore for future use",
            "C. Improving the performance of single-stage Spark jobs",
            "D. Optimizing the layout of Delta files on disk"
        ],
        "answer": [
            "A"
        ],
        "explanation": "Adaptive Query Execution (AQE) in Spark 3.x automatically optimizes query plans at runtime\nbased on the actual data characteristics observed during job execution.\nKey features of AQE include:\nDynamic switching of join strategies: Changes between sort-merge join and broadcast join\nbased on actual shuffle sizes.\nCoalescing shuffle partitions: Reduces small tasks and improves parallelism efficiency.\nHandling skew joins: Dynamically splits large partitions to avoid data skew.\nThus, the most accurate answer describing AQE's function is \"dynamically switching join\nstrategies.\" Why the other options are incorrect:\nB: Table statistics are collected manually or by the metastore, not by AQE.\nC: AQE benefits multi-stage jobs involving shuffles, not single-stage jobs.\nD: Delta file optimization is handled by Databricks utilities, not AQE.\nReference:\nDatabricks Exam Guide (June 2025): Section \"Troubleshooting and Tuning Apache Spark\nDataFrame API Applications\" - covers AQE and its benefits.\nSpark 3.5 Release Notes - Adaptive Query Execution dynamic optimizations."
    },
    {
        "question": "A data engineer wants to create a Streaming DataFrame that reads from a Kafka topic called\nfeed.\nWhich code fragment should be inserted in line 5 to meet the requirement?\nCode context:\nspark \\\n.readStream \\\n.format(\"kafka\") \\\n.option(\"kafka.bootstrap.servers\", \"host1:port1,host2:port2\") \\\n.[LINE 5] \\\n.load()\nOptions:",
        "options": [
            "A. .option(\"subscribe\", \"feed\")",
            "B. .option(\"subscribe.topic\", \"feed\")",
            "C. .option(\"kafka.topic\", \"feed\")",
            "D. .option(\"topic\", \"feed\")"
        ],
        "answer": [
            "A"
        ],
        "explanation": "To read from a specific Kafka topic using Structured Streaming, the correct syntax is:\n.option(\"subscribe\", \"feed\")\nThis is explicitly defined in the Spark documentation:\n\"subscribe - The Kafka topic to subscribe to. Only one topic can be specified for this option.\"\n(Source: Apache Spark Structured Streaming + Kafka Integration Guide)\n\"subscribe - The Kafka topic to subscribe to. Only one topic can be specified for this option.\"\n(Source: Apache Spark Structured Streaming + Kafka Integration Guide) B . \"subscribe.topic\"\nis invalid.\nC . \"kafka.topic\" is not a recognized option.\nD . \"topic\" is not valid for Kafka source in Spark."
    },
    {
        "question": "A developer is trying to join two tables, sales.purchases_fct and sales.customer_dim, using\nthe following code:\nfact_df = purch_df.join(cust_df, F.col('customer_id') == F.col('custid')) The developer has\ndiscovered that customers in the purchases_fct table that do not exist in the customer_dim\ntable are being dropped from the joined table.\nWhich change should be made to the code to stop these customer records from being\ndropped?",
        "options": [
            "A. fact_df = purch_df.join(cust_df, F.col('customer_id') == F.col('custid'), 'left')",
            "B. fact_df = cust_df.join(purch_df, F.col('customer_id') == F.col('custid'))",
            "C. fact_df = purch_df.join(cust_df, F.col('cust_id') == F.col('customer_id'))",
            "D. fact_df = purch_df.join(cust_df, F.col('customer_id') == F.col('custid'), 'right_outer')"
        ],
        "answer": [
            "A"
        ],
        "explanation": "In Spark, the default join type is an inner join, which returns only the rows with matching keys\nin both DataFrames. To retain all records from the left DataFrame (purch_df) and include\nmatching records from the right DataFrame (cust_df), a left outer join should be used.\nBy specifying the join type as 'left', the modified code ensures that all records from purch_df\nare preserved, and matching records from cust_df are included. Records in purch_df without\na corresponding match in cust_df will have null values for the columns from cust_df.\nThis approach is consistent with standard SQL join operations and is supported in PySpark's\nDataFrame API."
    },
    {
        "question": "A data analyst wants to add a column date derived from a timestamp column.\nOptions:",
        "options": [
            "A. dates_df.withColumn(\"date\", f.unix_timestamp(\"timestamp\")).show()",
            "B. dates_df.withColumn(\"date\", f.to_date(\"timestamp\")).show()",
            "C. dates_df.withColumn(\"date\", f.date_format(\"timestamp\", \"yyyy-MM-dd\")).show()",
            "D. dates_df.withColumn(\"date\", f.from_unixtime(\"timestamp\")).show()"
        ],
        "answer": [
            "B"
        ],
        "explanation": "f.to_date() converts a timestamp or string to a DateType.\nIdeal for extracting the date component (year-month-day) from a full timestamp.\nExample:\nfrom pyspark.sql.functions import to_date\ndates_df.withColumn(\"date\", to_date(\"timestamp\"))"
    },
    {
        "question": "An engineer wants to join two DataFrames df1 and df2 on the respective employee_id and\nemp_id columns:\ndf1: employee_id INT, name STRING\ndf2: emp_id INT, department STRING\nThe engineer uses:\nresult = df1.join(df2, df1.employee_id == df2.emp_id, how='inner')\nWhat is the behaviour of the code snippet?",
        "options": [
            "A. The code fails to execute because the column names employee_id and emp_id do not\nmatch automatically",
            "B. The code fails to execute because it must use on='employee_id' to specify the join column\nexplicitly",
            "C. The code fails to execute because PySpark does not support joining DataFrames with a\ndifferent structure",
            "D. The code works as expected because the join condition explicitly matches employee_id\nfrom df1 with emp_id from df2"
        ],
        "answer": [
            "D"
        ],
        "explanation": "In PySpark, when performing a join between two DataFrames, the columns do not have to\nshare the same name. You can explicitly provide a join condition by comparing specific\ncolumns from each DataFrame.\nThis syntax is correct and fully supported:\ndf1.join(df2, df1.employee_id == df2.emp_id, how='inner')\nThis will perform an inner join between df1 and df2 using the employee_id from df1 and\nemp_id from df2."
    },
    {
        "question": "An engineer has a large ORC file located at /file/test_data.orc and wants to read only specific\ncolumns to reduce memory usage.\n\n\nWhich code fragment will select the columns, i.e., col1, col2, during the reading process?",
        "options": [
            "A. spark.read.orc(\"/file/test_data.orc\").filter(\"col1 = 'value' \").select(\"col2\")",
            "B. spark.read.format(\"orc\").select(\"col1\", \"col2\").load(\"/file/test_data.orc\")",
            "C. spark.read.orc(\"/file/test_data.orc\").selected(\"col1\", \"col2\")",
            "D. spark.read.format(\"orc\").load(\"/file/test_data.orc\").select(\"col1\", \"col2\")"
        ],
        "answer": [
            "D"
        ],
        "explanation": "The correct way to load specific columns from an ORC file is to first load the file using .load()\nand then apply .select() on the resulting DataFrame. This is valid with .read.format(\"orc\") or\nthe shortcut .read.orc().\ndf = spark.read.format(\"orc\").load(\"/file/test_data.orc\").select(\"col1\", \"col2\") Why others are\nincorrect:\nA performs selection after filtering, but doesn't match the intention to minimize memory at\nload.\nB incorrectly tries to use .select() before .load(), which is invalid.\nC uses a non-existent .selected() method.\nD correctly loads and then selects."
    },
    {
        "question": "A developer is creating a Spark application that performs multiple DataFrame transformations\nand actions. The developer wants to maintain optimal performance by properly managing the\nSparkSession.\nHow should the developer handle the SparkSession throughout the application?",
        "options": [
            "A. Use a single SparkSession instance for the entire application.",
            "B. Avoid using a SparkSession and rely on SparkContext only.",
            "C. Create a new SparkSession instance before each transformation.",
            "D. Stop and restart the SparkSession after each action."
        ],
        "answer": [
            "A"
        ],
        "explanation": "The SparkSession is the entry point to Spark functionality in modern versions (2.x and later).\nIt unifies the SparkContext, SQLContext, and HiveContext into a single object.\nBest Practice:\nUse one SparkSession for the entire application.\nCreate it once at the start using SparkSession.builder.getOrCreate().\nReuse it across all transformations and actions.\nStop it only after all operations are completed.\nExample:\nfrom pyspark.sql import SparkSession\nspark = SparkSession.builder.appName(\"MyApp\").getOrCreate()\n# Perform transformations and actions\nspark.stop()\nWhy the other options are incorrect:\nB: SparkSession is the recommended interface; SparkContext alone is deprecated for\n\n\nSQL/DataFrame APIs.\nC: Creating multiple sessions increases overhead and wastes resources.\nD: Restarting SparkSession breaks lineage and adds unnecessary startup costs.\nReference:\nSpark API Reference - SparkSession lifecycle.\nDatabricks Exam Guide (June 2025): Section \"Apache Spark Architecture and Components\"\n- explains SparkSession lifecycle and application management."
    },
    {
        "question": "The data engineering team created a pipeline that extracts data from a transaction system.\nThe transaction system stores timestamps in UTC, and the data engineers must now\ntransform the transaction_datetime field to the \"America/New_York\" timezone for reporting.\nWhich code should be used to convert the timestamp to the target timezone?",
        "options": [
            "A. raw.withColumn(\"transaction_datetime\", from_utc_timestamp(col(\"transaction_datetime\"),\n\"America/New_York\"))",
            "B. raw.withColumn(\"transaction_datetime\", to_utc_timestamp(col(\"transaction_datetime\"),\n\"America/New_York\"))",
            "C. raw.withColumn(\"transaction_datetime\", date_format(col(\"transaction_datetime\"),\n\"America/New_York\"))",
            "D. raw.withColumn(\"transaction_datetime\", convert_timezone(col(\"transaction_datetime\"),\n\"America/New_York\"))"
        ],
        "answer": [
            "A"
        ],
        "explanation": "In Spark SQL, to convert a UTC timestamp to another timezone, you use the function\nfrom_utc_timestamp().\nCorrect syntax:\nfrom pyspark.sql.functions import from_utc_timestamp, col\ndf_converted = raw.withColumn(\n\"transaction_datetime\",\nfrom_utc_timestamp(col(\"transaction_datetime\"), \"America/New_York\")\n)\nThis adjusts the UTC time into the specified timezone using Spark's timezone database.\nWhy the other options are incorrect:\nB: to_utc_timestamp() converts local time to UTC, not the other way around.\nC: date_format() formats timestamps as strings but doesn't adjust timezones.\nD: convert_timezone() is not a valid Spark SQL function.\nReference:\nSpark SQL Functions - from_utc_timestamp() and to_utc_timestamp().\nDatabricks Exam Guide (June 2025): Section \"Using Spark SQL\" - working with timestamps\nand timezone conversions."
    },
    {
        "question": "Given the code fragment:\nimport pyspark.pandas as ps\npsdf = ps.DataFrame({'col1': [1, 2], 'col2': [3, 4]})\nWhich method is used to convert a Pandas API on Spark DataFrame\n(pyspark.pandas.DataFrame) into a standard PySpark DataFrame (pyspark.sql.DataFrame)?",
        "options": [
            "A. psdf.to_spark()",
            "B. psdf.to_pyspark()",
            "C. psdf.to_pandas()",
            "D. psdf.to_dataframe()"
        ],
        "answer": [
            "A"
        ],
        "explanation": "Pandas API on Spark (pyspark.pandas) allows interoperability with PySpark DataFrames. To\nconvert a pyspark.pandas.DataFrame to a standard PySpark DataFrame, you use\n.to_spark().\nExample:\ndf = psdf.to_spark()\nThis is the officially supported method as per Databricks Documentation.\nIncorrect options:\nB, D: Invalid or nonexistent methods.\nC: Converts to a local pandas DataFrame, not a PySpark DataFrame."
    },
    {
        "question": "An engineer notices a significant increase in the job execution time during the execution of a\nSpark job. After some investigation, the engineer decides to check the logs produced by the\nExecutors.\nHow should the engineer retrieve the Executor logs to diagnose performance issues in the\nSpark application?",
        "options": [
            "A. Locate the executor logs on the Spark master node, typically under the /tmp directory.",
            "B. Use the command spark-submit with the -verbose flag to print the logs to the console.",
            "C. Use the Spark UI to select the stage and view the executor logs directly from the stages\ntab.",
            "D. Fetch the logs by running a Spark job with the spark-sql CLI tool."
        ],
        "answer": [
            "C"
        ],
        "explanation": "The Spark UI is the standard and most effective way to inspect executor logs, task time, input\nsize, and shuffles.\nFrom the Databricks documentation:\n\"You can monitor job execution via the Spark Web UI. It includes detailed logs and metrics,\nincluding task-level execution time, shuffle reads/writes, and executor memory usage.\"\n\n\n(Source: Databricks Spark Monitoring Guide) Option A is incorrect: logs are not guaranteed\nto be in /tmp, especially in cloud environments.\nB . -verbose helps during job submission but doesn't give detailed executor logs.\nD . spark-sql is a CLI tool for running queries, not for inspecting logs.\nHence, the correct method is using the Spark UI → Stages tab → Executor logs."
    },
    {
        "question": "An application architect has been investigating Spark Connect as a way to modernize\nexisting Spark applications running in their organization.\nWhich requirement blocks the adoption of Spark Connect in this organization?",
        "options": [
            "A. Debuggability: the ability to perform interactive debugging directly from the application\ncode",
            "B. Upgradability: the ability to upgrade the Spark applications independently from the Spark\ndriver itself",
            "C. Complete Spark API support: the ability to migrate all existing code to Spark Connect\nwithout modification, including the RDD APIs",
            "D. Stability: isolation of application code and dependencies from each other and the Spark\ndriver"
        ],
        "answer": [
            "C"
        ],
        "explanation": "Spark Connect enables a decoupled client-server architecture, allowing remote clients to run\nSpark code via gRPC.\nHowever, as of Spark 3.5, Spark Connect supports DataFrame and SQL APIs, but not RDD\nAPIs.\nLimitation:\nApplications that rely heavily on RDD-based transformations or actions cannot be migrated\ndirectly to Spark Connect.\nThese APIs require tight driver integration, which Spark Connect intentionally decouples.\nThus, complete Spark API compatibility is not yet achieved - this is the key adoption blocker.\nWhy the other options are incorrect:\nA: Debugging is possible through IDE integration and logs on the client side.\nB: Spark Connect actually supports upgradable clients independent of the driver - this is an\nadvantage, not a limitation.\nD: Spark Connect provides strong isolation between the client and driver processes.\nReference:\nSpark 3.5 Documentation - Spark Connect architecture and supported APIs.\nDatabricks Exam Guide (June 2025): Section \"Using Spark Connect to Deploy Applications\" -\nSpark Connect limitations (no RDD API support)."
    },
    {
        "question": "A Spark engineer is troubleshooting a Spark application that has been encountering out-of-\nmemory errors during execution. By reviewing the Spark driver logs, the engineer notices\nmultiple \"GC overhead limit exceeded\" messages.\nWhich action should the engineer take to resolve this issue?",
        "options": [
            "A. Optimize the data processing logic by repartitioning the DataFrame.",
            "B. Modify the Spark configuration to disable garbage collection",
            "C. Increase the memory allocated to the Spark Driver.",
            "D. Cache large DataFrames to persist them in memory."
        ],
        "answer": [
            "C"
        ],
        "explanation": "The message \"GC overhead limit exceeded\" typically indicates that the JVM is spending too\nmuch time in garbage collection with little memory recovery. This suggests that the driver or\nexecutor is under-provisioned in memory.\nThe most effective remedy is to increase the driver memory using:\n--driver-memory 4g\nThis is confirmed in Spark's official troubleshooting documentation:\n\"If you see a lot of GC overhead limit exceeded errors in the driver logs, it's a sign that the\ndriver is running out of memory.\"\n- Spark Tuning Guide\n\"If you see a lot of GC overhead limit exceeded errors in the driver logs, it's a sign that the\ndriver is running out of memory.\"\n- Spark Tuning Guide\nWhy others are incorrect:\nA may help but does not directly address the driver memory shortage.\nB is not a valid action; GC cannot be disabled.\nD increases memory usage, worsening the problem."
    },
    {
        "question": "In the code block below, aggDF contains aggregations on a streaming DataFrame:\naggDF.writeStream \\\n.format(\"console\") \\\n.outputMode(\"???\") \\\n.start()\nWhich output mode at line 3 ensures that the entire result table is written to the console\nduring each trigger execution?",
        "options": [
            "A. AGGREGATE",
            "B. COMPLETE",
            "C. REPLACE",
            "D. APPEND"
        ],
        "answer": [
            "B"
        ],
        "explanation": "Structured Streaming supports three output modes:\nAppend: Writes only new rows since the last trigger.\nUpdate: Writes only updated rows.\nComplete: Writes the entire result table after every trigger execution.\nFor aggregations like groupBy().count(), only complete mode outputs the entire table each\ntime.\nExample:\naggDF.writeStream \\\n\n\n.outputMode(\"complete\") \\\n.format(\"console\") \\\n.start()\nWhy the other options are incorrect:\nA: \"AGGREGATE\" is not a valid output mode.\nC: \"REPLACE\" does not exist.\nD: \"APPEND\" writes only new rows, not the full table.\nReference:\nPySpark Structured Streaming - Output Modes (append, update, complete).\nDatabricks Exam Guide (June 2025): Section \"Structured Streaming\" - output modes and use\ncases for aggregations."
    },
    {
        "question": "Given a DataFrame df that has 10 partitions, after running the code:\ndf.repartition(20)\nHow many partitions will the result DataFrame have?",
        "options": [
            "A. 5",
            "B. 20",
            "C. Same number as the cluster executors",
            "D. 10"
        ],
        "answer": [
            "B"
        ],
        "explanation": "The repartition(n) transformation reshuffles data into exactly n partitions.\nUnlike coalesce(), repartition() always causes a shuffle to evenly redistribute the data.\nCorrect behavior:\ndf2 = df.repartition(20)\ndf2.rdd.getNumPartitions() # returns 20\nThus, the resulting DataFrame will have 20 partitions.\nWhy the other options are incorrect:\nA/D: Doesn't retain old partition count - it's explicitly set to 20.\nC: Number of partitions is not automatically tied to executors.\nReference:\nPySpark DataFrame API - repartition() vs. coalesce().\nDatabricks Exam Guide (June 2025): Section \"Developing Apache Spark\nDataFrame/DataSet API Applications\" - tuning partitioning and shuffling for performance."
    },
    {
        "question": "A data engineer is working on a Streaming DataFrame streaming_df with the given streaming\ndata:\n\n\nWhich operation is supported with streamingdf ?",
        "options": [
            "A. streaming_df. select (countDistinct (\"Name\") )",
            "B. streaming_df.groupby(\"Id\") .count ()",
            "C. streaming_df.orderBy(\"timestamp\").limit(4)",
            "D. streaming_df.filter (col(\"count\") < 30).show()"
        ],
        "answer": [
            "B"
        ],
        "explanation": "In Structured Streaming, only a limited subset of operations is supported.\nB: groupby(\"Id\").count() is supported — streaming aggregations over a key are allowed, Spark maintains intermediate state for each key.\nA: countDistinct() requires the full dataset and is not supported in streaming without watermark/windowing.\nC: orderBy().limit() requires a full view of the stream and is unsupported.\nD: show() is a blocking batch operation, not allowed on streaming DataFrames. Use writeStream instead."
    },
    {
        "question": "Given the code:\ndf = spark.read.csv(\"large_dataset.csv\")\nfiltered_df = df.filter(col(\"error_column\").contains(\"error\"))\nmapped_df = filtered_df.select(split(col(\"timestamp\"), \" \").getItem(0).alias(\"date\"),\nlit(1).alias(\"count\")) reduced_df = mapped_df.groupBy(\"date\").sum(\"count\")\nreduced_df.count() reduced_df.show() At which point will Spark actually begin processing the\ndata?",
        "options": [
            "A. When the filter transformation is applied",
            "B. When the count action is applied",
            "C. When the groupBy transformation is applied",
            "D. When the show action is applied"
        ],
        "answer": [
            "B"
        ],
        "explanation": "Spark uses lazy evaluation. Transformations like filter, select, and groupBy only define the\nDAG (Directed Acyclic Graph). No execution occurs until an action is triggered.\nThe first action in the code is:reduced_df.count()\nSo Spark starts processing data at this line."
    },
    {
        "question": "A Spark DataFrame df is cached using the MEMORY_AND_DISK storage level, but the\nDataFrame is too large to fit entirely in memory.\nWhat is the likely behavior when Spark runs out of memory to store the DataFrame?",
        "options": [
            "A. Spark duplicates the DataFrame in both memory and disk. If it doesn't fit in memory, the\nDataFrame is stored and retrieved from the disk entirely.",
            "B. Spark splits the DataFrame evenly between memory and disk, ensuring balanced storage\nutilization.",
            "C. Spark will store as much data as possible in memory and spill the rest to disk when\nmemory is full, continuing processing with performance overhead.",
            "D. Spark stores the frequently accessed rows in memory and less frequently accessed rows\n\n\non disk, utilizing both resources to offer balanced performance."
        ],
        "answer": [
            "C"
        ],
        "explanation": "When using the MEMORY_AND_DISK storage level, Spark attempts to cache as much of the\nDataFrame in memory as possible. If the DataFrame does not fit entirely in memory, Spark\nwill store the remaining partitions on disk. This allows processing to continue, albeit with a\nperformance overhead due to disk I/O.\nAs per the Spark documentation:\n\"MEMORY_AND_DISK: It stores partitions that do not fit in memory on disk and keeps the\nrest in memory. This can be useful when working with datasets that are larger than the\navailable memory.\"\n- Perficient Blogs: Spark - StorageLevel\nThis behavior ensures that Spark can handle datasets larger than the available memory by\nspilling excess data to disk, thus preventing job failures due to memory constraints."
    },
    {
        "question": "An MLOps engineer is building a Pandas UDF that applies a language model that translates\nEnglish strings into Spanish. The initial code is loading the model on every call to the UDF,\nwhich is hurting the performance of the data pipeline.\nThe initial code is:\ndef in_spanish_inner(df: pd.Series) -> pd.Series:\nmodel = get_translation_model(target_lang='es')\nreturn df.apply(model)\nin_spanish = sf.pandas_udf(in_spanish_inner, StringType())\nHow can the MLOps engineer change this code to reduce how many times the language\nmodel is loaded?",
        "options": [
            "A. Convert the Pandas UDF to a PySpark UDF",
            "B. Convert the Pandas UDF from a Series → Series UDF to a Series → Scalar UDF",
            "C. Run the in_spanish_inner() function in a mapInPandas() function call",
            "D. Convert the Pandas UDF from a Series → Series UDF to an Iterator[Series] →\nIterator[Series] UDF"
        ],
        "answer": [
            "D"
        ],
        "explanation": "The provided code defines a Pandas UDF of type Series-to-Series, where a new instance of\nthe language model is created on each call, which happens per batch. This is inefficient and\nresults in significant overhead due to repeated model initialization.\nTo reduce the frequency of model loading, the engineer should convert the UDF to an\niterator-based Pandas UDF (Iterator[pd.Series] -> Iterator[pd.Series]). This allows the model\n\n\nto be loaded once per executor and reused across multiple batches, rather than once per\ncall.\nFrom the official Databricks documentation:\n\"Iterator of Series to Iterator of Series UDFs are useful when the UDF initialization is\nexpensive... For example, loading a ML model once per executor rather than once per\nrow/batch.\"\n- Databricks Official Docs: Pandas UDFs\nCorrect implementation looks like:\n@pandas_udf(\"string\")\ndef translate_udf(batch_iter: Iterator[pd.Series]) -> Iterator[pd.Series]:\nmodel = get_translation_model(target_lang='es')\nfor batch in batch_iter:\nyield batch.apply(model)\nThis refactor ensures the get_translation_model() is invoked once per executor process, not\nper batch, significantly improving pipeline performance."
    },
    {
        "question": "Given the following code snippet in my_spark_app.py:\n\nfrom pyspark.sql import SparkSession\n\nspark = SparkSession.builder.appName(\"CoreComponentsExample\").getOrCreate()\n\ndata = [(\"Alice\", 34), (\"Bob\", 36), (\"Cathy\", 31)]\ncolumns = [\"Name\", \"Age\"]\n\ndf = spark.createDataFrame(data, columns).withColumn(\"Status\", \"Pass\")\ndf_filtered = df.filter(df.Age > 35)\ndf_filtered.show()\n\nspark.stop()\n\nWhat is the role of the driver node?",
        "options": [
            "A. The driver node orchestrates the execution by transforming actions into tasks and\ndistributing them to worker nodes",
            "B. The driver node only provides the user interface for monitoring the application",
            "C. The driver node holds the DataFrame data and performs all computations locally",
            "D. The driver node stores the final result after computations are completed by worker nodes"
        ],
        "answer": [
            "A"
        ],
        "explanation": "In the Spark architecture, the driver node is responsible for orchestrating the execution of a\nSpark application. It converts user-defined transformations and actions into a logical plan,\noptimizes it into a physical plan, and then splits the plan into tasks that are distributed to the\nexecutor nodes.\n\n\nAs per Databricks and Spark documentation:\n\"The driver node is responsible for maintaining information about the Spark application,\nresponding to a user's program or input, and analyzing, distributing, and scheduling work\nacross the executors.\" This means:\nOption A is correct because the driver schedules and coordinates the job execution.\nOption B is incorrect because the driver does more than just UI monitoring.\nOption C is incorrect since data and computations are distributed across executor nodes.\nOption D is incorrect; results are returned to the driver but not stored long-term by it."
    },
    {
        "question": "What is the relationship between jobs, stages, and tasks during execution in Apache Spark?",
        "options": [
            "A. A job contains multiple tasks, and each task contains multiple stages.",
            "B. A stage contains multiple jobs, and each job contains multiple tasks.",
            "C. A stage contains multiple tasks, and each task contains multiple jobs.",
            "D. A job contains multiple stages, and each stage contains multiple tasks."
        ],
        "answer": [
            "D"
        ],
        "explanation": "In Apache Spark's execution hierarchy, the relationships are structured as follows:\nJob: Created when an action (e.g., count(), collect(), save()) is triggered on an RDD or\nDataFrame.\nStage: Each job is divided into one or more stages, separated by shuffle boundaries (e.g.,\nafter a reduceByKey or join).\nTask: Each stage consists of multiple tasks, one per partition, executed in parallel on\nexecutors.\nExecution Hierarchy:\nJob → Stage(s) → Task(s)\nSo, a job contains multiple stages, and each stage contains multiple tasks.\nWhy the other options are incorrect:\nA: A job does not directly contain tasks without stages.\nB: A stage cannot contain multiple jobs; it belongs to a single job.\nC: Tasks do not contain jobs.\nReference (Databricks Apache Spark 3.5 - Python / Study Guide):\nSpark Architecture Overview - Execution Hierarchy: Jobs, Stages, and Tasks.\nDatabricks Exam Guide (June 2025): Section \"Apache Spark Architecture and Components\"\n- describes execution hierarchy and lazy evaluation."
    },
    {
        "question": "A data engineer observes that an upstream streaming source sends duplicate records, where\nduplicates share the same key and have at most a 30-minute difference in event_timestamp.\nThe engineer adds:\ndropDuplicatesWithinWatermark(\"event_timestamp\", \"30 minutes\")\nWhat is the result?",
        "options": [
            "A. It is not able to handle deduplication in this scenario",
            "B. It removes duplicates that arrive within the 30-minute window specified by the watermark",
            "C. It removes all duplicates regardless of when they arrive",
            "D. It accepts watermarks in seconds and the code results in an error"
        ],
        "answer": [
            "B"
        ],
        "explanation": "The method dropDuplicatesWithinWatermark() in Structured Streaming drops duplicate\nrecords based on a specified column and watermark window. The watermark defines the\nthreshold for how late data is considered valid.\nFrom the Spark documentation:\n\"dropDuplicatesWithinWatermark removes duplicates that occur within the event-time\nwatermark window.\" In this case, Spark will retain the first occurrence and drop subsequent\nrecords within the 30-minute watermark window.\nFinal answer: B"
    },
    {
        "question": "A data engineer needs to add all the rows from one table to all the rows from another, but not\nall the columns in the first table exist in the second table.\nThe error message is:\nAnalysisException: UNION can only be performed on tables with the same number of\ncolumns.\nThe existing code is:\nau_df.union(nz_df)\nThe DataFrame au_df has one extra column that does not exist in the DataFrame nz_df, but\notherwise both DataFrames have the same column names and data types.\nWhat should the data engineer fix in the code to ensure the combined DataFrame can be\nproduced as expected?",
        "options": [
            "A. df = au_df.unionByName(nz_df, allowMissingColumns=True)",
            "B. df = au_df.unionAll(nz_df)",
            "C. df = au_df.unionByName(nz_df, allowMissingColumns=False)",
            "D. df = au_df.union(nz_df, allowMissingColumns=True)"
        ],
        "answer": [
            "A"
        ],
        "explanation": "When two DataFrames have different column sets, the normal union() or unionAll() functions\nfail unless both have exactly the same columns in the same order.\nSolution: Use unionByName() with allowMissingColumns=True.\nThis aligns columns by name and automatically adds missing columns with null values.\nCorrect syntax:\ncombined_df = au_df.unionByName(nz_df, allowMissingColumns=True)\nThis ensures the union works even if one DataFrame has extra or missing columns.\nWhy the other options are incorrect:\nB: unionAll() is deprecated; also requires identical schemas.\n\n\nC: With allowMissingColumns=False, Spark still throws a mismatch error.\nD: union() doesn't accept the allowMissingColumns argument.\nReference:\nPySpark API - DataFrame.unionByName() with allowMissingColumns option.\nDatabricks Exam Guide (June 2025): Section \"Developing Apache Spark\nDataFrame/DataSet API Applications\" - combining DataFrames and schema alignment."
    },
    {
        "question": "A data engineer needs to write a Streaming DataFrame as Parquet files.\nGiven the code:\nWhich code fragment should be inserted to meet the requirement?\nA)\nB)\nC)\n \nD)\n \nWhich code fragment should be inserted to meet the requirement?",
        "options": [
            "A. .format(\"parquet\")\n.option(\"location\", \"path/to/destination/dir\")",
            "B. .option(\"format\", \"parquet\")\n.option(\"destination\", \"path/to/destination/dir\")",
            "C. .option(\"format\", \"parquet\")\n.option(\"location\", \"path/to/destination/dir\")",
            "D. .format(\"parquet\")\n.option(\"path\", \"path/to/destination/dir\")"
        ],
        "answer": [
            "D"
        ],
        "explanation": "To write a structured streaming DataFrame to Parquet files, the correct way to specify the\nformat and output directory is:\n.writeStream\n.format(\"parquet\")\n.option(\"path\", \"path/to/destination/dir\")\nAccording to Spark documentation:\n\"When writing to file-based sinks (like Parquet), you must specify the path using the\n.option(\"path\", ...) method. Unlike batch writes, .save() is not supported.\" Option A incorrectly\nuses .option(\"location\", ...) (invalid for Parquet sink).\nOption B incorrectly sets the format via .option(\"format\", ...), which is not the correct method.\nOption C repeats the same issue.\nOption D is correct: .format(\"parquet\") + .option(\"path\", ...) is the required syntax.\nFinal answer: D"
    },
    {
        "question": "A data engineer is building a Structured Streaming pipeline and wants the pipeline to recover\nfrom failures or intentional shutdowns by continuing where the pipeline left off.\nHow can this be achieved?",
        "options": [
            "A. By configuring the option checkpointLocation during readStream",
            "B. By configuring the option recoveryLocation during the SparkSession initialization",
            "C. By configuring the option recoveryLocation during writeStream",
            "D. By configuring the option checkpointLocation during writeStream"
        ],
        "answer": [
            "D"
        ],
        "explanation": "To enable a Structured Streaming query to recover from failures or intentional shutdowns, it\nis essential to specify the checkpointLocation option during the writeStream operation. This\ncheckpoint location stores the progress information of the streaming query, allowing it to\nresume from where it left off.\nAccording to the Databricks documentation:\n\"You must specify the checkpointLocation option before you run a streaming query, as in the\nfollowing example:\n.option(\"checkpointLocation\", \"/path/to/checkpoint/dir\")\n.toTable(\"catalog.schema.table\")\n- Databricks Documentation: Structured Streaming checkpoints\nBy setting the checkpointLocation during writeStream, Spark can maintain state information\nand ensure exactly-once processing semantics, which are crucial for reliable streaming\napplications."
    },
    {
        "question": "A Spark engineer must select an appropriate deployment mode for the Spark jobs.\nWhat is the benefit of using cluster mode in Apache Spark™?",
        "options": [
            "A. In cluster mode, resources are allocated from a resource manager on the cluster, enabling\nbetter performance and scalability for large jobs",
            "B. In cluster mode, the driver is responsible for executing all tasks locally without distributing\nthem across the worker nodes.",
            "C. In cluster mode, the driver runs on the client machine, which can limit the application's\nability to handle large datasets efficiently.",
            "D. In cluster mode, the driver program runs on one of the worker nodes, allowing the\napplication to fully utilize the distributed resources of the cluster."
        ],
        "answer": [
            "D"
        ],
        "explanation": "In Apache Spark's cluster mode:\n\"The driver program runs on the cluster's worker node instead of the client's local machine.\nThis allows the driver to be close to the data and other executors, reducing network overhead\nand improving fault tolerance for production jobs.\" (Source: Apache Spark documentation -\nCluster Mode Overview)\n\"The driver program runs on the cluster's worker node instead of the client's local machine.\nThis allows the driver to be close to the data and other executors, reducing network overhead\nand improving fault tolerance for production jobs.\" (Source: Apache Spark documentation -\nCluster Mode Overview) This deployment is ideal for production environments where the job\nis submitted from a gateway node, and Spark manages the driver lifecycle on the cluster\nitself.\nOption A is partially true but less specific than D.\nOption B is incorrect: the driver never executes all tasks; executors handle distributed tasks.\nOption C describes client mode, not cluster mode."
    },
    {
        "question": "Given the schema:\n \nevent_ts TIMESTAMP,\nsensor_id STRING,\nmetric_value LONG,\ningest_ts TIMESTAMP,\n\n\nsource_file_path STRING\nThe goal is to deduplicate based on: event_ts, sensor_id, and metric_value.\nOptions:",
        "options": [
            "A. dropDuplicates on all columns (wrong criteria)",
            "B. dropDuplicates with no arguments (removes based on all columns)",
            "C. groupBy without aggregation (invalid use)",
            "D. dropDuplicates on the exact matching fields"
        ],
        "answer": [
            "D"
        ],
        "explanation": "dedup_df = iot_bronze_df.dropDuplicates([\"event_ts\", \"sensor_id\", \"metric_value\"])\ndropDuplicates accepts a list of columns to use for deduplication.\nThis ensures only unique records based on the specified keys are retained."
    },
    {
        "question": "A data scientist has identified that some records in the user profile table contain null values in\nany of the fields, and such records should be removed from the dataset before processing.\nThe schema includes fields like user_id, username, date_of_birth, created_ts, etc.\nThe schema of the user profile table looks like this:\n \nWhich block of Spark code can be used to achieve this requirement?\nOptions:",
        "options": [
            "A. filtered_df = users_raw_df.na.drop(thresh=0)",
            "B. filtered_df = users_raw_df.na.drop(how='all')",
            "C. filtered_df = users_raw_df.na.drop(how='any')",
            "D. filtered_df = users_raw_df.na.drop(how='all', thresh=None)"
        ],
        "answer": [
            "C"
        ],
        "explanation": ".na.drop(how='any') drops any row that has at least one null value.\nThis is exactly what's needed when the goal is to retain only fully complete records.\nUsage:filtered_df = users_raw_df.na.drop(how='any')\n\n\nExplanation of incorrect options:\nA: thresh=0 is invalid - thresh must be ≥ 1.\nB: how='all' drops only rows where all columns are null (too lenient).\nD: spark.na.drop doesn't support mixing how and thresh in that way; it's incorrect syntax."
    },
    {
        "question": "A data engineer needs to write a DataFrame df to a Parquet file, partitioned by the column\ncountry, and overwrite any existing data at the destination path.\nWhich code should the data engineer use to accomplish this task in Apache Spark?",
        "options": [
            "A. df.write.mode(\"overwrite\").partitionBy(\"country\").parquet(\"/data/output\")",
            "B. df.write.mode(\"append\").partitionBy(\"country\").parquet(\"/data/output\")",
            "C. df.write.mode(\"overwrite\").parquet(\"/data/output\")",
            "D. df.write.partitionBy(\"country\").parquet(\"/data/output\")"
        ],
        "answer": [
            "A"
        ],
        "explanation": "The .mode(\"overwrite\") ensures that existing files at the path will be replaced.\n.partitionBy(\"country\") optimizes queries by writing data into partitioned folders.\nCorrect syntax:\ndf.write.mode(\"overwrite\").partitionBy(\"country\").parquet(\"/data/output\")\n- Source: Spark SQL, DataFrames and Datasets Guide"
    },
    {
        "question": "A data engineer is working on the DataFrame df1 and wants the Name with the highest count\nto appear first (descending order by count), followed by the next highest, and so on.\nThe DataFrame has columns:\nid | Name | count | timestamp\n---------------------------------\n1 | USA | 10\n2 | India | 20\n3 | England | 50\n4 | India | 50\n5 | France | 20\n6 | India | 10\n7 | USA | 30\n8 | USA | 40\nWhich code fragment should the engineer use to sort the data in the Name and count\ncolumns?",
        "options": [
            "A. df1.orderBy(col(\"count\").desc(), col(\"Name\").asc())",
            "B. df1.sort(\"Name\", \"count\")",
            "C. df1.orderBy(\"Name\", \"count\")",
            "D. df1.orderBy(col(\"Name\").desc(), col(\"count\").asc())"
        ],
        "answer": [
            "A"
        ],
        "explanation": "To sort a Spark DataFrame by multiple columns, use .orderBy() (or .sort()) with column\n\n\nexpressions.\nCorrect syntax for descending and ascending mix:\nfrom pyspark.sql.functions import col\ndf1.orderBy(col(\"count\").desc(), col(\"Name\").asc())\nThis sorts primarily by count in descending order and secondarily by Name in ascending\norder (alphabetically).\nWhy the other options are incorrect:\nB/C: Default sort order is ascending; won't place highest counts first.\nD: Reverses sorting logic - sorts Name descending, not required.\nReference:\nPySpark DataFrame API - orderBy() and col() for sorting with direction.\nDatabricks Exam Guide (June 2025): Section \"Using Spark DataFrame APIs\" - sorting,\nordering, and column expressions."
    },
    {
        "question": "A data scientist is working with a Spark DataFrame called customerDF that contains\ncustomer information.\nThe DataFrame has a column named email with customer email addresses.\nThe data scientist needs to split this column into username and domain parts.\nWhich code snippet splits the email column into username and domain columns?",
        "options": [
            "A. customerDF = customerDF \\\n.withColumn(\"username\", split(col(\"email\"), \"@\").getItem(0)) \\\n.withColumn(\"domain\", split(col(\"email\"), \"@\").getItem(1))",
            "B. customerDF = customerDF.withColumn(\"username\", regexp_replace(col(\"email\"), \"@\", \"\"))",
            "C. customerDF = customerDF.select(\"email\").alias(\"username\", \"domain\")",
            "D. customerDF = customerDF.withColumn(\"domain\", col(\"email\").split(\"@\")[1])"
        ],
        "answer": [
            "A"
        ],
        "explanation": "The split() function in PySpark splits strings into an array based on a given delimiter.\nThen, .getItem(index) extracts a specific element from the array.\nCorrect usage:\nfrom pyspark.sql.functions import split, col\ncustomerDF = customerDF \\\n.withColumn(\"username\", split(col(\"email\"), \"@\").getItem(0)) \\\n.withColumn(\"domain\", split(col(\"email\"), \"@\").getItem(1))\nThis creates two new columns derived from the email field:\n\"username\" → text before @\n\"domain\" → text after @\nWhy the other options are incorrect:\nB: regexp_replace only replaces text; does not split into multiple columns.\n\n\nC: .select() cannot alias multiple derived columns like this.\nD: Column objects are not native Python strings; cannot use standard .split().\nReference:\nPySpark SQL Functions - split() and getItem().\nDatabricks Exam Guide (June 2025): Section \"Developing Apache Spark\nDataFrame/DataSet API Applications\" - manipulating and splitting column data."
    },
    {
        "question": "Given the code fragment:\nimport pyspark.pandas as ps\npdf = ps.DataFrame(data)\nWhich method is used to convert a Pandas API on Spark DataFrame\n(pyspark.pandas.DataFrame) into a standard PySpark DataFrame (pyspark.sql.DataFrame)?",
        "options": [
            "A. pdf.to_pandas()",
            "B. pdf.to_spark()",
            "C. pdf.to_dataframe()",
            "D. pdf.spark()"
        ],
        "answer": [
            "B"
        ],
        "explanation": "In Pandas API on Spark (previously Koalas), the method .to_spark() converts a\npyspark.pandas.DataFrame into a PySpark DataFrame.\nCorrect usage:\nspark_df = pdf.to_spark()\nThis enables interoperability between the Pandas API on Spark and the PySpark SQL API,\nallowing developers to switch seamlessly between both for transformations or performance\noptimization.\nWhy the other options are incorrect:\nA (to_pandas): Converts to a local Pandas DataFrame, not a PySpark DataFrame.\nC (to_dataframe): Not a valid API method.\nD (spark): Not an existing DataFrame method.\nReference:\nPySpark Pandas API Reference - DataFrame.to_spark() method.\nDatabricks Exam Guide (June 2025): Section \"Using Pandas API on Apache Spark\" - covers\nDataFrame conversions and interoperability."
    },
    {
        "question": "What is the difference between df.cache() and df.persist() in Spark DataFrame?",
        "options": [
            "A. Both cache() and persist() can be used to set the default storage level\n(MEMORY_AND_DISK_SER)",
            "B. Both functions perform the same operation. The persist() function provides improved\nperformance as its default storage level is DISK_ONLY.",
            "C. persist() - Persists the DataFrame with the default storage level\n(MEMORY_AND_DISK_SER) and cache() - Can be used to set different storage levels to\npersist the contents of the DataFrame.",
            "D. cache() - Persists the DataFrame with the default storage level (MEMORY_AND_DISK)\nand persist() - Can be used to set different storage levels to persist the contents of the\nDataFrame"
        ],
        "answer": [
            "D"
        ],
        "explanation": "df.cache() is shorthand for df.persist(StorageLevel.MEMORY_AND_DISK)\ndf.persist() allows specifying any storage level such as MEMORY_ONLY, DISK_ONLY,\nMEMORY_AND_DISK_SER, etc.\nBy default, persist() uses MEMORY_AND_DISK, unless specified otherwise."
    },
    {
        "question": "A developer is running Spark SQL queries and notices underutilization of resources.\nExecutors are idle, and the number of tasks per stage is low.\nWhat should the developer do to improve cluster utilization?",
        "options": [
            "A. Increase the value of spark.sql.shuffle.partitions",
            "B. Reduce the value of spark.sql.shuffle.partitions",
            "C. Increase the size of the dataset to create more partitions",
            "D. Enable dynamic resource allocation to scale resources as needed"
        ],
        "answer": [
            "A"
        ],
        "explanation": "The number of tasks is controlled by the number of partitions. By default,\nspark.sql.shuffle.partitions is 200. If stages are showing very few tasks (less than total cores),\nyou may not be leveraging full parallelism.\nFrom the Spark tuning guide:\n\"To improve performance, especially for large clusters, increase spark.sql.shuffle.partitions to\ncreate more tasks and parallelism.\" Thus:\nA is correct: increasing shuffle partitions increases parallelism\nB is wrong: it further reduces parallelism\nC is invalid: increasing dataset size doesn't guarantee more partitions D is irrelevant to task\ncount per stage Final answer: A"
    },
    {
        "question": "A data analyst builds a Spark application to analyze finance data and performs the following\noperations: filter, select, groupBy, and coalesce.\nWhich operation results in a shuffle?",
        "options": [
            "A. groupBy",
            "B. filter",
            "C. select",
            "D. coalesce"
        ],
        "answer": [
            "A"
        ],
        "explanation": "The groupBy() operation causes a shuffle because it requires all values for a specific key to\nbe brought together, which may involve moving data across partitions.\nIn contrast:\n\n\nfilter() and select() are narrow transformations and do not cause shuffles.\ncoalesce() tries to reduce the number of partitions and avoids shuffling by moving data to\nfewer partitions without a full shuffle (unlike repartition())."
    },
    {
        "question": "A data scientist of an e-commerce company is working with user data obtained from its\nsubscriber database and has stored the data in a DataFrame df_user. Before further\nprocessing the data, the data scientist wants to create another DataFrame df_user_non_pii\nand store only the non-PII columns in this DataFrame. The PII columns in df_user are\nfirst_name, last_name, email, and birthdate.\nWhich code snippet can be used to meet this requirement?",
        "options": [
            "A. df_user_non_pii = df_user.drop(\"first_name\", \"last_name\", \"email\", \"birthdate\")",
            "B. df_user_non_pii = df_user.drop([\"first_name\", \"last_name\", \"email\", \"birthdate\"])",
            "C. df_user_non_pii = df_user.dropfields(\"first_name\", \"last_name\", \"email\", \"birthdate\")",
            "D. df_user_non_pii = df_user.dropfields(\"first_name, last_name, email, birthdate\")"
        ],
        "answer": [
            "A"
        ],
        "explanation": "To remove specific columns from a PySpark DataFrame, the drop() method is used.\nCorrect syntax: df_user.drop(\"col1\", \"col2\") — pass each column name as a separate string argument.\nA: Correct. Uses drop() with multiple column names as separate arguments.\nB: Incorrect. drop() does not accept a list — use separate string arguments, not a Python list.\nC: Incorrect. dropfields() is not a DataFrame method — it's used for nested StructType fields only.\nD: Incorrect. dropfields() with a single comma-separated string is not valid PySpark syntax."
    },
    {
        "question": "A data scientist is working with a massive dataset that exceeds the memory capacity of a\nsingle machine. The data scientist is considering using Apache Spark™ instead of traditional\nsingle-machine languages like standard Python scripts.\nWhich two advantages does Apache Spark™ offer over a normal single-machine language in\n\n\nthis scenario? (Choose 2 answers)",
        "options": [
            "A. It can distribute data processing tasks across a cluster of machines, enabling horizontal\nscalability.",
            "B. It requires specialized hardware to run, making it unsuitable for commodity hardware\nclusters.",
            "C. It processes data solely on disk storage, reducing the need for memory resources.",
            "D. It eliminates the need to write any code, automatically handling all data processing.",
            "E. It has built-in fault tolerance, allowing it to recover seamlessly from node failures during\ncomputation."
        ],
        "answer": [
            "A",
            "E"
        ],
        "explanation": "Apache Spark is a distributed data processing engine designed for large-scale, cluster-based\ncomputation.\nAdvantages:\nHorizontal Scalability: Spark can distribute tasks across many machines, handling datasets\nlarger than the memory of a single node.\nFault Tolerance: Spark automatically recovers from node or task failures using the lineage\ngraph (RDD recovery mechanism) and retry logic.\nThese two features allow Spark to process huge datasets efficiently and reliably, unlike\nstandard Python scripts that are limited to one machine and fail on single-node errors.\nWhy the other options are incorrect:\nB: Spark runs on commodity hardware; no specialized machines required.\nC: Spark emphasizes in-memory processing, not disk-only operations.\nD: Spark still requires user code in Python, Scala, SQL, or Java.\nReference:\nDatabricks Exam Guide (June 2025): Section \"Apache Spark Architecture and Components\"\n- advantages, cluster execution, and fault tolerance.\nApache Spark Overview - distributed processing and resilience design."
    },
    {
        "question": "Which code should be used to display the schema of the Parquet file stored in the location\nevents.parquet?",
        "options": [
            "A. spark.sql(\"SELECT * FROM events.parquet\").show()",
            "B. spark.read.format(\"parquet\").load(\"events.parquet\").show()",
            "C. spark.read.parquet(\"events.parquet\").printSchema()",
            "D. spark.sql(\"SELECT schema FROM events.parquet\").show()"
        ],
        "answer": [
            "C"
        ],
        "explanation": "To view the schema of a Parquet file, you must use the DataFrameReader to load the\n\n\nParquet data and call the .printSchema() method.\nCorrect syntax:\nspark.read.parquet(\"events.parquet\").printSchema()\nThis command loads the file metadata (without triggering a full read) and prints the column\nnames, data types, and nullability information in a tree format.\nWhy the other options are incorrect:\nA/D: SQL queries can't directly introspect file schemas.\nB: .show() displays data rows, not schema.\nReference:\nPySpark DataFrameReader API - read.parquet() and DataFrame.printSchema().\nDatabricks Exam Guide (June 2025): Section \"Using Spark SQL\" - describes reading files and\nexamining schemas in Spark SQL and DataFrame APIs."
    },
    {
        "question": "A data engineer is asked to build an ingestion pipeline for a set of Parquet files delivered by an upstream team on a nightly basis. The data is stored in a directory structure with a base path of \"/path/events/data\". The upstream team drops daily data into the underlying subdirectories following the convention year/month/day.\nA few examples of the directory structure are:\n\n/path/events/data/2024/01/01\n/path/events/data/2024/01/02\n/path/events/data/2024/01/03\n/path/events/data/2023/01/01\n/path/events/data/2023/01/02\n/path/events/data/2023/01/03\n\nWhich of the following code snippets will read all the data within the directory structure?",
        "options": [
            "A. df = spark.read.option(\"inferSchema\", \"true\").parquet(\"/path/events/data/\")",
            "B. df = spark.read.option(\"recursiveFileLookup\", \"true\").parquet(\"/path/events/data/\")",
            "C. df = spark.read.parquet(\"/path/events/data/*\")",
            "D. df = spark.read.parquet(\"/path/events/data/\")"
        ],
        "answer": [
            "B"
        ],
        "explanation": "To read all files recursively within a nested directory structure, Spark requires the\nrecursiveFileLookup option to be explicitly enabled. According to Databricks official\ndocumentation, when dealing with deeply nested Parquet files in a directory tree (as shown in\nthis example), you should set:\ndf = spark.read.option(\"recursiveFileLookup\", \"true\").parquet(\"/path/events/data/\") This\nensures that Spark searches through all subdirectories under /path/events/data/ and reads\nany Parquet files it finds, regardless of the folder depth.\nOption A is incorrect because while it includes an option, inferSchema is irrelevant here and\ndoes not enable recursive file reading.\n\n\nOption C is incorrect because wildcards may not reliably match deep nested structures\nbeyond one directory level.\nOption D is incorrect because it will only read files directly within /path/events/data/ and not\nsubdirectories like /2023/01/01.\nDatabricks documentation reference:\n\"To read files recursively from nested folders, set the recursiveFileLookup option to true. This\nis useful when data is organized in hierarchical folder structures\" - Databricks documentation\non Parquet files ingestion and options."
    },
    {
        "question": "Given this code:\n.withWatermark(\"event_time\", \"10 minutes\")\n.groupBy(window(\"event_time\", \"15 minutes\"))\n.count()\nWhat happens to data that arrives after the watermark threshold?\nOptions:",
        "options": [
            "A. Records that arrive later than the watermark threshold (10 minutes) will automatically be\nincluded in the aggregation if they fall within the 15-minute window.",
            "B. Any data arriving more than 10 minutes after the watermark threshold will be ignored and\nnot included in the aggregation.",
            "C. Data arriving more than 10 minutes after the latest watermark will still be included in the\naggregation but will be placed into the next window.",
            "D. The watermark ensures that late data arriving within 10 minutes of the latest event_time\nwill be processed and included in the windowed aggregation."
        ],
        "answer": [
            "B"
        ],
        "explanation": "According to Spark's watermarking rules:\n\"Records that are older than the watermark (event time < current watermark) are considered\ntoo late and are dropped.\" So, if a record's event_time is earlier than (max event_time seen\nso far - 10 minutes), it is discarded."
    },
    {
        "question": "What is the benefit of using Pandas API on Spark for data transformations?",
        "options": [
            "A. It executes queries faster using all the available cores in the cluster as well as provides\nPandas's rich set of features.",
            "B. It is available only with Python, thereby reducing the learning curve.",
            "C. It runs on a single node only, utilizing memory efficiently.",
            "D. It computes results immediately using eager execution."
        ],
        "answer": [
            "A"
        ],
        "explanation": "Pandas API on Spark provides a distributed implementation of the Pandas DataFrame API\non top of Apache Spark.\nAdvantages:\nExecutes transformations in parallel across all nodes and cores in the cluster.\nMaintains Pandas-like syntax, making it easy for Python users to transition.\nEnables scaling of existing Pandas code to handle large datasets without memory limits.\nTherefore, it combines Pandas usability with Spark's distributed power, offering both speed\nand scalability.\nWhy the other options are incorrect:\nB: While it uses Python, that's not its main advantage.\nC: It runs distributed across the cluster, not on a single node.\nD: Pandas API on Spark uses lazy evaluation, not eager computation.\nReference:\nPySpark Pandas API Overview - advantages of distributed execution.\nDatabricks Exam Guide (June 2025): Section \"Using Pandas API on Apache Spark\" -\nexplains the benefits of Pandas API integration for scalable transformations."
    },
    {
        "question": "What is the main advantage of partitioning the data when persisting tables?",
        "options": [
            "A. It compresses the data to save disk space.",
            "B. It automatically cleans up unused partitions to optimize storage.",
            "C. It ensures that data is loaded into memory all at once for faster query execution.",
            "D. It optimizes by reading only the relevant subset of data from fewer partitions."
        ],
        "answer": [
            "D"
        ],
        "explanation": "Partitioning a dataset divides data into separate directories based on partition column values.\nWhen queries filter on partitioned columns, Spark can prune irrelevant partitions - meaning it\nonly reads files that match the filter criteria.\nAdvantage:\nReduces I/O and improves performance by scanning only relevant subsets of data.\nExample:\n/data/sales/year=2023/month=10/...\n/data/sales/year=2024/month=01/...\nA query filtering WHERE year = 2024 reads only the relevant partition.\nWhy the other options are incorrect:\nA: Compression is independent of partitioning.\nB: Spark does not automatically clean partitions unless managed manually.\nC: Partitioning does not cause Spark to load entire data into memory.\nReference:\nDatabricks Exam Guide (June 2025): Section \"Using Spark SQL\" - partitioning and pruning for\noptimized data retrieval.\n\n\nSpark SQL Documentation - DataFrameWriter partitionBy() and query optimization."
    },
    {
        "question": "A data engineer is running a Spark job to process a dataset of 1 TB stored in distributed\nstorage. The cluster has 10 nodes, each with 16 CPUs. Spark UI shows:\nLow number of Active Tasks\nMany tasks complete in milliseconds\nFewer tasks than available CPUs\nWhich approach should be used to adjust the partitioning for optimal resource allocation?",
        "options": [
            "A. Set the number of partitions equal to the total number of CPUs in the cluster",
            "B. Set the number of partitions to a fixed value, such as 200",
            "C. Set the number of partitions equal to the number of nodes in the cluster",
            "D. Set the number of partitions by dividing the dataset size (1 TB) by a reasonable partition\nsize, such as 128 MB"
        ],
        "answer": [
            "D"
        ],
        "explanation": "Spark's best practice is to estimate partition count based on data volume and a reasonable\npartition size - typically 128 MB to 256 MB per partition.\nWith 1 TB of data: 1 TB / 128 MB ≈ ~8000 partitions\nThis ensures that tasks are distributed across available CPUs for parallelism and that each\ntask processes an optimal volume of data.\nOption A (equal to cores) may result in partitions that are too large.\nOption B (fixed 200) is arbitrary and may underutilize the cluster.\nOption C (nodes) gives too few partitions (10), limiting parallelism."
    },
    {
        "question": "A developer wants to refactor some older Spark code to leverage built-in functions introduced in Spark 3.5.0. The existing code performs array manipulations manually.\n\nExisting code:\nimport pyspark.sql.functions as F\n\nmin_price = 110.50\n\nresult_df = prices_df \\\n    .filter(F.col(\"spot_price\") >= F.lit(min_price)) \\\n    .agg(F.count(\"*\"))\n\nWhich of the following code snippets utilizes new built-in functions in Spark 3.5.0 for array operations?",
        "options": [
            "A. result_df = prices_df \\\n.withColumn(\"valid_price\", F.when(F.col(\"spot_price\") > F.lit(min_price), 1).otherwise(0))",
            "B. \n\nresult_df = prices_df \\\n.agg(F.count_if(F.col(\"spot_price\") >= F.lit(min_price)))",
            "C. result_df = prices_df \\\n.agg(F.min(\"spot_price\"), F.max(\"spot_price\"))",
            "D. result_df = prices_df \\\n.agg(F.count(\"spot_price\").alias(\"spot_price\")) \\\n.filter(F.col(\"spot_price\") > F.lit(\"min_price\"))"
        ],
        "answer": [
            "B"
        ],
        "explanation": "count_if(condition) counts the number of rows that meet the specified boolean condition.\nIn this example, it directly counts how many times spot_price >= min_price evaluates to true,\nreplacing the older verbose combination of when/otherwise and filtering or summing.\nOfficial Spark 3.5.0 documentation notes the addition of count_if to simplify this kind of logic:\n\"Added count_if aggregate function to count only the rows where a boolean condition holds\n(SPARK-43773).\" Why other options are incorrect or outdated:\nA uses a legacy-style method of adding a flag column (when().otherwise()), which is verbose\ncompared to count_if.\nC performs a simple min/max aggregation-useful but unrelated to conditional array\noperations or the updated functionality.\nD incorrectly applies .filter() after .agg() which will cause an error, and misuses string\n\"min_price\" rather than the variable.\nTherefore, B is the only option leveraging new functionality from Spark 3.5.0 correctly and\nefficiently.\nExplanation:\nThe correct answer is B because it uses the new function count_if, introduced in Spark 3.5.0,\nwhich simplifies conditional counting within aggregations."
    },
    {
        "question": "A data engineer is working on the DataFrame:\n\n+---+----------+-----+----------------------------------+\n| Id|      Name|count|                         timestamp|\n+---+----------+-----+----------------------------------+\n|  4|Washington|   10|2024-09-19T10:10:40.000+00:00     |\n|  1|     Delhi|   20|2024-09-19T10:10:10.000+00:00     |\n|  2|    London|   50|2024-09-19T10:10:20.000+00:00     |\n|  1|     Delhi|   50|2024-09-19T10:10:50.000+00:00     |\n|  3|     Paris|   20|2024-09-19T10:11:20.000+00:00     |\n|  1|     Delhi|   10|2024-09-19T10:11:10.000+00:00     |\n|  3|     Paris|   30|2024-09-19T10:10:30.000+00:00     |\n|  4|Washington|   40|2024-09-19T10:11:00.000+00:00     |\n+---+----------+-----+----------------------------------+\n\nWhich code fragment should the engineer use to extract the unique values in the Name column into an alphabetically ordered list?",
        "options": [
            "A. df.select(\"Name\").orderBy(df[\"Name\"].asc())",
            "B. df.select(\"Name\").distinct().orderBy(df[\"Name\"])",
            "C. df.select(\"Name\").distinct()",
            "D. df.select(\"Name\").distinct().orderBy(df[\"Name\"].desc())"
        ],
        "answer": [
            "B"
        ],
        "explanation": "To extract unique values from a column and sort them alphabetically:\ndistinct() is required to remove duplicate values.\norderBy() is needed to sort the results alphabetically (ascending by default).\nCorrect code:\ndf.select(\"Name\").distinct().orderBy(df[\"Name\"])\nThis is directly aligned with standard DataFrame API usage in PySpark, as documented in\nthe official Databricks Spark APIs. Option A is incorrect because it may not remove\nduplicates. Option C omits sorting. Option D sorts in descending order, which doesn't meet\nthe requirement for alphabetical (ascending) order."
    },
    {
        "question": "A DataFrame df has columns name, age, and salary. The developer needs to sort the\nDataFrame by age in ascending order and salary in descending order.\nWhich code snippet meets the requirement of the developer?",
        "options": [
            "A. df.orderBy(col(\"age\").asc(), col(\"salary\").asc()).show()",
            "B. df.sort(\"age\", \"salary\", ascending=[True, True]).show()",
            "C. df.sort(\"age\", \"salary\", ascending=[False, True]).show()",
            "D. df.orderBy(\"age\", \"salary\", ascending=[True, False]).show()"
        ],
        "answer": [
            "D"
        ],
        "explanation": "To sort a PySpark DataFrame by multiple columns with mixed sort directions, the correct\nusage is:\ndf.orderBy(\"age\", \"salary\", ascending=[True, False])\nage will be sorted in ascending order\nsalary will be sorted in descending order\nThe orderBy() and sort() methods in PySpark accept a list of booleans to specify the sort\ndirection for each column.\nDocumentation Reference: PySpark API - DataFrame.orderBy"
    },
    {
        "question": "Which command overwrites an existing JSON file when writing a DataFrame?",
        "options": [
            "A. df.write.json(\"path/to/file\")",
            "B. df.write.mode(\"append\").json(\"path/to/file\")",
            "C. df.write.option(\"overwrite\").json(\"path/to/file\")",
            "D. df.write.mode(\"overwrite\").json(\"path/to/file\")"
        ],
        "answer": [
            "D"
        ],
        "explanation": "When writing DataFrames to files using the Spark DataFrameWriter API, Spark by default\nraises an error if the target path already exists. To explicitly overwrite existing data, you must\nspecify the write mode as \"overwrite\".\nCorrect Syntax:\ndf.write.mode(\"overwrite\").json(\"path/to/file\")\nThis command removes the existing file or directory at the specified path and writes the new\noutput in JSON format.\nOther supported save modes include:\n\"append\" - Adds new data to existing files.\n\"ignore\" - Skips writing if the path already exists.\n\"error\" or \"errorifexists\" - Fails the job if the output path exists (default).\nWhy other options are incorrect:\nA: Defaults to \"error\" mode, which fails if the path exists.\nB: \"append\" only adds data; it does not overwrite existing data.\nC: .option(\"overwrite\") is invalid - mode(\"overwrite\") must be used instead.\nReference (Databricks Apache Spark 3.5 - Python / Study Guide):\nPySpark API Reference: DataFrameWriter.mode() - describes valid write modes including\n\"overwrite\".\nPySpark API Reference: DataFrameWriter.json() - method to write DataFrames in JSON\nformat.\nDatabricks Certified Associate Developer for Apache Spark Exam Guide (June 2025):\nSection \"Using Spark DataFrame APIs\" - Reading and writing DataFrames using save modes,\nschema management, and partitioning."
    },
    {
        "question": "A developer has been asked to debug an issue with a Spark application. The developer\nidentified that the data being loaded from a CSV file is being read incorrectly into a\nDataFrame.\nThe CSV file has been read using the following Spark SQL statement:\nCREATE TABLE locations\nUSING csv\nOPTIONS (path '/data/locations.csv')\nThe first lines of the command SELECT * FROM locations look like this:\n| city | lat | long |\n| ALTI Sydney | -33... | ... |\nWhich parameter can the developer add to the OPTIONS clause in the CREATE TABLE\nstatement to read the CSV data correctly again?",
        "options": [
            "A. 'header' 'true'",
            "B. 'header' 'false'",
            "C. 'sep' ','",
            "D. 'sep' '|'"
        ],
        "answer": [
            "A"
        ],
        "explanation": "When reading CSV files using Spark SQL or the DataFrame API, Spark by default assumes\nthat the first line of the file is data, not headers. To interpret the first line as column names,\nthe header option must be set to true.\nCorrect syntax:\nCREATE TABLE locations\nUSING csv\nOPTIONS (\npath '/data/locations.csv',\nheader 'true'\n);\nThis tells Spark to read the first row as column headers and correctly map columns like city,\nlat, and long.\nWhy the other options are incorrect:\nB (header 'false'): Default behavior; would keep reading header as data.\nC / D (sep): Used to specify the delimiter; not relevant unless the file uses a different\nseparator (e.g., |).\nReference (Databricks Apache Spark 3.5 - Python / Study Guide):\nPySpark SQL Data Sources - CSV options (header, inferSchema, sep).\nDatabricks Exam Guide (June 2025): Section \"Using Spark SQL\" - Reading data from files\nwith different formats using Spark SQL and DataFrame APIs."
    },
    {
        "question": "A developer is working on a Spark application that processes a large dataset using SQL\nqueries. Despite having a large cluster, the developer notices that the job is underutilizing the\navailable resources. Executors remain idle for most of the time, and logs reveal that the\nnumber of tasks per stage is very low. The developer suspects that this is causing suboptimal\ncluster performance.\nWhich action should the developer take to improve cluster utilization?",
        "options": [
            "A. Increase the value of spark.sql.shuffle.partitions",
            "B. Reduce the value of spark.sql.shuffle.partitions",
            "C. Enable dynamic resource allocation to scale resources as needed",
            "D. Increase the size of the dataset to create more partitions"
        ],
        "answer": [
            "A"
        ],
        "explanation": "In Spark SQL and DataFrame operations, the configuration parameter\nspark.sql.shuffle.partitions defines the number of partitions created during shuffle operations\nsuch as join, groupBy, and distinct.\nThe default value (in Spark 3.5) is 200.\nIf this number is too low, Spark creates fewer tasks, leading to idle executors and poor\ncluster utilization.\nIncreasing this value allows Spark to create more tasks that can run in parallel across\nexecutors, effectively using more cluster resources.\nCorrect approach:\nspark.conf.set(\"spark.sql.shuffle.partitions\", 400)\nThis increases the parallelism level of shuffle stages and improves overall resource\nutilization.\nWhy the other options are incorrect:\nB: Reducing partitions further would decrease parallelism and worsen the underutilization\nissue.\nC: Dynamic resource allocation scales executors up or down based on workload, but it\ndoesn't fix low task parallelism caused by insufficient shuffle partitions.\nD: Increasing dataset size is not a tuning solution and doesn't address task-level under-\nparallelization.\nReference (Databricks Apache Spark 3.5 - Python / Study Guide):\nSpark SQL Configuration: spark.sql.shuffle.partitions - controls the number of shuffle\npartitions.\nDatabricks Exam Guide (June 2025): Section \"Troubleshooting and Tuning Apache Spark\nDataFrame API Applications\" - tuning strategies, partitioning, and optimizing cluster utilization\n."
    },
    {
        "question": "Which UDF implementation calculates the length of strings in a Spark DataFrame?",
        "options": [
            "A. df.withColumn(\"length\", spark.udf(\"len\", StringType()))",
            "B. df.select(length(col(\"stringColumn\")).alias(\"length\"))",
            "C. spark.udf.register(\"stringLength\", lambda s: len(s))",
            "D. df.withColumn(\"length\", udf(lambda s: len(s), StringType()))"
        ],
        "answer": [
            "B"
        ],
        "explanation": "Option B uses Spark's built-in SQL function length(), which is efficient and avoids the\noverhead of a Python UDF:\nfrom pyspark.sql.functions import length, col\ndf.select(length(col(\"stringColumn\")).alias(\"length\"))\nExplanation of other options:\nOption A is incorrect syntax; spark.udf is not called this way.\nOption C registers a UDF but doesn't apply it in the DataFrame transformation.\nOption D is syntactically valid but uses a Python UDF which is less efficient than built-in\nfunctions.\nFinal answer: B"
    },
    {
        "question": "A data engineer uses a broadcast variable to share a DataFrame containing millions of rows\nacross executors for lookup purposes. What will be the outcome?",
        "options": [
            "A. The job may fail if the memory on each executor is not large enough to accommodate the\nDataFrame being broadcasted",
            "B. The job may fail if the executors do not have enough CPU cores to process the\nbroadcasted dataset",
            "C. The job will hang indefinitely as Spark will struggle to distribute and serialize such a large\nbroadcast variable to all executors",
            "D. The job may fail because the driver does not have enough CPU cores to serialize the\nlarge DataFrame"
        ],
        "answer": [
            "A"
        ],
        "explanation": "In Apache Spark, broadcast variables are used to efficiently distribute large, read-only data to\nall worker nodes. However, broadcasting very large datasets can lead to memory issues on\nexecutors if the data does not fit into the available memory.\nAccording to the Spark documentation:\n\"Broadcast variables allow the programmer to keep a read-only variable cached on each\nmachine rather than shipping a copy of it with tasks. This can greatly reduce the amount of\ndata sent over the network.\" However, it also notes:\n\"Using the broadcast functionality available in SparkContext can greatly reduce the size of\neach serialized task, and the cost of launching a job over a cluster. If your tasks use any\nlarge object from the driver program inside of them (e.g., a static lookup table), consider\nturning it into a broadcast variable.\" But caution is advised when broadcasting large datasets:\n\"Broadcasting large variables can cause out-of-memory errors if the data does not fit in the\nmemory of each executor.\" Therefore, if the broadcasted DataFrame containing millions of\nrows exceeds the memory capacity of the executors, the job may fail due to memory\nconstraints."
    },
    {
        "question": "A data engineer noticed improved performance after upgrading from Spark 3.0 to Spark 3.5.\n\n\nThe engineer found that Adaptive Query Execution (AQE) was enabled.\nWhich operation is AQE implementing to improve performance?",
        "options": [
            "A. Dynamically switching join strategies",
            "B. Collecting persistent table statistics and storing them in the metastore for future use",
            "C. Improving the performance of single-stage Spark jobs",
            "D. Optimizing the layout of Delta files on disk"
        ],
        "answer": [
            "A"
        ],
        "explanation": "Adaptive Query Execution (AQE) is a Spark 3.x feature that dynamically optimizes query\nplans at runtime. One of its core features is:\nDynamically switching join strategies (e.g., from sort-merge to broadcast) based on runtime\nstatistics.\nOther AQE capabilities include:\nCoalescing shuffle partitions\nSkew join handling\nOption A is correct.\nOption B refers to statistics collection, which is not AQE's primary function.\nOption C is too broad and not AQE-specific.\nOption D refers to Delta Lake optimizations, unrelated to AQE.\nFinal answer: A"
    },
    {
        "question": "A data engineer is working on a num_df DataFrame and has a Python UDF defined as:\ndef cube_func(val):\nreturn val * val * val\nWhich code fragment registers and uses this UDF as a Spark SQL function to work with the\nDataFrame num_df?",
        "options": [
            "A. spark.udf.register(\"cube_func\", cube_func)\nnum_df.selectExpr(\"cube_func(num)\").show()",
            "B. num_df.select(cube_func(\"num\")).show()",
            "C. spark.createDataFrame(cube_func(\"num\")).show()",
            "D. num_df.register(\"cube_func\").select(\"num\").show()"
        ],
        "answer": [
            "A"
        ],
        "explanation": "To use a Python function as a UDF (User Defined Function) in Spark SQL, it must first be\nregistered using spark.udf.register().\nCorrect usage:\nspark.udf.register(\"cube_func\", cube_func)\nnum_df.selectExpr(\"cube_func(num)\").show()\n\n\nThis registers cube_func as a callable SQL function available in expressions or queries.\nWhy the other options are incorrect:\nB: You must wrap with udf() or selectExpr; calling plain Python functions won't work.\nC: createDataFrame is for building DataFrames, not calling UDFs.\nD: DataFrames cannot directly register UDFs.\nReference:\nPySpark SQL Functions - spark.udf.register() and selectExpr().\nDatabricks Exam Guide (June 2025): Section \"Using Spark SQL\" - user-defined functions and\nSpark SQL integration."
    },
    {
        "question": "What is the benefit of using Pandas on Spark for data transformations?\nOptions:",
        "options": [
            "A. It is available only with Python, thereby reducing the learning curve.",
            "B. It computes results immediately using eager execution, making it simple to use.",
            "C. It runs on a single node only, utilizing the memory with memory-bound DataFrames and\nhence cost-efficient.",
            "D. It executes queries faster using all the available cores in the cluster as well as provides\nPandas's rich set of features."
        ],
        "answer": [
            "D"
        ],
        "explanation": "Pandas API on Spark (formerly Koalas) offers:\nFamiliar Pandas-like syntax\nDistributed execution using Spark under the hood\nScalability for large datasets across the cluster\nIt provides the power of Spark while retaining the productivity of Pandas."
    },
    {
        "question": "A data engineer observes that the upstream streaming source feeds the event table\nfrequently and sends duplicate records. Upon analyzing the current production table, the data\nengineer found that the time difference in the event_timestamp column of the duplicate\nrecords is, at most, 30 minutes.\nTo remove the duplicates, the engineer adds the code:\ndf = df.withWatermark(\"event_timestamp\", \"30 minutes\")\nWhat is the result?",
        "options": [
            "A. It removes all duplicates regardless of when they arrive.",
            "B. It accepts watermarks in seconds and the code results in an error.",
            "C. It removes duplicates that arrive within the 30-minute window specified by the watermark.",
            "D. It is not able to handle deduplication in this scenario."
        ],
        "answer": [
            "C"
        ],
        "explanation": "In Structured Streaming, a watermark defines the maximum delay for event-time data to be\nconsidered in stateful operations like deduplication or window aggregations.\nBehavior:\ndf = df.withWatermark(\"event_timestamp\", \"30 minutes\")\n\n\nThis sets a 30-minute watermark, meaning Spark will only keep track of events that arrive\nwithin 30 minutes of the latest event time seen so far. When used with:\ndf.dropDuplicates([\"event_id\", \"event_timestamp\"])\nSpark removes duplicates that arrive within the watermark threshold (in this case, within 30\nminutes).\nWhy other options are incorrect:\nA: Watermarks do not remove all duplicates; they only manage those within the defined\nevent-time window.\nB: Watermark durations can be expressed as strings like \"30 minutes\", \"10 seconds\", etc.,\nnot only seconds.\nD: Structured Streaming supports deduplication using withWatermark() and dropDuplicates().\nReference (Databricks Apache Spark 3.5 - Python / Study Guide):\nPySpark Structured Streaming Guide - withWatermark() and dropDuplicates() methods for\nevent-time deduplication.\nDatabricks Certified Associate Developer for Apache Spark Exam Guide (June 2025):\nSection \"Structured Streaming\" - Topic: Streaming Deduplication with and without watermark\nusage."
    },
    {
        "question": "An engineer has two DataFrames - df1 (small) and df2 (large). To optimize the join, the\nengineer uses a broadcast join:\nfrom pyspark.sql.functions import broadcast\ndf_result = df2.join(broadcast(df1), on=\"id\", how=\"inner\")\nWhat is the purpose of using broadcast() in this scenario?",
        "options": [
            "A. It increases the partition size for df1 and df2.",
            "B. It ensures that the join happens only when the id values are identical.",
            "C. It reduces the number of shuffle operations by replicating the smaller DataFrame to all\nnodes.",
            "D. It filters the id values before performing the join."
        ],
        "answer": [
            "C"
        ],
        "explanation": "A broadcast join is a type of join where the smaller DataFrame is replicated (broadcast) to all\nworker nodes in the cluster. This avoids shuffling the large DataFrame across the network.\nBenefits:\nEliminates shuffle for the smaller dataset.\nGreatly improves performance when one side of the join is small enough to fit in memory.\nCorrect usage example:\ndf_result = df2.join(broadcast(df1), \"id\")\nThis is a map-side join, where each executor joins its local partition of the large dataset with\nthe broadcasted copy of the small one.\nWhy the other options are incorrect:\nA: Broadcasting does not change partition sizes.\nB: Joins always match on key equality; this is not specific to broadcast joins.\nD: Broadcasting does not filter; it distributes data for faster joins.\n\n\nReference:\nDatabricks Exam Guide (June 2025): Section \"Developing Apache Spark\nDataFrame/DataSet API Applications\" - broadcast joins and partitioning strategies.\nPySpark SQL Functions - broadcast() method documentation."
    },
    {
        "question": "A data engineer replaces the exact percentile() function with approx_percentile() to improve\nperformance, but the results are drifting too far from expected values.\nWhich change should be made to solve the issue?",
        "options": [
            "A. Decrease the first value of the percentage parameter to increase the accuracy of the\npercentile ranges",
            "B. Decrease the value of the accuracy parameter in order to decrease the memory usage but\nalso improve the accuracy",
            "C. Increase the last value of the percentage parameter to increase the accuracy of the\npercentile ranges",
            "D. Increase the value of the accuracy parameter in order to increase the memory usage but\nalso improve the accuracy"
        ],
        "answer": [
            "D"
        ],
        "explanation": "The approx_percentile function in Spark is a performance-optimized alternative to percentile.\nIt takes an optional accuracy parameter:\napprox_percentile(column, percentage, accuracy)\nHigher accuracy values → more precise results, but increased memory/computation.\nLower values → faster but less accurate.\nFrom the documentation:\n\"Increasing the accuracy improves precision but increases memory usage.\" Final answer: D"
    },
    {
        "question": "Which Spark configuration controls the number of tasks that can run in parallel on an\nexecutor?",
        "options": [
            "A. spark.executor.cores",
            "B. spark.task.maxFailures",
            "C. spark.executor.memory",
            "D. spark.sql.shuffle.partitions"
        ],
        "answer": [
            "A"
        ],
        "explanation": "The Spark configuration spark.executor.cores defines how many concurrent tasks can be\nexecuted within a single executor process.\nEach executor is assigned a number of CPU cores.\nEach core executes one task at a time.\nTherefore, increasing spark.executor.cores allows an executor to run more tasks\n\n\nconcurrently.\nExample:\n--conf spark.executor.cores=4\n→ Each executor can run 4 parallel tasks.\nWhy the other options are incorrect:\nB (spark.task.maxFailures): Sets retry attempts for failed tasks.\nC (spark.executor.memory): Sets executor memory, not concurrency.\nD (spark.sql.shuffle.partitions): Defines number of shuffle partitions, not executor\nconcurrency.\nReference:\nSpark Configuration Guide - Executor cores, tasks, and parallelism.\nDatabricks Exam Guide (June 2025): Section \"Apache Spark Architecture and Components\"\n- executor configuration, CPU cores, and parallel task execution."
    },
    {
        "question": "A Spark developer wants to improve the performance of an existing PySpark UDF that runs a\nhash function that is not available in the standard Spark functions library. The existing UDF\ncode is:\nimport hashlib\nimport pyspark.sql.functions as sf\nfrom pyspark.sql.types import StringType\ndef shake_256(raw):\nreturn hashlib.shake_256(raw.encode()).hexdigest(20)\nshake_256_udf = sf.udf(shake_256, StringType())\nThe developer wants to replace this existing UDF with a Pandas UDF to improve\nperformance. The developer changes the definition of shake_256_udf to this:shake_256_udf = sf.pandas_udf(shake_256, StringType()) However, the developer receives\nthe error:\nWhat should the signature of the shake_256() function be changed to in order to fix this\nerror?",
        "options": [
            "A. def shake_256(df: pd.Series) -> str:",
            "B. def shake_256(df: Iterator[pd.Series]) -> Iterator[pd.Series]:",
            "C. def shake_256(raw: str) -> str:",
            "D. def shake_256(df: pd.Series) -> pd.Series:"
        ],
        "answer": [
            "D"
        ],
        "explanation": "When converting a standard PySpark UDF to a Pandas UDF for performance optimization,\nthe function must operate on a Pandas Series as input and return a Pandas Series as output.\nIn this case, the original function signature:\ndef shake_256(raw: str) -> str\nis scalar - not compatible with Pandas UDFs.\nAccording to the official Spark documentation:\n\"Pandas UDFs operate on pandas.Series and return pandas.Series. The function definition\nshould be:\ndef my_udf(s: pd.Series) -> pd.Series:\nand it must be registered using pandas_udf(...).\"\nTherefore, to fix the error:\nThe function should be updated to:\ndef shake_256(df: pd.Series) -> pd.Series:\nreturn df.apply(lambda x: hashlib.shake_256(x.encode()).hexdigest(20))\nThis will allow Spark to efficiently execute the Pandas UDF in vectorized form, improving\nperformance compared to standard UDFs."
    },
    {
        "question": "A Spark application is experiencing performance issues in client mode because the driver is\nresource-constrained.\nHow should this issue be resolved?",
        "options": [
            "A. Add more executor instances to the cluster",
            "B. Increase the driver memory on the client machine",
            "C. Switch the deployment mode to cluster mode",
            "D. Switch the deployment mode to local mode"
        ],
        "answer": [
            "C"
        ],
        "explanation": "In Spark's client mode, the driver runs on the local machine that submitted the job. If that\nmachine is resource-constrained (e.g., low memory), performance degrades.\nFrom the Spark documentation:\n\"In cluster mode, the driver runs inside the cluster, benefiting from cluster resources and\nscalability.\" Option A is incorrect - executors do not help the driver directly.\nOption B might help short-term but does not scale.\n\n\nOption C is correct - switching to cluster mode moves the driver to the cluster.\nOption D (local mode) is for development/testing, not production.\nFinal answer: C"
    },
    {
        "question": "A developer needs to produce a Python dictionary using data stored in a small Parquet table,\nwhich looks like this:\nregion_id\nregion_name\n10\nNorth\n12\nEast\n14\nWest\nThe resulting Python dictionary must contain a mapping of region_id to region_name,\ncontaining the smallest 3 region_id values.\nWhich code fragment meets the requirements?",
        "options": [
            "A. regions_dict = dict(regions.take(3))",
            "B. regions_dict = regions.select(\"region_id\", \"region_name\").take(3)",
            "C. regions_dict = dict(regions.select(\"region_id\", \"region_name\").rdd.collect())",
            "D. regions_dict = dict(regions.orderBy(\"region_id\").limit(3).rdd.map(lambda x: (x.region_id,\nx.region_name)).collect())"
        ],
        "answer": [
            "D"
        ],
        "explanation": "To create a Python dictionary from a Spark DataFrame, you can first collect the data to the\ndriver node and then convert it into a Python dictionary using dict().\nSteps:\nSelect only relevant columns.\nOrder by region_id to get the smallest ones.\nLimit to 3 rows.\n\n\nMap each row into key-value pairs.\nCollect results to the driver and convert to a dictionary.\nCorrect code:\nregions_dict = dict(\nregions.orderBy(\"region_id\")\n.limit(3)\n.rdd.map(lambda x: (x.region_id, x.region_name))\n.collect()\n)\nThis produces a dictionary like:\n{10: 'North', 12: 'East', 14: 'West'}\nWhy the other options are incorrect:\nA/B: take(3) returns a list of Row objects, not key-value pairs.\nC: Doesn't order or limit by smallest IDs, so the result may not be correct.\nReference:\nPySpark RDD API - map() and collect().\nDatabricks Exam Guide (June 2025): Section \"Using Spark DataFrame APIs\" - covers\nDataFrame-to-local data conversions and collect operations."
    },
    {
        "question": "A developer is working with a pandas DataFrame containing user behavior data from a web\napplication.\nWhich approach should be used for executing a groupBy operation in parallel across all\nworkers in Apache Spark 3.5?\nA)\nUse the applylnPandas API\nB)\nC)",
        "options": [
            "A. Use the applyInPandas API:\ndf.groupby(\"user_id\").applyInPandas(mean_func, schema=\"user_id long, value\ndouble\").show()",
            "B. Use the mapInPandas API:\ndf.mapInPandas(mean_func, schema=\"user_id long, value double\").show()",
            "C. Use a regular Spark UDF:\nfrom pyspark.sql.functions import mean\ndf.groupBy(\"user_id\").agg(mean(\"value\")).show()",
            "D. Use a Pandas UDF:\n@pandas_udf(\"double\")\ndef mean_func(value: pd.Series) -> float:\nreturn value.mean()\ndf.groupby(\"user_id\").agg(mean_func(df[\"value\"])).show()"
        ],
        "answer": [
            "A"
        ],
        "explanation": "The correct approach to perform a parallelized groupBy operation across Spark worker\nnodes using Pandas API is via applyInPandas. This function enables grouped map\noperations using Pandas logic in a distributed Spark environment. It applies a user-defined\nfunction to each group of data represented as a Pandas DataFrame.\nAs per the Databricks documentation:\n\"applyInPandas() allows for vectorized operations on grouped data in Spark. It applies a\nuser-defined function to each group of a DataFrame and outputs a new DataFrame. This is\nthe recommended approach for using Pandas logic across grouped data with parallel\nexecution.\" Option A is correct and achieves this parallel execution.\nOption B (mapInPandas) applies to the entire DataFrame, not grouped operations.\nOption C uses built-in aggregation functions, which are efficient but not customizable with\nPandas logic.\nOption D creates a scalar Pandas UDF which does not perform a group-wise transformation.\nTherefore, to run a groupBy with parallel Pandas logic on Spark workers, Option A using\napplyInPandas is the only correct answer."
    },
    {
        "question": "A developer wants to test Spark Connect with an existing Spark application.\nWhat are the two alternative ways the developer can start a local Spark Connect server\nwithout changing their existing application code? (Choose 2 answers)",
        "options": [
            "A. Execute their pyspark shell with the option --remote \"https://localhost\"",
            "B. Execute their pyspark shell with the option --remote \"sc://localhost\"",
            "C. Set the environment variable SPARK_REMOTE=\"sc://localhost\" before starting the\npyspark shell",
            "D. Add .remote(\"sc://localhost\") to their SparkSession.builder calls in their Spark code",
            "E. Ensure the Spark property spark.connect.grpc.binding.port is set to 15002 in the\napplication code"
        ],
        "answer": [
            "B",
            "C"
        ],
        "explanation": "Spark Connect enables decoupling of the client and Spark driver processes, allowing remote\naccess. Spark supports configuring the remote Spark Connect server in multiple ways:\nFrom Databricks and Spark documentation:\nOption B (--remote \"sc://localhost\") is a valid command-line argument for the pyspark shell to\nconnect using Spark Connect.\nOption C (setting SPARK_REMOTE environment variable) is also a supported method to\nconfigure the remote endpoint.\nOption A is incorrect because Spark Connect uses the sc:// protocol, not https://.\nOption D requires modifying the code, which the question explicitly avoids.\nOption E configures the port on the server side but doesn't start a client connection.\nFinal Answers: B and C"
    },
    {
        "question": "Which components of Apache Spark's Architecture are responsible for carrying out tasks\nwhen assigned to them?",
        "options": [
            "A. Driver Nodes",
            "B. Executors",
            "C. CPU Cores",
            "D. Worker Nodes"
        ],
        "answer": [
            "B"
        ],
        "explanation": "In Spark's distributed architecture:\nThe Driver Node coordinates the execution of a Spark application. It converts the logical plan\ninto a physical plan of stages and tasks.\nThe Executors, running on Worker Nodes, are responsible for executing tasks assigned by\nthe driver and storing data (in memory or disk) during execution.\nKey point:\nExecutors are the active agents that perform the actual computations on data partitions. Each\nexecutor runs multiple tasks in parallel using available CPU cores.\nWhy the other options are incorrect:\nA (Driver Nodes): The driver schedules tasks; it doesn't execute them.\n\n\nC (CPU Cores): CPU cores execute within executors, but they are hardware, not Spark\narchitectural components.\nD (Worker Nodes): Worker nodes host executors but do not directly execute tasks; executors\ndo.\nReference (Databricks Apache Spark 3.5 - Python / Study Guide):\nSpark Architecture Components - Driver, Executors, Cluster Manager, Worker Nodes.\nDatabricks Exam Guide (June 2025): Section \"Apache Spark Architecture and Components\"\n- describes the roles of driver and executor nodes in distributed processing."
    },
    {
        "question": "How can a Spark developer ensure optimal resource utilization when running Spark jobs in\nLocal Mode for testing?\nOptions:",
        "options": [
            "A. Configure the application to run in cluster mode instead of local mode.",
            "B. Increase the number of local threads based on the number of CPU cores.",
            "C. Use the spark.dynamicAllocation.enabled property to scale resources dynamically.",
            "D. Set the spark.executor.memory property to a large value."
        ],
        "answer": [
            "B"
        ],
        "explanation": "When running in local mode (e.g., local[4]), the number inside the brackets defines how many\nthreads Spark will use.\nUsing local[*] ensures Spark uses all available CPU cores for parallelism.\nExample:\nspark-submit --master local[*]\nDynamic allocation and executor memory apply to cluster-based deployments, not local\nmode."
    },
    {
        "question": "A data scientist wants to ingest a directory full of plain text files so that each record in\nthe output DataFrame contains the entire contents of a single file and the full path of the file\nthe text was read from.\nThe first attempt does read the text files, but each record contains a single line. This code is\nshown below:\ntxt_path = \"/datasets/raw_txt/*\"\ndf = spark.read.text(txt_path) # one row per line by default\ndf = df.withColumn(\"file_path\", input_file_name()) # add full path\nWhich code change can be implemented in a DataFrame that meets the data scientist's\nrequirements?",
        "options": [
            "A. Add the option wholetext to the text() function.",
            "B. Add the option lineSep to the text() function.",
            "C. Add the option wholetext=False to the text() function.",
            "D. Add the option lineSep=\", \" to the text() function."
        ],
        "answer": [
            "A"
        ],
        "explanation": "By default, the spark.read.text() method reads a text file one line per record. This means that\n\n\neach line in a text file becomes one row in the resulting DataFrame.\nTo read each file as a single record, Apache Spark provides the option wholetext, which,\nwhen set to True, causes Spark to treat the entire file contents as one single string per row.\nCorrect usage:\ndf = spark.read.option(\"wholetext\", True).text(txt_path)\nThis way, each record in the DataFrame will contain the full content of one file instead of one\nline per record.\nTo also include the file path, the function input_file_name() can be used to create an\nadditional column that stores the complete path of the file being read:\nfrom pyspark.sql.functions import input_file_name\ndf = spark.read.option(\"wholetext\", True).text(txt_path) \\\n.withColumn(\"file_path\", input_file_name())\nThis approach satisfies both requirements from the question:\nEach record holds the entire contents of a file.\nEach record also contains the file path from which the text was read.\nWhy the other options are incorrect:\nB or D (lineSep) - The lineSep option only defines the delimiter between lines. It does not\ncombine the entire file content into a single record.\nC (wholetext=False) - This is the default behavior, which still reads one record per line rather\nthan per file.\nReference (Databricks Apache Spark 3.5 - Python / Study Guide):\nPySpark API Reference: DataFrameReader.text - describes the wholetext option.\nPySpark Functions: input_file_name() - adds a column with the source file path.\nDatabricks Certified Associate Developer for Apache Spark Exam Guide (June 2025):\nSection \"Using Spark DataFrame APIs\" - covers reading files and handling DataFrames."
    },
    {
        "question": "Given a CSV file with the content:\nAnd the following code:\nfrom pyspark.sql.types import *\nschema = StructType([\nStructField(\"name\", StringType()),\nStructField(\"age\", IntegerType())\n])\nspark.read.schema(schema).csv(path).collect()\nWhat is the resulting output?",
        "options": [
            "A. [Row(name='bambi'), Row(name='alladin', age=20)]",
            "B. [Row(name='alladin', age=20)]",
            "C. [Row(name='bambi', age=None), Row(name='alladin', age=20)]",
            "D. The code throws an error due to a schema mismatch."
        ],
        "answer": [
            "C"
        ],
        "explanation": "In Spark, when a CSV row does not match the provided schema, Spark does not raise an\nerror by default. Instead, it returns null for fields that cannot be parsed correctly.\nIn the first row, \"hello\" cannot be cast to Integer for the age field → Spark sets age=None In\nthe second row, \"20\" is a valid integer → age=20 So the output will be:\n[Row(name='bambi', age=None), Row(name='alladin', age=20)]\nFinal answer: C"
    },
    {
        "question": "A data engineer wants to create an external table from a JSON file located at /data/input.json\nwith the following requirements:\nCreate an external table named users\nAutomatically infer schema\nMerge records with differing schemas\nWhich code snippet should the engineer use?\nOptions:",
        "options": [
            "A. CREATE TABLE users USING json OPTIONS (path '/data/input.json')",
            "B. CREATE EXTERNAL TABLE users USING json OPTIONS (path '/data/input.json')",
            "C. CREATE EXTERNAL TABLE users USING json OPTIONS (path '/data/input.json',\nmergeSchema 'true')",
            "D. CREATE EXTERNAL TABLE users USING json OPTIONS (path '/data/input.json',\nschemaMerge 'true')"
        ],
        "answer": [
            "C"
        ],
        "explanation": "To create an external table and enable schema merging, the correct syntax is:\nCREATE EXTERNAL TABLE users\nUSING json\nOPTIONS (\npath '/data/input.json',\nmergeSchema 'true'\n)\nmergeSchema is the correct option key (not schemaMerge)\nEXTERNAL allows Spark to query files without managing their lifecycle"
    },
    {
        "question": "A data scientist wants each record in the DataFrame to contain:\nThe first attempt at the code does read the text files but each record contains a single line.\nThis code is shown below:\n\n\n \nThe entire contents of a file\nThe full file path\nThe issue: reading line-by-line rather than full text per file.\nCode:\ncorpus = spark.read.text(\"/datasets/raw_txt/*\") \\\n.select('*', '_metadata.file_path')\nWhich change will ensure one record per file?\nOptions:",
        "options": [
            "A. Add the option wholetext=True to the text() function",
            "B. Add the option lineSep='\\n' to the text() function",
            "C. Add the option wholetext=False to the text() function",
            "D. Add the option lineSep=\", \" to the text() function"
        ],
        "answer": [
            "A"
        ],
        "explanation": "To read each file as a single record, use:\nspark.read.text(path, wholetext=True)\nThis ensures that Spark reads the entire file contents into one row."
    },
    {
        "question": "A data scientist is working on a large dataset in Apache Spark using PySpark. The data\nscientist has a DataFrame df with columns user_id, product_id, and purchase_amount and\nneeds to perform some operations on this data efficiently.\nWhich sequence of operations results in transformations that require a shuffle followed by\ntransformations that do not?",
        "options": [
            "A. df.filter(df.purchase_amount > 100).groupBy(\"user_id\").sum(\"purchase_amount\")",
            "B. df.withColumn(\"discount\", df.purchase_amount * 0.1).select(\"discount\")",
            "C. df.withColumn(\"purchase_date\", current_date()).where(\"total_purchase > 50\")",
            "D. df.groupBy(\"user_id\").agg(sum(\"purchase_amount\").alias(\"total_purchase\")).repartition(10\n)"
        ],
        "answer": [
            "D"
        ],
        "explanation": "Shuffling occurs in operations like groupBy, reduceByKey, or join-which cause data to be\nmoved across partitions. The repartition() operation can also cause a shuffle, but in this\ncontext, it follows an aggregation.\nIn Option D, the groupBy followed by agg results in a shuffle due to grouping across nodes.\nThe repartition(10) is a partitioning transformation but does not involve a new shuffle since\n\n\nthe data is already grouped.\nThis sequence - shuffle (groupBy) followed by non-shuffling (repartition) - is correct.\nOption A does the opposite: the filter does not cause a shuffle, but groupBy does - this makes\nit the wrong order."
    },
    {
        "question": "A Data Analyst is working on employees_df and needs to add a new column where a 10%\ntax is calculated on the salary.\nAdditionally, the DataFrame contains the column age, which is not needed.\nWhich code fragment adds the tax column and removes the age column?",
        "options": [
            "A. employees_df = employees_df.withColumn(\"tax\", col(\"salary\") * 0.1).drop(\"age\")",
            "B. employees_df = employees_df.withColumn(\"tax\", lit(0.1)).drop(\"age\")",
            "C. employees_df = employees_df.dropField(\"age\").withColumn(\"tax\", col(\"salary\") * 0.1)",
            "D. employees_df = employees_df.withColumn(\"tax\", col(\"salary\") + 0.1).drop(\"age\")"
        ],
        "answer": [
            "A"
        ],
        "explanation": "To create a new calculated column in Spark, use the .withColumn() method.\nTo remove an unwanted column, use the .drop() method.\nCorrect syntax:\nfrom pyspark.sql.functions import col\nemployees_df = employees_df.withColumn(\"tax\", col(\"salary\") * 0.1).drop(\"age\")\n.withColumn(\"tax\", col(\"salary\") * 0.1) → adds a new column where tax = 10% of salary.\n.drop(\"age\") → removes the age column from the DataFrame.\nWhy the other options are incorrect:\nB: lit(0.1) creates a constant value, not a calculated tax.\nC: .dropField() is not a DataFrame API method (used only in struct field manipulations).\nD: Adds 0.1 to salary instead of calculating 10%.\nReference:\nPySpark DataFrame API - withColumn(), drop(), and col().\nDatabricks Exam Guide (June 2025): Section \"Developing Apache Spark\nDataFrame/DataSet API Applications\" - manipulating, renaming, and dropping columns."
    },
    {
        "question": "You have:\nDataFrame A: 128 GB of transactions\nDataFrame B: 1 GB user lookup table\nWhich strategy is correct for broadcasting?",
        "options": [
            "A. DataFrame B should be broadcasted because it is smaller and will eliminate the need for\nshuffling itself",
            "B. DataFrame B should be broadcasted because it is smaller and will eliminate the need for\n\n\nshuffling DataFrame A",
            "C. DataFrame A should be broadcasted because it is larger and will eliminate the need for\nshuffling DataFrame B",
            "D. DataFrame A should be broadcasted because it is smaller and will eliminate the need for\nshuffling itself"
        ],
        "answer": [
            "B"
        ],
        "explanation": "Broadcast joins work by sending the smaller DataFrame to all executors, eliminating the\nshuffle of the larger DataFrame.\nFrom Spark documentation:\n\"Broadcast joins are efficient when one DataFrame is small enough to fit in memory. Spark\navoids shuffling the larger table.\" DataFrame B (1 GB) fits within the default threshold and\nshould be broadcasted.\nIt eliminates the need to shuffle the large DataFrame A.\nFinal answer: B"
    },
    {
        "question": "A data engineer is implementing a streaming pipeline with watermarking to handle late-\narriving records.\nThe engineer has written the following code:\ninputStream \\\n.withWatermark(\"event_time\", \"10 minutes\") \\\n.groupBy(window(\"event_time\", \"15 minutes\"))\nWhat happens to data that arrives after the watermark threshold?",
        "options": [
            "A. Any data arriving more than 10 minutes after the watermark threshold will be ignored and\nnot included in the aggregation.",
            "B. Records that arrive later than the watermark threshold (10 minutes) will automatically be\nincluded in the aggregation if they fall within the 15-minute window.",
            "C. Data arriving more than 10 minutes after the latest watermark will still be included in the\naggregation but will be placed into the next window.",
            "D. The watermark ensures that late data arriving within 10 minutes of the latest event time\nwill be processed and included in the windowed aggregation."
        ],
        "answer": [
            "A"
        ],
        "explanation": "Watermarking in Structured Streaming defines how late a record can arrive based on event\ntime before Spark discards it.\nBehavior:\n.withWatermark(\"event_time\", \"10 minutes\")\nThis means Spark will keep state for 10 minutes beyond the maximum event time seen so\nfar.\nAny data arriving later than 10 minutes after the current watermark is ignored - it will not be\nincluded in the aggregation or output.\nWhy the other options are incorrect:\nB: Late data beyond the watermark threshold is not included.\n\n\nC: Late data is not moved to a new window; it's simply dropped.\nD: True for late data within the watermark threshold, not after it.\nReference:\nSpark Structured Streaming Guide - withWatermark() behavior and late data handling.\nDatabricks Exam Guide (June 2025): Section \"Structured Streaming\" - watermarking and\nstate cleanup behavior."
    },
    {
        "question": "An organization has been running a Spark application in production and is considering\ndisabling the Spark History Server to reduce resource usage.\nWhat will be the impact of disabling the Spark History Server in production?",
        "options": [
            "A. Prevention of driver log accumulation during long-running jobs",
            "B. Improved job execution speed due to reduced logging overhead",
            "C. Loss of access to past job logs and reduced debugging capability for completed jobs",
            "D. Enhanced executor performance due to reduced log size"
        ],
        "answer": [
            "C"
        ],
        "explanation": "The Spark History Server provides a web UI for viewing past completed applications,\nincluding event logs, stages, and performance metrics.\nIf disabled:\nSpark jobs still run normally,\nBut users lose the ability to review historical job metrics, DAGs, or logs after completion.\nThus, debugging, performance analysis, and audit capabilities are lost.\nWhy the other options are incorrect:\nA: Disabling History Server doesn't manage logs.\nB/D: Minimal overhead; disabling doesn't improve runtime speed or executor performance.\nReference:\nDatabricks Exam Guide (June 2025): Section \"Apache Spark Architecture and Components\"\n- Spark UI, History Server, and event logging.\nSpark Administration Docs - History Server functionality and configuration."
    },
    {
        "question": "A data engineer is building a Structured Streaming pipeline and wants it to recover from\nfailures or intentional shutdowns by continuing where it left off.\nHow can this be achieved?",
        "options": [
            "A. By configuring the option recoveryLocation during SparkSession initialization.",
            "B. By configuring the option checkpointLocation during readStream.",
            "C. By configuring the option checkpointLocation during writeStream.",
            "D. By configuring the option recoveryLocation during writeStream."
        ],
        "answer": [
            "C"
        ],
        "explanation": "In Structured Streaming, checkpoints store state information (offsets, progress, and\nmetadata) needed to resume a stream after a failure or restart.\n\n\nCorrect usage:\nSet the checkpointLocation option when writing the streaming output:\nstreaming_df.writeStream \\\n.format(\"delta\") \\\n.option(\"checkpointLocation\", \"/path/to/checkpoint/dir\") \\\n.start(\"/path/to/output\")\nSpark uses this checkpoint directory to recover progress automatically and maintain exactly-\nonce semantics.\nWhy the other options are incorrect:\nA/D: recoveryLocation is not a valid Spark configuration option.\nB: Checkpointing must be configured in writeStream, not during readStream.\nReference:\nPySpark Structured Streaming Guide - Checkpointing and recovery.\nDatabricks Exam Guide (June 2025): Section \"Structured Streaming\" - explains checkpointing\nand fault-tolerant streaming recovery."
    },
    {
        "question": "A data scientist at a financial services company is working with a Spark DataFrame\ncontaining transaction records. The DataFrame has millions of rows and includes columns for\ntransaction_id, account_number, transaction_amount, and timestamp. Due to an issue with\nthe source system, some transactions were accidentally recorded multiple times with\nidentical information across all fields. The data scientist needs to remove rows with\nduplicates across all fields to ensure accurate financial reporting.\nWhich approach should the data scientist use to deduplicate the orders using PySpark?",
        "options": [
            "A. df = df.dropDuplicates()",
            "B. df = df.groupBy(\"transaction_id\").agg(F.first(\"account_number\"),\nF.first(\"transaction_amount\"), F.first(\"timestamp\"))",
            "C. df = df.filter(F.col(\"transaction_id\").isNotNull())",
            "D. df = df.dropDuplicates([\"transaction_amount\"])"
        ],
        "answer": [
            "A"
        ],
        "explanation": "dropDuplicates() with no column list removes duplicates based on all columns.\nIt's the most efficient and semantically correct way to deduplicate records that are completely\nidentical across all fields.\nFrom the PySpark documentation:\ndropDuplicates(): Return a new DataFrame with duplicate rows removed, considering all\ncolumns if none are specified.\n- Source: PySpark DataFrame.dropDuplicates() API"
    },
    {
        "question": "Which Spark configuration controls the number of tasks that can run in parallel on the\nexecutor?\nOptions:",
        "options": [
            "A. spark.executor.cores",
            "B. spark.task.maxFailures",
            "C. spark.driver.cores",
            "D. spark.executor.memory"
        ],
        "answer": [
            "A"
        ],
        "explanation": "spark.executor.cores determines how many concurrent tasks an executor can run.\nFor example, if set to 4, each executor can run up to 4 tasks in parallel.\nOther settings:\nspark.task.maxFailures controls task retry logic.\nspark.driver.cores is for the driver, not executors.\nspark.executor.memory sets memory limits, not task concurrency."
    },
    {
        "question": "Given a DataFrame df that has 10 partitions, after running the code:\nresult = df.coalesce(20)\nHow many partitions will the result DataFrame have?",
        "options": [
            "A. 10",
            "B. Same number as the cluster executors",
            "C. 1",
            "D. 20"
        ],
        "answer": [
            "A"
        ],
        "explanation": "The .coalesce(numPartitions) function is used to reduce the number of partitions in a\nDataFrame. It does not increase the number of partitions. If the specified number of partitions\nis greater than the current number, it will not have any effect.\nFrom the official Spark documentation:\n\"coalesce() results in a narrow dependency, e.g. if you go from 1000 partitions to 100\npartitions, there will not be a shuffle, instead each of the 100 new partitions will claim one or\nmore of the current partitions.\" However, if you try to increase partitions using coalesce (e.g.,\nfrom 10 to 20), the number of partitions remains unchanged.\nHence, df.coalesce(20) will still return a DataFrame with 10 partitions."
    },
    {
        "question": "What is the relationship between jobs, stages, and tasks during execution in Apache Spark?\nOptions:",
        "options": [
            "A. A job contains multiple stages, and each stage contains multiple tasks.",
            "B. A job contains multiple tasks, and each task contains multiple stages.",
            "C. A stage contains multiple jobs, and each job contains multiple tasks.",
            "D. A stage contains multiple tasks, and each task contains multiple jobs."
        ],
        "answer": [
            "A"
        ],
        "explanation": "A Spark job is triggered by an action (e.g., count, show).\nThe job is broken into stages, typically one per shuffle boundary.\nEach stage is divided into multiple tasks, which are distributed across worker nodes."
    },
    {
        "question": "Which feature of Spark Connect is considered when designing an application to enable\nremote interaction with the Spark cluster?",
        "options": [
            "A. It provides a way to run Spark applications remotely in any programming language",
            "B. It can be used to interact with any remote cluster using the REST API",
            "C. It allows for remote execution of Spark jobs",
            "D. It is primarily used for data ingestion into Spark from external sources"
        ],
        "answer": [
            "C"
        ],
        "explanation": "Spark Connect introduces a decoupled client-server architecture. Its key feature is enabling\nSpark job submission and execution from remote clients - in Python, Java, etc.\nFrom Databricks documentation:\n\"Spark Connect allows remote clients to connect to a Spark cluster and execute Spark jobs\nwithout being co-located with the Spark driver.\" A is close, but \"any language\" is overstated\n(currently supports Python, Java, etc., not literally all).\nB refers to REST, which is not Spark Connect's mechanism.\nD is incorrect; Spark Connect isn't focused on ingestion.\nFinal answer: C"
    },
    {
        "question": "A data engineer needs to persist a file-based data source to a specific location. However, by\ndefault, Spark writes to the warehouse directory (e.g., /user/hive/warehouse). To override\nthis, the engineer must explicitly define the file path.\nWhich line of code ensures the data is saved to a specific location?\nOptions:",
        "options": [
            "A. users.write(path=\"/some/path\").saveAsTable(\"default_table\")",
            "B. users.write.saveAsTable(\"default_table\").option(\"path\", \"/some/path\")",
            "C. users.write.option(\"path\", \"/some/path\").saveAsTable(\"default_table\")",
            "D. users.write.saveAsTable(\"default_table\", path=\"/some/path\")"
        ],
        "answer": [
            "C"
        ],
        "explanation": "To persist a table and specify the save path, use:\nusers.write.option(\"path\", \"/some/path\").saveAsTable(\"default_table\")\nThe .option(\"path\", ...) must be applied before calling saveAsTable.\nOption A uses invalid syntax (write(path=...)).\nOption B applies .option() after .saveAsTable()-which is too late.\nOption D uses incorrect syntax (no path parameter in saveAsTable)."
    },
    {
        "question": "A Spark application needs to read multiple Parquet files from a directory where the files have\ndiffering but compatible schemas.\nThe data engineer wants to create a DataFrame that includes all columns from all files.\nWhich code should the data engineer use to read the Parquet files and include all columns\nusing Apache Spark?",
        "options": [
            "A. spark.read.parquet(\"/data/parquet/\")",
            "B. spark.read.option(\"mergeSchema\", True).parquet(\"/data/parquet/\")",
            "C. spark.read.format(\"parquet\").option(\"inferSchema\", \"true\").load(\"/data/parquet/\")",
            "D. spark.read.parquet(\"/data/parquet/\").option(\"mergeAllCols\", True)"
        ],
        "answer": [
            "B"
        ],
        "explanation": "When reading Parquet files, Spark infers a unified schema automatically only if all files share\nidentical structures.\nIf files have different but compatible schemas, you must enable schema merging by setting\nthe option mergeSchema=True.\nCorrect syntax:\ndf = spark.read.option(\"mergeSchema\", True).parquet(\"/data/parquet/\")\nThis option ensures Spark merges all discovered fields across Parquet files into one unified\nDataFrame schema.\nWhy the other options are incorrect:\nA: Loads files but ignores extra columns - uses only the first file's schema.\nC: inferSchema applies to CSV/JSON, not Parquet.\nD: mergeAllCols is not a valid Spark option.\nReference:\nSpark SQL Data Sources - Parquet options (mergeSchema, path).\nDatabricks Exam Guide (June 2025): Section \"Using Spark DataFrame APIs\" - reading/writing\nDataFrames with schema evolution and merging."
    },
    {
        "question": "A data engineer is working with Spark SQL and has a large JSON file stored at\n/data/input.json.\nThe file contains records with varying schemas, and the engineer wants to create an external\ntable in Spark SQL that:\nReads directly from /data/input.json.\nInfers the schema automatically.\nMerges differing schemas.\nWhich code snippet should the engineer use?",
        "options": [
            "A. CREATE EXTERNAL TABLE users\nUSING json\nOPTIONS (path '/data/input.json', mergeSchema 'true');",
            "B. CREATE TABLE users\nUSING json\nOPTIONS (path '/data/input.json');",
            "C. CREATE EXTERNAL TABLE users\nUSING json\nOPTIONS (path '/data/input.json', inferSchema 'true');",
            "D. CREATE EXTERNAL TABLE users\nUSING json\nOPTIONS (path '/data/input.json', mergeAll 'true');"
        ],
        "answer": [
            "A"
        ],
        "explanation": "To handle JSON files with evolving or differing schemas, Spark SQL supports the option\nmergeSchema 'true', which merges all fields across files into a unified schema.\nCorrect syntax:\nCREATE EXTERNAL TABLE users\nUSING json\nOPTIONS (path '/data/input.json', mergeSchema 'true');\nThis creates an external table directly on the JSON data, inferring schema automatically and\nmerging variations.\nWhy the other options are incorrect:\nB: Missing schema merge configuration - fails with inconsistent files.\nC: inferSchema applies to CSV/other file types, not JSON.\nD: mergeAll is not a valid Spark SQL option.\nReference:\nSpark SQL Data Sources - JSON file options (mergeSchema, path).\nDatabricks Exam Guide (June 2025): Section \"Using Spark SQL\" - creating external tables\nand schema inference for JSON data."
    },
    {
        "question": "A data engineer wants to write a Spark job that creates a new managed table. If the table\nalready exists, the job should fail and not modify anything.\nWhich save mode and method should be used?",
        "options": [
            "A. saveAsTable with mode ErrorIfExists",
            "B. saveAsTable with mode Overwrite",
            "C. save with mode Ignore",
            "D. save with mode ErrorIfExists"
        ],
        "answer": [
            "A"
        ],
        "explanation": "The method saveAsTable() creates a new table and optionally fails if the table exists.\nFrom Spark documentation:\n\"The mode 'ErrorIfExists' (default) will throw an error if the table already exists.\" Thus:\nOption A is correct.\nOption B (Overwrite) would overwrite existing data - not acceptable here.\nOption C and D use save(), which doesn't create a managed table with metadata in the\nmetastore.\nFinal answer: A"
    },
    {
        "question": "A Data Analyst needs to retrieve employees with 5 or more years of tenure.\nWhich code snippet filters and shows the list?",
        "options": [
            "A. employees_df.filter(employees_df.tenure >= 5).show()",
            "B. employees_df.where(employees_df.tenure >= 5)",
            "C. filter(employees_df.tenure >= 5)",
            "D. employees_df.filter(employees_df.tenure >= 5).collect()"
        ],
        "answer": [
            "A"
        ],
        "explanation": "To filter rows based on a condition and display them in Spark, use filter(...).show():\nemployees_df.filter(employees_df.tenure >= 5).show()\nOption A is correct and shows the results.\nOption B filters but doesn't display them.\nOption C uses Python's built-in filter, not Spark.\nOption D collects the results to the driver, which is unnecessary if .show() is sufficient.\nFinal answer: A"
    },
    {
        "question": "A developer needs to write the output of a complex chain of Spark transformations to a\nParquet table called events.liveLatest.\nConsumers of this table query it frequently with filters on both year and month of the event_ts\ncolumn (a timestamp).\nThe current code:\nfrom pyspark.sql import functions as F\nfinal = df.withColumn(\"event_year\", F.year(\"event_ts\")) \\\n.withColumn(\"event_month\", F.month(\"event_ts\")) \\\n.bucketBy(42, [\"event_year\", \"event_month\"]) \\\n.saveAsTable(\"events.liveLatest\")\nHowever, consumers report poor query performance.\nWhich change will enable efficient querying by year and month?",
        "options": [
            "A. Replace .bucketBy() with .partitionBy(\"event_year\", \"event_month\")",
            "B. Change the bucket count (42) to a lower number",
            "C. Add .sortBy() after .bucketBy()",
            "D. Replace .bucketBy() with .partitionBy(\"event_year\") only"
        ],
        "answer": [
            "A"
        ],
        "explanation": "When queries frequently filter on certain columns, partitioning by those columns ensures\npartition pruning, allowing Spark to scan only relevant directories instead of the entire\ndataset.\nCorrect code:\nfinal.write.partitionBy(\"event_year\", \"event_month\").parquet(\"events.liveLatest\") This\nimproves read performance dramatically for filters like:\nSELECT * FROM events.liveLatest WHERE event_year = 2024 AND event_month = 5;\nbucketBy() helps in clustering and joins, not in partition pruning for file-based tables.\nWhy the other options are incorrect:\nB: Bucket count changes parallelism, not query pruning.\n\n\nC: sortBy organizes data within files, not across partitions.\nD: Partitioning by only one column limits pruning benefits.\nReference:\nSpark SQL DataFrameWriter - partitionBy() for partitioned tables.\nDatabricks Exam Guide (June 2025): Section \"Using Spark SQL\" - partitioning vs. bucketing\nand query optimization."
    },
    {
        "question": "A data engineer is working on a Streaming DataFrame (streaming_df) with the following\nstreaming data:\nid\nname\ncount\ntimestamp\n1\nDelhi\n20\n\n\n2024-09-19T10:11\n1\nDelhi\n50\n2024-09-19T10:12\n2\nLondon\n50\n2024-09-19T10:15\n3\nParis\n30\n2024-09-19T10:18\n3\nParis\n20\n2024-09-19T10:20\n4\nWashington\n10\n2024-09-19T10:22\nWhich operation is supported with streaming_df?",
        "options": [
            "A. streaming_df.count()",
            "B. streaming_df.filter(\"count < 30\")",
            "C. streaming_df.select(countDistinct(\"name\"))",
            "D. streaming_df.show()"
        ],
        "answer": [
            "B"
        ],
        "explanation": "In Structured Streaming, only transformation operations are allowed on streaming\nDataFrames. These include select(), filter(), where(), groupBy(), withColumn(), etc.\nExample of supported transformation:\nfiltered_df = streaming_df.filter(\"count < 30\")\nHowever, actions such as count(), show(), and collect() are not supported directly on\nstreaming DataFrames because streaming queries are unbounded and never finish until\nstopped.\nTo perform aggregations, the query must be executed through writeStream and an output\nsink.\nWhy the other options are incorrect:\nA: count() is an action, not allowed directly on streaming DataFrames.\nC: countDistinct() is a stateful aggregation, not supported outside of a proper streaming\nquery.\nD: show() is also an action, unsupported on streaming queries.\nReference:\nPySpark Structured Streaming Programming Guide - supported transformations and actions.\nDatabricks Exam Guide (June 2025): Section \"Structured Streaming\" - performing operations\n\n\non streaming DataFrames and understanding supported transformations."
    },
    {
        "question": "A data engineer is building an Apache Spark™ Structured Streaming application to process a\nstream of JSON events in real time. The engineer wants the application to be fault-tolerant\nand resume processing from the last successfully processed record in case of a failure. To\nachieve this, the data engineer decides to implement checkpoints.\nWhich code snippet should the data engineer use?",
        "options": [
            "A. query = streaming_df.writeStream \\\n.format(\"console\") \\\n.option(\"checkpoint\", \"/path/to/checkpoint\") \\\n.outputMode(\"append\") \\\n.start()",
            "B. query = streaming_df.writeStream \\\n.format(\"console\") \\\n.outputMode(\"append\") \\\n.option(\"checkpointLocation\", \"/path/to/checkpoint\") \\\n.start()",
            "C. query = streaming_df.writeStream \\\n.format(\"console\") \\\n.outputMode(\"complete\") \\\n.start()",
            "D. query = streaming_df.writeStream \\\n.format(\"console\") \\\n.outputMode(\"append\") \\\n.start()"
        ],
        "answer": [
            "B"
        ],
        "explanation": "To enable fault tolerance and ensure that Spark can resume from the last committed offset\nafter failure, you must configure a checkpoint location using the correct option key:\n\"checkpointLocation\".\nFrom the official Spark Structured Streaming guide:\n\"To make a streaming query fault-tolerant and recoverable, a checkpoint directory must be\nspecified using .option(\"checkpointLocation\", \"/path/to/dir\").\" Explanation of options:\nOption A uses an invalid option name: \"checkpoint\" (should be \"checkpointLocation\") Option\nB is correct: it sets checkpointLocation properly Option C lacks checkpointing and won't\nresume after failure Option D also lacks checkpointing configuration"
    },
    {
        "question": "Which feature of Spark Connect should be considered when designing an application that\n\n\nplans to enable remote interaction with a Spark cluster?",
        "options": [
            "A. It is primarily used for data ingestion into Spark from external sources.",
            "B. It provides a way to run Spark applications remotely in any programming language.",
            "C. It can be used to interact with any remote cluster using the REST API.",
            "D. It allows for remote execution of Spark jobs."
        ],
        "answer": [
            "D"
        ],
        "explanation": "Spark Connect enables remote execution of Spark jobs by decoupling the client from the\ndriver using the Spark Connect protocol (gRPC).\nIt allows users to run Spark code from different environments (like notebooks, IDEs, or\nremote clients) while executing jobs on the cluster.\nKey Features:\nEnables remote interaction between client and Spark driver.\nSupports interactive development and lightweight client sessions.\nImproves developer productivity without needing driver resources locally.\nWhy the other options are incorrect:\nA: Spark Connect is not limited to ingestion tasks.\nB: It allows multi-language clients (Python, Scala, etc.) but runs via Spark Connect API, not\narbitrary remote code.\nC: Uses gRPC protocol, not REST.\nReference:\nDatabricks Exam Guide (June 2025): Section \"Using Spark Connect to Deploy Applications\" -\ndescribes Spark Connect architecture and remote execution model.\nSpark 3.5 Documentation - Spark Connect overview and client-server protocol."
    },
    {
        "question": "Given this view definition:\ndf.createOrReplaceTempView(\"users_vw\")\nWhich approach can be used to query the users_vw view after the session is terminated?\nOptions:",
        "options": [
            "A. Query the users_vw using Spark",
            "B. Persist the users_vw data as a table",
            "C. Recreate the users_vw and query the data using Spark",
            "D. Save the users_vw definition and query using Spark"
        ],
        "answer": [
            "B"
        ],
        "explanation": "Temp views like createOrReplaceTempView are session-scoped.\nThey disappear once the Spark session ends.\nTo retain data across sessions, it must be persisted:\ndf.write.saveAsTable(\"users_vw\")\nThus, the view needs to be persisted as a table to survive session termination."
    },
    {
        "question": "A Spark developer is developing a Spark application to monitor task performance across a\n\n\ncluster.\nOne requirement is to track the maximum processing time for tasks on each worker node and\nconsolidate this information on the driver for further analysis.\nWhich technique should the developer use?",
        "options": [
            "A. Broadcast a variable to share the maximum time among workers.",
            "B. Configure the Spark UI to automatically collect maximum times.",
            "C. Use an RDD action like reduce() to compute the maximum time.",
            "D. Use an accumulator to record the maximum time on the driver."
        ],
        "answer": [
            "C"
        ],
        "explanation": "RDD actions like reduce() aggregate values across all partitions and return the result to the\ndriver.\nTo compute the maximum processing time, reduce() is ideal because it combines results\nfrom all tasks efficiently.\nExample:\nmax_time = rdd_times.reduce(lambda x, y: max(x, y))\nThis aggregates maximum values from all executors into a single result on the driver.\nWhy the other options are incorrect:\nA: Broadcast variables distribute read-only data; they cannot aggregate results.\nB: Spark UI provides visualization, not programmatic collection.\nD: Accumulators support additive operations only (e.g., counters, sums), not non-associative\nones like max.\nReference:\nSpark RDD API - reduce() for aggregations.\nDatabricks Exam Guide (June 2025): Section \"Apache Spark Architecture and Components\"\n- actions, accumulators, and broadcast variables."
    },
    {
        "question": "A developer wants to refactor older Spark code to take advantage of built-in functions\nintroduced in Spark 3.5.\nThe original code:\nfrom pyspark.sql import functions as F\nmin_price = 110.50\nresult_df = prices_df.filter(F.col(\"price\") > min_price).agg(F.count(\"*\")) Which code block\nshould the developer use to refactor the code?",
        "options": [
            "A. result_df = prices_df.filter(F.col(\"price\") > F.lit(min_price)).agg(F.count(\"*\"))",
            "B. result_df = prices_df.where(F.lit(\"price\") > min_price).groupBy().count()",
            "C. result_df = prices_df.withColumn(\"valid_price\", when(col(\"price\") > F.lit(min_price), True))",
            "D. result_df = prices_df.filter(F.lit(min_price) > F.col(\"price\")).count()"
        ],
        "answer": [
            "A"
        ],
        "explanation": "To compare a column value with a Python literal constant in a DataFrame expression, use\nF.lit() to convert it into a Spark literal.\n\n\nCorrect refactor:\nfrom pyspark.sql import functions as F\nmin_price = 110.50\nresult_df = prices_df.filter(F.col(\"price\") > F.lit(min_price)).agg(F.count(\"*\")) This avoids type\nmismatches and ensures Spark executes the filter expression on the cluster.\nWhy the other options are incorrect:\nB: where() syntax is valid, but F.lit(\"price\") is incorrect - wraps string literal, not a column.\nC: withColumn adds a column, not needed for this aggregation.\nD: Comparison logic reversed.\nReference:\nPySpark SQL Functions - lit(), col(), and DataFrame filters.\nDatabricks Exam Guide (June 2025): Section \"Developing Apache Spark\nDataFrame/DataSet API Applications\" - filtering, literals, and aggregations."
    },
    {
        "question": "A Data Analyst is working on the DataFrame sensor_df, which contains two columns:\nWhich code fragment returns a DataFrame that splits the record column into separate\ncolumns and has one array item per row?",
        "options": [
            "A. exploded_df = sensor_df.withColumn(\"record_exploded\", explode(\"record\")) exploded_df =\nexploded_df.select(\"record_datetime\", \"sensor_id\", \"status\", \"health\")",
            "B. exploded_df = exploded_df.select(\n\"record_datetime\",\n\"record_exploded.sensor_id\",\n\"record_exploded.status\",\n\"record_exploded.health\"\n)\nexploded_df = sensor_df.withColumn(\"record_exploded\", explode(\"record\"))",
            "C. exploded_df = sensor_df.withColumn(\"record_exploded\", explode(\"record\"))\nexploded_df = exploded_df.select(\n\"record_datetime\",\n\"record_exploded.sensor_id\",\n\"record_exploded.status\",\n\"record_exploded.health\"\n)",
            "D. exploded_df = exploded_df.select(\"record_datetime\", \"record_exploded\")"
        ],
        "answer": [
            "C"
        ],
        "explanation": "To flatten an array of structs into individual rows and access fields within each struct, you\nmust:\nUse explode() to expand the array so each struct becomes its own row.\nAccess the struct fields via dot notation (e.g., record_exploded.sensor_id).\nOption C does exactly that:\nFirst, explode the record array column into a new column record_exploded.\nThen, access fields of the struct using the dot syntax in select.\nThis is standard practice in PySpark for nested data transformation.\nFinal answer: C"
    },
    {
        "question": "Given:\nspark.sparkContext.setLogLevel(\"<LOG_LEVEL>\")\nWhich set contains the suitable configuration settings for Spark driver LOG_LEVELs?",
        "options": [
            "A. ALL, DEBUG, FAIL, INFO",
            "B. ERROR, WARN, TRACE, OFF",
            "C. WARN, NONE, ERROR, FATAL",
            "D. FATAL, NONE, INFO, DEBUG"
        ],
        "answer": [
            "B"
        ],
        "explanation": "The setLogLevel() method of SparkContext sets the logging level on the driver, which\ncontrols the verbosity of logs emitted during job execution. Supported levels are inherited\nfrom log4j and include the following:\nALL\nDEBUG\nERROR\nFATAL\nINFO\nOFF\nTRACE\nWARN\nAccording to official Spark and Databricks documentation:\n\"Valid log levels include: ALL, DEBUG, ERROR, FATAL, INFO, OFF, TRACE, and WARN.\"\n\n\nAmong the choices provided, only option B (ERROR, WARN, TRACE, OFF) includes four\nvalid log levels and excludes invalid ones like \"FAIL\" or \"NONE\"."
    },
    {
        "question": "The following code fragment results in an error:\n@F.udf(T.IntegerType())\ndef simple_udf(t: str) -> str:\nreturn answer * 3.14159\nWhich code fragment should be used instead?",
        "options": [
            "A. @F.udf(T.IntegerType())\ndef simple_udf(t: int) -> int:\nreturn t * 3.14159",
            "B. @F.udf(T.DoubleType())\ndef simple_udf(t: float) -> float:\nreturn t * 3.14159",
            "C. @F.udf(T.DoubleType())\ndef simple_udf(t: int) -> int:\nreturn t * 3.14159",
            "D. @F.udf(T.IntegerType())\ndef simple_udf(t: float) -> float:\nreturn t * 3.14159"
        ],
        "answer": [
            "B"
        ],
        "explanation": "The original code has several issues:\nIt references a variable answer that is undefined.\nThe function is annotated to return a str, but the logic attempts numeric multiplication.\nThe UDF return type is declared as T.IntegerType() but the function performs a floating-point\noperation, which is incompatible.\nOption B correctly:\nUses DoubleType to reflect the fact that the multiplication involves a float (3.14159).\nDeclares the input as float, which aligns with the multiplication.\nReturns a float, which matches both the logic and the schema type annotation.\nThis structure aligns with how PySpark expects User Defined Functions (UDFs) to be\ndeclared:\n\"To define a UDF you must specify a Python function and provide the return type using the\nrelevant Spark SQL type (e.g., DoubleType for float results).\" Example from official\ndocumentation:\nfrom pyspark.sql.functions import udf\nfrom pyspark.sql.types import DoubleType\n@udf(returnType=DoubleType())\ndef multiply_by_pi(x: float) -> float:\n\n\nreturn x * 3.14159\nThis makes Option B the syntactically and semantically correct choice."
    },
    {
        "question": "A data engineer is investigating a Spark cluster that is experiencing underutilization during\nscheduled batch jobs.\nAfter checking the Spark logs, they noticed that tasks are often getting killed due to timeout\nerrors, and there are several warnings about insufficient resources in the logs.\nWhich action should the engineer take to resolve the underutilization issue?",
        "options": [
            "A. Set the spark.network.timeout property to allow tasks more time to complete without being\nkilled.",
            "B. Increase the executor memory allocation in the Spark configuration.",
            "C. Reduce the size of the data partitions to improve task scheduling.",
            "D. Increase the number of executor instances to handle more concurrent tasks."
        ],
        "answer": [
            "D"
        ],
        "explanation": "Underutilization with timeout warnings often indicates insufficient parallelism - meaning there\naren't enough executors to process all tasks concurrently.\nSolution:\nIncrease the number of executors to allow more parallel task execution and better resource\nutilization.\nExample configuration:\n--conf spark.executor.instances=8\nThis distributes the workload more effectively across cluster nodes and reduces idle time for\npending tasks.\nWhy the other options are incorrect:\nA: Extending timeouts hides the symptom, not the root cause (lack of executors).\nB: More memory per executor won't fix scheduling bottlenecks.\nC: Reducing partition size may increase overhead and does not fix resource imbalance.\nReference:\nDatabricks Exam Guide (June 2025): Section \"Troubleshooting and Tuning Apache Spark\nDataFrame API Applications\" - tuning executors and cluster utilization.\nSpark Configuration - executor instances and resource scaling."
    },
    {
        "question": "A Spark developer wants to improve the performance of an existing PySpark UDF that runs a\nhash function not available in the standard Spark functions library.\nThe existing UDF code is:\nimport hashlib\nfrom pyspark.sql.types import StringType\ndef shake_256(raw):\nreturn hashlib.shake_256(raw.encode()).hexdigest(20)\nshake_256_udf = udf(shake_256, StringType())\n\n\nThe developer replaces this UDF with a Pandas UDF for better performance:\n@pandas_udf(StringType())\ndef shake_256(raw: str) -> str:\nreturn hashlib.shake_256(raw.encode()).hexdigest(20)\nHowever, the developer receives this error:\nTypeError: Unsupported signature: (raw: str) -> str\nWhat should the signature of the shake_256() function be changed to in order to fix this\nerror?",
        "options": [
            "A. def shake_256(raw: str) -> str:",
            "B. def shake_256(raw: [pd.Series]) -> pd.Series:",
            "C. def shake_256(raw: pd.Series) -> pd.Series:",
            "D. def shake_256(raw: [str]) -> [str]:"
        ],
        "answer": [
            "C"
        ],
        "explanation": "Pandas UDFs (vectorized UDFs) process entire Pandas Series objects, not scalar values.\nEach invocation operates on a column (Series) rather than a single value.\nCorrect syntax:\n@pandas_udf(StringType())\ndef shake_256(raw: pd.Series) -> pd.Series:\nreturn raw.apply(lambda x: hashlib.shake_256(x.encode()).hexdigest(20)) This allows Spark\nto apply the function in a vectorized way, improving performance significantly over traditional\nPython UDFs.\nWhy the other options are incorrect:\nA/D: These define scalar functions - not compatible with Pandas UDFs.\nB: Uses an invalid type hint [pd.Series] (not a valid Python type annotation).\nReference:\nPySpark Pandas API - @pandas_udf decorator and function signatures.\nDatabricks Exam Guide (June 2025): Section \"Using Pandas API on Apache Spark\" - creating\nand invoking Pandas UDFs."
    },
    {
        "question": "A Spark developer is building an app to monitor task performance. They need to track the\nmaximum task processing time per worker node and consolidate it on the driver for analysis.\nWhich technique should be used?",
        "options": [
            "A. Use an RDD action like reduce() to compute the maximum time",
            "B. Use an accumulator to record the maximum time on the driver",
            "C. Broadcast a variable to share the maximum time among workers",
            "D. Configure the Spark UI to automatically collect maximum times"
        ],
        "answer": [
            "A"
        ],
        "explanation": "The correct way to aggregate information (e.g., max value) from distributed workers back to\nthe driver is using RDD actions such as reduce() or aggregate().\nFrom the documentation:\n\"To perform global aggregations on distributed data, actions like reduce() are commonly used\nto collect summaries such as min/max/avg.\" Accumulators (Option B) do not support max\noperations directly and are not intended for such analytics.\nBroadcast (Option C) is used to send data to workers, not collect from them.\nSpark UI (Option D) is a monitoring tool - not an analytics collection interface.\nFinal answer: A"
    },
    {
        "question": "A data engineer is working on a real-time analytics pipeline using Apache Spark Structured Streaming. The engineer wants to process incoming data and ensure that triggers control when the query is executed. The system needs to process data in micro-batches with a fixed interval of 5 seconds.\n\nWhich code snippet the data engineer could use to fulfil this requirement?\n\nA)\nquery = df.writeStream \\\n    .outputMode(\"append\") \\\n    .trigger(continuous='5 seconds') \\\n    .start()\n\nB)\nquery = df.writeStream \\\n    .outputMode(\"append\") \\\n    .trigger() \\\n    .start()\n\nC)\nquery = df.writeStream \\\n    .outputMode(\"append\") \\\n    .trigger(processingTime='5 seconds') \\\n    .start()\n\nD)\nquery = df.writeStream \\\n    .outputMode(\"append\") \\\n    .trigger(processingTime=5000) \\\n    .start()",
        "options": [
            "A. Uses trigger(continuous='5 seconds') - continuous processing mode.",
            "B. Uses trigger() - default micro-batch trigger without interval.",
            "C. Uses trigger(processingTime='5 seconds') - correct micro-batch trigger with interval.",
            "D. Uses trigger(processingTime=5000) - invalid, as processingTime expects a string."
        ],
        "answer": [
            "C"
        ],
        "explanation": "To define a micro-batch interval, the correct syntax is:\nquery = df.writeStream \\\n.outputMode(\"append\") \\\n.trigger(processingTime='5 seconds') \\\n.start()\nThis schedules the query to execute every 5 seconds.\nContinuous mode (used in Option A) is experimental and has limited sink support.\nOption D is incorrect because processingTime must be a string (not an integer).\nOption B triggers as fast as possible without interval control."
    },
    {
        "question": "A data scientist is working on a project that requires processing large amounts of structured\ndata, performing SQL queries, and applying machine learning algorithms. The data scientist\nis considering using Apache Spark for this task.\nWhich combination of Apache Spark modules should the data scientist use in this scenario?\nOptions:",
        "options": [
            "A. Spark DataFrames, Structured Streaming, and GraphX",
            "B. Spark SQL, Pandas API on Spark, and Structured Streaming",
            "C. Spark Streaming, GraphX, and Pandas API on Spark",
            "D. Spark DataFrames, Spark SQL, and MLlib"
        ],
        "answer": [
            "D"
        ],
        "explanation": "Comprehensive\nTo cover structured data processing, SQL querying, and machine learning in Apache Spark,\nthe correct combination of components is:\nSpark DataFrames: for structured data processing\nSpark SQL: to execute SQL queries over structured data\nMLlib: Spark's scalable machine learning library\nThis trio is designed for exactly this type of use case.\nWhy other options are incorrect:\n\n\nA: GraphX is for graph processing - not needed here.\nB: Pandas API on Spark is useful, but MLlib is essential for ML, which this option omits.\nC: Spark Streaming is legacy; GraphX is irrelevant here."
    },
    {
        "question": "A data engineer is working on a real-time analytics pipeline using Spark Structured\nStreaming.\nThey want the system to process incoming data in micro-batches at a fixed interval of 5\nseconds.\nWhich code snippet fulfills this requirement?",
        "options": [
            "A. query = df.writeStream \\\n.outputMode(\"append\") \\\n.trigger(processingTime=\"5 seconds\") \\\n.start()",
            "B. query = df.writeStream \\\n.outputMode(\"append\") \\\n.trigger(continuous=\"5 seconds\") \\\n.start()",
            "C. query = df.writeStream \\\n.outputMode(\"append\") \\\n.trigger(once=True) \\\n.start()",
            "D. query = df.writeStream \\\n.outputMode(\"append\") \\\n.start()"
        ],
        "answer": [
            "A"
        ],
        "explanation": "To process data in fixed micro-batch intervals, use the .trigger(processingTime=\"interval\")\noption in Structured Streaming.\nCorrect usage:\nquery = df.writeStream \\\n.outputMode(\"append\") \\\n.trigger(processingTime=\"5 seconds\") \\\n.start()\nThis instructs Spark to process available data every 5 seconds.\nWhy the other options are incorrect:\nB: continuous triggers are for continuous processing mode (different execution model).\nC: once=True runs the stream a single time (batch mode).\nD: Default trigger runs as fast as possible, not fixed intervals.\nReference:\n\n\nPySpark Structured Streaming Guide - Trigger types: processingTime, once, continuous.\nDatabricks Exam Guide (June 2025): Section \"Structured Streaming\" - controlling streaming\ntriggers and batch intervals."
    },
    {
        "question": "A data engineer is working with a large JSON dataset containing order information. The\ndataset is stored in a distributed file system and needs to be loaded into a Spark DataFrame\nfor analysis. The data engineer wants to ensure that the schema is correctly defined and that\nthe data is read efficiently.\nWhich approach should the data scientist use to efficiently load the JSON data into a Spark\nDataFrame with a predefined schema?",
        "options": [
            "A. Use spark.read.json() to load the data, then use DataFrame.printSchema() to view the\ninferred schema, and finally use DataFrame.cast() to modify column types.",
            "B. Use spark.read.json() with the inferSchema option set to true",
            "C. Use spark.read.format(\"json\").load() and then use DataFrame.withColumn() to cast each\ncolumn to the desired data type.",
            "D. Define a StructType schema and use spark.read.schema(predefinedSchema).json() to\nload the data."
        ],
        "answer": [
            "D"
        ],
        "explanation": "The most efficient and correct approach is to define a schema using StructType and pass it\nto spark.read.schema(...).\nThis avoids schema inference overhead and ensures proper data types are enforced during\nread.\nExample:\nfrom pyspark.sql.types import StructType, StructField, StringType, DoubleType schema =\nStructType([ StructField(\"order_id\", StringType(), True), StructField(\"amount\", DoubleType(),\nTrue),\n...\n])\ndf = spark.read.schema(schema).json(\"path/to/json\")\n- Source: Databricks Guide - Read JSON with predefined schema"
    },
    {
        "question": "What is a feature of Spark Connect?",
        "options": [
            "A. It supports DataStreamReader, DataStreamWriter, StreamingQuery, and Streaming APIs",
            "B. Supports DataFrame, Functions, Column, SparkContext PySpark APIs",
            "C. It supports only PySpark applications",
            "D. It has built-in authentication"
        ],
        "answer": [
            "A"
        ],
        "explanation": "Spark Connect is a client-server architecture introduced in Apache Spark 3.4, designed to\ndecouple the client from the Spark driver, enabling remote connectivity to Spark clusters.\nAccording to the Spark 3.5.5 documentation:\n\"Majority of the Streaming API is supported, including DataStreamReader, DataStreamWriter,\nStreamingQuery and StreamingQueryListener.\" This indicates that Spark Connect supports\nkey components of Structured Streaming, allowing for robust streaming data processing\ncapabilities.\nRegarding other options:\nB . While Spark Connect supports DataFrame, Functions, and Column APIs, it does not\nsupport SparkContext and RDD APIs.\nC . Spark Connect supports multiple languages, including PySpark and Scala, not just\nPySpark.\nD . Spark Connect does not have built-in authentication but is designed to work seamlessly\nwith existing authentication infrastructures."
    },
    {
        "question": "Which configuration can be enabled to optimize the conversion between Pandas and\nPySpark DataFrames using Apache Arrow?",
        "options": [
            "A. spark.conf.set(\"spark.pandas.arrow.enabled\", \"true\")",
            "B. spark.conf.set(\"spark.sql.execution.arrow.pyspark.enabled\", \"true\")",
            "C. spark.conf.set(\"spark.sql.execution.arrow.enabled\", \"true\")",
            "D. spark.conf.set(\"spark.sql.arrow.pandas.enabled\", \"true\")"
        ],
        "answer": [
            "B"
        ],
        "explanation": "Apache Arrow is used under the hood to optimize conversion between Pandas and PySpark\nDataFrames. The correct configuration setting is:\nspark.conf.set(\"spark.sql.execution.arrow.pyspark.enabled\", \"true\")\nFrom the official documentation:\n\"This configuration must be enabled to allow for vectorized execution and efficient conversion\nbetween Pandas and PySpark using Arrow.\" Option B is correct.\nOptions A, C, and D are invalid config keys and not recognized by Spark.\nFinal answer: B"
    },
    {
        "question": "A data engineer has been asked to produce a Parquet table which is overwritten every day\nwith the latest data. The downstream consumer of this Parquet table has a hard requirement\nthat the data in this table is produced with all records sorted by the market_time field.\nWhich line of Spark code will produce a Parquet table that meets these requirements?",
        "options": [
            "A. final_df \\\n.sort(\"market_time\") \\\n.write \\\n.format(\"parquet\") \\\n.mode(\"overwrite\") \\\n.saveAsTable(\"output.market_events\")",
            "B. final_df \\\n.orderBy(\"market_time\") \\\n.write \\\n.format(\"parquet\") \\\n.mode(\"overwrite\") \\\n.saveAsTable(\"output.market_events\")",
            "C. final_df \\\n.sort(\"market_time\") \\\n.coalesce(1) \\\n.write \\\n.format(\"parquet\") \\\n.mode(\"overwrite\") \\\n.saveAsTable(\"output.market_events\")",
            "D. final_df \\\n.sortWithinPartitions(\"market_time\") \\\n.write \\\n.format(\"parquet\") \\\n.mode(\"overwrite\") \\\n.saveAsTable(\"output.market_events\")"
        ],
        "answer": [
            "D"
        ],
        "explanation": "To ensure that data written out to disk is sorted, it is important to consider how Spark writes\ndata when saving to Parquet tables. The methods .sort() or .orderBy() apply a global sort but\ndo not guarantee that the sorting will persist in the final output files unless certain conditions\nare met (e.g. a single partition via .coalesce(1) - which is not scalable).\nInstead, the proper method in distributed Spark processing to ensure rows are sorted within\ntheir respective partitions when written out is:\n.sortWithinPartitions(\"column_name\")\nAccording to Apache Spark documentation:\n\"sortWithinPartitions() ensures each partition is sorted by the specified columns. This is\nuseful for downstream systems that require sorted files.\" This method works efficiently in\ndistributed settings, avoids the performance bottleneck of global sorting (as in .orderBy() or\n.sort()), and guarantees each output partition has sorted records - which meets the\nrequirement of consistently sorted data.\nThus:\nOption A and B do not guarantee the persisted file contents are sorted.\nOption C introduces a bottleneck via .coalesce(1) (single partition).\nOption D correctly applies sorting within partitions and is scalable."
    },
    {
        "question": "Which of the following code blocks stores a part of the data in DataFrame itemsDf on\nexecutors?",
        "options": [
            "A. itemsDf.cache().count()",
            "B. itemsDf.cache(eager=True)",
            "C. cache(itemsDf)",
            "D. itemsDf.cache().filter()",
            "E. itemsDf.rdd.storeCopy()"
        ],
        "answer": [
            "A"
        ],
        "explanation": "Caching means storing a copy of a partition on an executor, so it can be accessed quicker by\nsubsequent operations, instead of having to be recalculated. cache() is a lazily-evaluated method of\nthe DataFrame. Since count() is an action (while filter() is not), it triggers the caching process.\nMore info: pyspark.sql.DataFrame.cache - PySpark 3.1.2 documentation, Learning Spark, 2nd Edition,\nChapter 7 Static notebook | Dynamic notebook: See test 2"
    },
    {
        "question": "The code block shown below should return only the average prediction error (column\npredError) of a random subset, without replacement, of approximately 15% of rows in DataFrame\ntransactionsDf. Choose the answer that correctly fills the blanks in the code block to accomplish this.\ntransactionsDf.__1__(__2__, __3__).__4__(avg('predError'))",
        "options": [
            "A. 1. sample\n2. True\n3. 0.15\n4. filter",
            "B. 1. sample\n2. False\n3. 0.15\n4. select",
            "C. 1. sample\n2. 0.85\n3. False\n4. select",
            "D. 1. fraction\n2. 0.15\n3. True\n4. where",
            "E. 1. fraction\n2. False\n3. 0.85\n4. select"
        ],
        "answer": [
            "B"
        ],
        "explanation": "Correct code block:\ntransactionsDf.sample(withReplacement=False, fraction=0.15).select(avg('predError')) You should\nremember that getting a random subset of rows means sampling. This, in turn should point you to\nthe DataFrame.sample() method. Once you know this, you can look up the correct order of\n\n\narguments in the documentation (link below).\nLastly, you have to decide whether to use filter, where or select. where is just an alias for filter().\nfilter() is not the correct method to use here, since it would only allow you to filter rows based on\nsome condition. However, the question asks to return only the average prediction error. You can\ncontrol the columns that a query returns with the select() method - so this is the correct method to\nuse here.\nMore info: pyspark.sql.DataFrame.sample - PySpark 3.1.2 documentation\nStatic notebook | Dynamic notebook: See test 2"
    },
    {
        "question": "In which order should the code blocks shown below be run in order to return the number of\nrecords that are not empty in column value in the DataFrame resulting from an inner join of\nDataFrame transactionsDf and itemsDf on columns productId and itemId, respectively?\n1. .filter(~isnull(col('value')))\n2. .count()\n3. transactionsDf.join(itemsDf, col(\"transactionsDf.productId\")==col(\"itemsDf.itemId\"))\n4. transactionsDf.join(itemsDf, transactionsDf.productId==itemsDf.itemId, how='inner')\n5. .filter(col('value').isnotnull())\n6. .sum(col('value'))",
        "options": [
            "A. 4, 1, 2",
            "B. 3, 1, 6",
            "C. 3, 1, 2",
            "D. 3, 5, 2",
            "E. 4, 6"
        ],
        "answer": [
            "A"
        ],
        "explanation": "Correct code block:\ntransactionsDf.join(itemsDf, transactionsDf.productId==itemsDf.itemId,\nhow='inner').filter(~isnull(col('value'))).count()\nExpressions col(\"transactionsDf.productId\") and col(\"itemsDf.itemId\") are invalid. col() does not\naccept the name of a DataFrame, only column names.\nStatic notebook | Dynamic notebook: See test 2"
    },
    {
        "question": "Which of the following statements about lazy evaluation is incorrect?",
        "options": [
            "A. Predicate pushdown is a feature resulting from lazy evaluation.",
            "B. Execution is triggered by transformations.",
            "C. Spark will fail a job only during execution, but not during definition.",
            "D. Accumulators do not change the lazy evaluation model of Spark.",
            "E. Lineages allow Spark to coalesce transformations into stages"
        ],
        "answer": [
            "B"
        ],
        "explanation": "Execution is triggered by transformations.\nCorrect. Execution is triggered by actions only, not by transformations.\nLineages allow Spark to coalesce transformations into stages.\nIncorrect. In Spark, lineage means a recording of transformations. This lineage enables lazy\nevaluation in Spark.\n\n\nPredicate pushdown is a feature resulting from lazy evaluation.\nWrong. Predicate pushdown means that, for example, Spark will execute filters as early in the\nprocess as possible so that it deals with the least possible amount of data in subsequent\ntransformations, resulting in a performance improvements.\nAccumulators do not change the lazy evaluation model of Spark.\nIncorrect. In Spark, accumulators are only updated when the query that refers to the is actually\nexecuted. In other words, they are not updated if the query is not (yet) executed due to lazy\nevaluation.\nSpark will fail a job only during execution, but not during definition.\nWrong. During definition, due to lazy evaluation, the job is not executed and thus certain errors, for\nexample reading from a non-existing file, cannot be caught. To be caught, the job needs to be\nexecuted, for example through an action."
    },
    {
        "question": "Which of the following code blocks returns about 150 randomly selected rows from the 1000-\nrow DataFrame transactionsDf, assuming that any row can appear more than once in the returned\nDataFrame?",
        "options": [
            "A. transactionsDf.resample(0.15, False, 3142)",
            "B. transactionsDf.sample(0.15, False, 3142)",
            "C. transactionsDf.sample(0.15)",
            "D. transactionsDf.sample(0.85, 8429)",
            "E. transactionsDf.sample(True, 0.15, 8261)"
        ],
        "answer": [
            "E"
        ],
        "explanation": "Answering this question correctly depends on whether you understand the arguments to the\nDataFrame.sample() method (link to the documentation below). The arguments are as follows:\nDataFrame.sample(withReplacement=None, fraction=None, seed=None).\nThe first argument withReplacement specified whether a row can be drawn from the DataFrame\nmultiple times. By default, this option is disabled in Spark. But we have to enable it here, since the\nquestion asks for a row being able to appear more than once. So, we need to pass True for this\nargument.\nAbout replacement: \"Replacement\" is easiest explained with the example of removing random items\nfrom a box. When you remove those \"with replacement\" it means that after you have taken an item\nout of the box, you put it back inside. So, essentially, if you would randomly take 10 items out of a\nbox with 100 items, there is a chance you take the same item twice or more times. \"Without\nreplacement\" means that you would not put the item back into the box after removing it. So, every\ntime you remove an item from the box, there is one less item in the box and you can never take the\nsame item twice.\nThe second argument to the withReplacement method is fraction. This referes to the fraction of\nitems that should be returned. In the question we are asked for 150 out of 1000 items - a fraction of\n0.15.\nThe last argument is a random seed. A random seed makes a randomized processed repeatable. This\nmeans that if you would re-run the same sample() operation with the same random seed, you would\nget the same rows returned from the sample() command. There is no behavior around the random\nseed specified in the question. The varying random seeds are only there to confuse you!\nMore info: pyspark.sql.DataFrame.sample - PySpark 3.1.1 documentation\nStatic notebook | Dynamic notebook: See test 1"
    },
    {
        "question": "The code block shown below should return a DataFrame with two columns, itemId and col. In\nthis DataFrame, for each element in column attributes of DataFrame itemDf there should be a\nseparate row in which the column itemId contains the associated itemId from DataFrame itemsDf.\nThe new DataFrame should only contain rows for rows in DataFrame itemsDf in which the column\nattributes contains the element cozy.\nA sample of DataFrame itemsDf is below.\nCode block:\nitemsDf.__1__(__2__).__3__(__4__, __5__(__6__))",
        "options": [
            "A. 1. filter\n2. array_contains(\"cozy\")\n3. select\n4. \"itemId\"\n5. explode\n6. \"attributes\"",
            "B. 1. where\n2. \"array_contains(attributes, 'cozy')\"\n3. select\n4. itemId\n5. explode\n6. attributes",
            "C. 1. filter\n2. \"array_contains(attributes, 'cozy')\"\n3. select\n4. \"itemId\"\n5. map\n6. \"attributes\"",
            "D. 1. filter\n2. \"array_contains(attributes, cozy)\"\n3. select\n4. \"itemId\"\n5. explode\n6. \"attributes\"",
            "E. 1. filter\n2. \"array_contains(attributes, 'cozy')\"\n3. select\n4. \"itemId\"\n5. explode\n6. \"attributes\""
        ],
        "answer": [
            "E"
        ],
        "explanation": "The correct code block is:\nitemsDf.filter(\"array_contains(attributes, 'cozy')\").select(\"itemId\", explode(\"attributes\")) The key\nhere is understanding how to use array_contains(). You can either use it as an expression in a string,\nor you can import it from pyspark.sql.functions. In that case, the following would also work:\n\n\nitemsDf.filter(array_contains(\"attributes\", \"cozy\")).select(\"itemId\", explode(\"attributes\")) Static\nnotebook | Dynamic notebook: See test 1\n(https://flrs.github.io/spark_practice_tests_code/#1/29.html ,\nhttps://bit.ly/sparkpracticeexams_import_instructions)"
    },
    {
        "question": "Which of the following describes the characteristics of accumulators?",
        "options": [
            "A. Accumulators are used to pass around lookup tables across the cluster.",
            "B. All accumulators used in a Spark application are listed in the Spark UI.",
            "C. Accumulators can be instantiated directly via the accumulator(n) method of the pyspark.RDD\nmodule.",
            "D. Accumulators are immutable.",
            "E. If an action including an accumulator fails during execution and Spark manages to restart the\naction and complete it successfully, only the successful attempt will be counted in the accumulator."
        ],
        "answer": [
            "E"
        ],
        "explanation": "If an action including an accumulator fails during execution and Spark manages to restart the action\nand complete it successfully, only the successful attempt will be counted in the accumulator.\nCorrect, when Spark tries to rerun a failed action that includes an accumulator, it will only update the\naccumulator if the action succeeded.\nAccumulators are immutable.\nNo. Although accumulators behave like write-only variables towards the executors and can only be\nread by the driver, they are not immutable.\nAll accumulators used in a Spark application are listed in the Spark UI.\nIncorrect. For scala, only named, but not unnamed, accumulators are listed in the Spark UI. For\npySpark, no accumulators are listed in the Spark UI - this feature is not yet implemented.\nAccumulators are used to pass around lookup tables across the cluster.\nWrong - this is what broadcast variables do.\nAccumulators can be instantiated directly via the accumulator(n) method of the pyspark.RDD\nmodule.\nWrong, accumulators are instantiated via the accumulator(n) method of the sparkContext, for\nexample: counter\n= spark.sparkContext.accumulator(0).\nMore info: python - In Spark, RDDs are immutable, then how Accumulators are implemented? - Stac\nk Overflow, apache spark - When are accumulators truly reliable? - Stack Overflow, Spark - T\nhe Definitive Guide, Chapter 14"
    },
    {
        "question": "Which of the following code blocks removes all rows in the 6-column DataFrame\ntransactionsDf that have missing data in at least 3 columns?",
        "options": [
            "A. transactionsDf.dropna(\"any\")",
            "B. transactionsDf.dropna(thresh=4)",
            "C. transactionsDf.drop.na(\"\",2)",
            "D. transactionsDf.dropna(thresh=2)",
            "E. transactionsDf.dropna(\"\",4)"
        ],
        "answer": [
            "B"
        ],
        "explanation": "transactionsDf.dropna(thresh=4)\nCorrect. Note that by only working with the thresh keyword argument, the first how keyword\nargument is ignored. Also, figuring out which value to set for thresh can be difficult, especially when\nunder pressure in the exam. Here, I recommend you use the notes to create a \"simulation\" of what\ndifferent values for thresh would do to a DataFrame. Here is an explanatory image why thresh=4 is\nthe correct answer to the question:\ntransactionsDf.dropna(thresh=2)\nAlmost right. See the comment about thresh for the correct answer above.\ntransactionsDf.dropna(\"any\")\nNo, this would remove all rows that have at least one missing value.\ntransactionsDf.drop.na(\"\",2)\nNo, drop.na is not a proper DataFrame method.\ntransactionsDf.dropna(\"\",4)\nNo, this does not work and will throw an error in Spark because Spark cannot understand the first\nargument.\nMore info: pyspark.sql.DataFrame.dropna - PySpark 3.1.1 documentation (https://bit.ly/2QZpiCp)\nStatic notebook | Dynamic notebook: See test 1\n(https://flrs.github.io/spark_practice_tests_code/#1/20.html ,\nhttps://bit.ly/sparkpracticeexams_import_instructions)"
    },
    {
        "question": "The code block displayed below contains an error. The code block should create DataFrame\nitemsAttributesDf which has columns itemId and attribute and lists every attribute from the\nattributes column in DataFrame itemsDf next to the itemId of the respective row in itemsDf. Find the\nerror.\nA sample of DataFrame itemsDf is below.\n \nCode block:\nitemsAttributesDf = itemsDf.explode(\"attributes\").alias(\"attribute\").select(\"attribute\", \"itemId\")",
        "options": [
            "A. Since itemId is the index, it does not need to be an argument to the select() method.",
            "B. The alias() method needs to be called after the select() method.",
            "C. The explode() method expects a Column object rather than a string.",
            "D. explode() is not a method of DataFrame. explode() should be used inside the select() method\ninstead.",
            "E. The split() method should be used inside the select() method instead of the explode() method."
        ],
        "answer": [
            "D"
        ],
        "explanation": "The correct code block looks like this:\n\n\nThen, the first couple of rows of itemAttributesDf look like this:\n \nexplode() is not a method of DataFrame. explode() should be used inside the select() method instead.\nThis is correct.\nThe split() method should be used inside the select() method instead of the explode() method.\nNo, the split() method is used to split strings into parts. However, column attributs is an array of\nstrings. In this case, the explode() method is appropriate.\nSince itemId is the index, it does not need to be an argument to the select() method.\nNo, itemId still needs to be selected, whether it is used as an index or not.\nThe explode() method expects a Column object rather than a string.\nNo, a string works just fine here. This being said, there are some valid alternatives to passing in a\nstring:\nThe alias() method needs to be called after the select() method.\nNo.\nMore info: pyspark.sql.functions.explode - PySpark 3.1.1 documentation (https://bit.ly/2QUZI1J)\nStatic notebook | Dynamic notebook: See test 1\n(https://flrs.github.io/spark_practice_tests_code/#1/22.html ,\nhttps://bit.ly/sparkpracticeexams_import_instructions)"
    },
    {
        "question": "The code block shown below should set the number of partitions that Spark uses when\nshuffling data for joins or aggregations to 100. Choose the answer that correctly fills the blanks in the\ncode block to accomplish this.\nspark.sql.shuffle.partitions\n__1__.__2__.__3__(__4__, 100)",
        "options": [
            "A. 1. spark\n2. conf\n3. set\n4. \"spark.sql.shuffle.partitions\"",
            "B. 1. pyspark\n2. config\n3. set\n4. spark.shuffle.partitions",
            "C. 1. spark\n2. conf\n3. get\n4. \"spark.sql.shuffle.partitions\"",
            "D. 1. pyspark\n2. config\n3. set\n4. \"spark.sql.shuffle.partitions\"",
            "E. 1. spark\n2. conf\n3. set\n4. \"spark.sql.aggregate.partitions\""
        ],
        "answer": [
            "A"
        ],
        "explanation": "Correct code block:\nspark.conf.set(\"spark.sql.shuffle.partitions\", 100)\nThe conf interface is part of the SparkSession, so you need to call it through spark and not pyspark.\nTo configure spark, you need to use the set method, not the get method. get reads a property, but\ndoes not write it. The correct property to achieve what is outlined in the question is\nspark.sql.aggregate.partitions, which needs to be passed to set as a string. Properties\nspark.shuffle.partitions and spark.sql.aggregate.partitions do not exist in Spark.\nStatic notebook | Dynamic notebook: See test 2"
    },
    {
        "question": "Which of the following statements about the differences between actions and\ntransformations is correct?",
        "options": [
            "A. Actions are evaluated lazily, while transformations are not evaluated lazily.",
            "B. Actions generate RDDs, while transformations do not.",
            "C. Actions do not send results to the driver, while transformations do.",
            "D. Actions can be queued for delayed execution, while transformations can only be processed\nimmediately.",
            "E. Actions can trigger Adaptive Query Execution, while transformation cannot."
        ],
        "answer": [
            "E"
        ],
        "explanation": "Actions can trigger Adaptive Query Execution, while transformation cannot.\nCorrect. Adaptive Query Execution optimizes queries at runtime. Since transformations are evaluated\nlazily, Spark does not have any runtime information to optimize the query until an action is called. If\nAdaptive Query Execution is enabled, Spark will then try to optimize the query based on the feedback\nit gathers while it is evaluating the query.\nActions can be queued for delayed execution, while transformations can only be processed\nimmediately.\nNo, there is no such concept as \"delayed execution\" in Spark. Actions cannot be evaluated lazily,\nmeaning that they are executed immediately.\nActions are evaluated lazily, while transformations are not evaluated lazily.\nIncorrect, it is the other way around: Transformations are evaluated lazily and actions trigger their\nevaluation.\nActions generate RDDs, while transformations do not.\nNo. Transformations change the data and, since RDDs are immutable, generate new RDDs along the\nway.\nActions produce outputs in Python and data types (integers, lists, text files,...) based on the RDDs, but\nthey do not generate them.\nHere is a great tip on how to differentiate actions from transformations: If an operation returns a\nDataFrame, Dataset, or an RDD, it is a transformation. Otherwise, it is an action.\nActions do not send results to the driver, while transformations do.\nNo. Actions send results to the driver. Think about running DataFrame.count(). The result of this\ncommand will return a number to the driver. Transformations, however, do not send results back to\nthe driver. They produce RDDs that remain on the worker nodes.\nMore info: What is the difference between a transformation and an action in Apache Spark? |\nBartosz Mikulski, How to Speed up SQL Queries with Adaptive Query Execution"
    },
    {
        "question": "Which of the following code blocks reads in the two-partition parquet file stored at filePath,\nmaking sure all columns are included exactly once even though each partition has a different\nschema?\nSchema of first partition:\n1.root\n2. |-- transactionId: integer (nullable = true)\n3. |-- predError: integer (nullable = true)\n4. |-- value: integer (nullable = true)\n5. |-- storeId: integer (nullable = true)\n6. |-- productId: integer (nullable = true)\n7. |-- f: integer (nullable = true)\nSchema of second partition:\n1.root\n2. |-- transactionId: integer (nullable = true)\n3. |-- predError: integer (nullable = true)\n4. |-- value: integer (nullable = true)\n5. |-- storeId: integer (nullable = true)\n6. |-- rollId: integer (nullable = true)\n7. |-- f: integer (nullable = true)\n\n\n8. |-- tax_id: integer (nullable = false)",
        "options": [
            "A. spark.read.parquet(filePath, mergeSchema='y')",
            "B. spark.read.option(\"mergeSchema\", \"true\").parquet(filePath)",
            "C. spark.read.parquet(filePath)",
            "D. 1.nx = 0\n2.for file in dbutils.fs.ls(filePath):\n3. if not file.name.endswith(\".parquet\"):\n4. continue\n5. df_temp = spark.read.parquet(file.path)\n6. if nx == 0:\n7. df = df_temp\n8. else:\n9. df = df.union(df_temp)\n10. nx = nx+1\n11.df",
            "E. 1.nx = 0\n2.for file in dbutils.fs.ls(filePath):\n3. if not file.name.endswith(\".parquet\"):\n4. continue\n5. df_temp = spark.read.parquet(file.path)\n6. if nx == 0:\n7. df = df_temp\n8. else:\n9. df = df.join(df_temp, how=\"outer\")\n10. nx = nx+1\n11.df"
        ],
        "answer": [
            "B"
        ],
        "explanation": "This is a very tricky question and involves both knowledge about merging as well as schemas when\nreading parquet files.\nspark.read.option(\"mergeSchema\", \"true\").parquet(filePath)\nCorrect. Spark's DataFrameReader's mergeSchema option will work well here, since columns that\nappear in both partitions have matching data types. Note that mergeSchema would fail if one or\nmore columns with the same name that appear in both partitions would have different data types.\nspark.read.parquet(filePath)\nIncorrect. While this would read in data from both partitions, only the schema in the parquet file that\nis read in first would be considered, so some columns that appear only in the second partition (e.g.\ntax_id) would be lost.\nnx = 0\nfor file in dbutils.fs.ls(filePath):\nif not file.name.endswith(\".parquet\"):\ncontinue\ndf_temp = spark.read.parquet(file.path)\nif nx == 0:\ndf = df_temp\nelse:\n\n\ndf = df.union(df_temp)\nnx = nx+1\ndf\nWrong. The key idea of this solution is the DataFrame.union() command. While this command\nmerges all data, it requires that both partitions have the exact same number of columns with\nidentical data types.\nspark.read.parquet(filePath, mergeSchema=\"y\")\nFalse. While using the mergeSchema option is the correct way to solve this problem and it can even\nbe called with DataFrameReader.parquet() as in the code block, it accepts the value True as a\nboolean or string variable. But 'y' is not a valid option.\nnx = 0\nfor file in dbutils.fs.ls(filePath):\nif not file.name.endswith(\".parquet\"):\ncontinue\ndf_temp = spark.read.parquet(file.path)\nif nx == 0:\ndf = df_temp\nelse:\ndf = df.join(df_temp, how=\"outer\")\nnx = nx+1\ndf\nNo. This provokes a full outer join. While the resulting DataFrame will have all columns of both\npartitions, columns that appear in both partitions will be duplicated - the question says all columns\nthat are included in the partitions should appear exactly once.\nMore info: Merging different schemas in Apache Spark | by Thiago Cordon | Data Arena | Medium\nStatic notebook | Dynamic notebook: See test 3"
    },
    {
        "question": "The code block shown below should return a copy of DataFrame transactionsDf without\ncolumns value and productId and with an additional column associateId that has the value 5. Choose\nthe answer that correctly fills the blanks in the code block to accomplish this.\ntransactionsDf.__1__(__2__, __3__).__4__(__5__, 'value')",
        "options": [
            "A. 1. withColumn\n2. 'associateId'\n3. 5\n4. remove\n5. 'productId'",
            "B. 1. withNewColumn\n2. associateId\n3. lit(5)\n4. drop\n5. productId",
            "C. 1. withColumn\n2. 'associateId'\n3. lit(5)\n4. drop\n5. 'productId'",
            "D. 1. withColumnRenamed\n2. 'associateId'\n3. 5\n4. drop\n5. 'productId'",
            "E. 1. withColumn\n2. col(associateId)\n3. lit(5)\n4. drop\n5. col(productId)"
        ],
        "answer": [
            "C"
        ],
        "explanation": "Correct code block:\ntransactionsDf.withColumn('associateId', lit(5)).drop('productId', 'value') For solving this question it is\nimportant that you know the lit() function (link to documentation below). This function enables you\nto add a column of a constant value to a DataFrame.\nMore info: pyspark.sql.functions.lit - PySpark 3.1.1 documentation\nStatic notebook | Dynamic notebook: See test 1"
    },
    {
        "question": "The code block shown below should return an exact copy of DataFrame transactionsDf that\ndoes not include rows in which values in column storeId have the value 25. Choose the answer that\ncorrectly fills the blanks in the code block to accomplish this.",
        "options": [
            "A. transactionsDf.remove(transactionsDf.storeId==25)",
            "B. transactionsDf.where(transactionsDf.storeId!=25)",
            "C. transactionsDf.filter(transactionsDf.storeId==25)",
            "D. transactionsDf.drop(transactionsDf.storeId==25)",
            "E. transactionsDf.select(transactionsDf.storeId!=25)"
        ],
        "answer": [
            "B"
        ],
        "explanation": "transactionsDf.where(transactionsDf.storeId!=25)\nCorrect. DataFrame.where() is an alias for the DataFrame.filter() method. Using this method, it is\nstraightforward to filter out rows that do not have value 25 in column storeId.\ntransactionsDf.select(transactionsDf.storeId!=25)\nWrong. The select operator allows you to build DataFrames column-wise, but when using it as shown,\nit does not filter out rows.\ntransactionsDf.filter(transactionsDf.storeId==25)\nIncorrect. Although the filter expression works for filtering rows, the == in the filtering condition is\ninappropriate. It should be != instead.\ntransactionsDf.drop(transactionsDf.storeId==25)\nNo. DataFrame.drop() is used to remove specific columns, but not rows, from the DataFrame.\ntransactionsDf.remove(transactionsDf.storeId==25)\nFalse. There is no DataFrame.remove() operator in PySpark.\nMore info: pyspark.sql.DataFrame.where - PySpark 3.1.2 documentation\nStatic notebook | Dynamic notebook: See test 3"
    },
    {
        "question": "Which of the following statements about stages is correct?",
        "options": [
            "A. Different stages in a job may be executed in parallel.",
            "B. Stages consist of one or more jobs.",
            "C. Stages ephemerally store transactions, before they are committed through actions.",
            "D. Tasks in a stage may be executed by multiple machines at the same time.",
            "E. Stages may contain multiple actions, narrow, and wide transformations."
        ],
        "answer": [
            "D"
        ],
        "explanation": "Tasks in a stage may be executed by multiple machines at the same time.\nThis is correct. Within a single stage, tasks do not depend on each other. Executors on multiple\nmachines may execute tasks belonging to the same stage on the respective partitions they are\nholding at the same time.\nDifferent stages in a job may be executed in parallel.\nNo. Different stages in a job depend on each other and cannot be executed in parallel. The nuance is\nthat every task in a stage may be executed in parallel by multiple machines.\nFor example, if a job consists of Stage A and Stage B, tasks belonging to those stages may not be\nexecuted in parallel. However, tasks from Stage A may be executed on multiple machines at the same\ntime, with each machine running it on a different partition of the same dataset. Then, afterwards,\ntasks from Stage B may be executed on multiple machines at the same time.\nStages may contain multiple actions, narrow, and wide transformations.\nNo, stages may not contain multiple wide transformations. Wide transformations mean that shuffling\nis required. Shuffling typically terminates a stage though, because data needs to be exchanged across\nthe cluster. This data exchange often causes partitions to change and rearrange, making it impossible\nto perform tasks in parallel on the same dataset.\nStages ephemerally store transactions, before they are committed through actions.\nNo, this does not make sense. Stages do not \"store\" any data. Transactions are not \"committed\" in\nSpark.\nStages consist of one or more jobs.\nNo, it is the other way around: Jobs consist of one more stages.\nMore info: Spark: The Definitive Guide, Chapter 15."
    },
    {
        "question": "Which of the following statements about broadcast variables is correct?",
        "options": [
            "A. Broadcast variables are serialized with every single task.",
            "B. Broadcast variables are commonly used for tables that do not fit into memory.",
            "C. Broadcast variables are immutable.",
            "D. Broadcast variables are occasionally dynamically updated on a per-task basis.",
            "E. Broadcast variables are local to the worker node and not shared across the cluster."
        ],
        "answer": [
            "C"
        ],
        "explanation": "Broadcast variables are local to the worker node and not shared across the cluster.\nThis is wrong because broadcast variables are meant to be shared across the cluster. As such, they\nare never just local to the worker node, but available to all worker nodes.\nBroadcast variables are commonly used for tables that do not fit into memory.\nThis is wrong because broadcast variables can only be broadcast because they are small and do fit\ninto memory.\n\n\nBroadcast variables are serialized with every single task.\nThis is wrong because they are cached on every machine in the cluster, precisely avoiding to have to\nbe serialized with every single task.\nBroadcast variables are occasionally dynamically updated on a per-task basis.\nThis is wrong because broadcast variables are immutable - they are never updated.\nMore info: Spark - The Definitive Guide, Chapter 14"
    },
    {
        "question": "Which of the following describes Spark's Adaptive Query Execution?",
        "options": [
            "A. Adaptive Query Execution features include dynamically coalescing shuffle partitions, dynamically\ninjecting scan filters, and dynamically optimizing skew joins.",
            "B. Adaptive Query Execution is enabled in Spark by default.",
            "C. Adaptive Query Execution reoptimizes queries at execution points.",
            "D. Adaptive Query Execution features are dynamically switching join strategies and dynamically\noptimizing skew joins.",
            "E. Adaptive Query Execution applies to all kinds of queries."
        ],
        "answer": [
            "D"
        ],
        "explanation": "Adaptive Query Execution features include dynamically coalescing shuffle partitions, dynamically\ninjecting scan filters, and dynamically optimizing skew joins.\nThis is almost correct. All of these features, except for dynamically injecting scan filters, are part of\nAdaptive Query Execution. Dynamically injecting scan filters for join operations to limit the amount of\ndata to be considered in a query is part of Dynamic Partition Pruning and not of Adaptive Query\nExecution.\nAdaptive Query Execution reoptimizes queries at execution points.\nNo, Adaptive Query Execution reoptimizes queries at materialization points.\nAdaptive Query Execution is enabled in Spark by default.\nNo, Adaptive Query Execution is disabled in Spark needs to be enabled through the\nspark.sql.adaptive.enabled property.\nAdaptive Query Execution applies to all kinds of queries.\nNo, Adaptive Query Execution applies only to queries that are not streaming queries and that contain\nat least one exchange (typically expressed through a join, aggregate, or window operator) or one\nsubquery.\nMore info: How to Speed up SQL Queries with Adaptive Query Execution, Learning Spark, 2nd Edition,\nChapter 12 (https://bit.ly/3tOh8M1)"
    },
    {
        "question": "The code block shown below should store DataFrame transactionsDf on two different\nexecutors, utilizing the executors' memory as much as possible, but not writing anything to disk.\nChoose the answer that correctly fills the blanks in the code block to accomplish this.\n1.from pyspark import StorageLevel\n2.transactionsDf.__1__(StorageLevel.__2__).__3__",
        "options": [
            "A. 1. cache\n2. MEMORY_ONLY_2\n3. count()",
            "B. 1. persist\n2. DISK_ONLY_2\n\n\n3. count()",
            "C. 1. persist\n2. MEMORY_ONLY_2\n3. select()",
            "D. 1. cache\n2. DISK_ONLY_2\n3. count()",
            "E. 1. persist\n2. MEMORY_ONLY_2\n3. count()"
        ],
        "answer": [
            "E"
        ],
        "explanation": "Correct code block:\nfrom pyspark import StorageLevel\ntransactionsDf.persist(StorageLevel.MEMORY_ONLY_2).count()\nOnly persist takes different storage levels, so any option using cache() cannot be correct. persist() is\nevaluated lazily, so an action needs to follow this command. select() is not an action, but count() is -\nso all options using select() are incorrect.\nFinally, the question states that \"the executors' memory should be utilized as much as possible, but\nnot writing anything to disk\". This points to a MEMORY_ONLY storage level. In this storage level,\npartitions that do not fit into memory will be recomputed when they are needed, instead of being\nwritten to disk, as with the storage option MEMORY_AND_DISK. Since the data need to be duplicated\nacross two executors, _2 needs to be appended to the storage level.\nStatic notebook | Dynamic notebook: See test 2"
    },
    {
        "question": "Which of the following statements about storage levels is incorrect?",
        "options": [
            "A. The cache operator on DataFrames is evaluated like a transformation.",
            "B. In client mode, DataFrames cached with the MEMORY_ONLY_2 level will not be stored in the edge\nnode's memory.",
            "C. Caching can be undone using the DataFrame.unpersist() operator.",
            "D. MEMORY_AND_DISK replicates cached DataFrames both on memory and disk.",
            "E. DISK_ONLY will not use the worker node's memory."
        ],
        "answer": [
            "D"
        ],
        "explanation": "MEMORY_AND_DISK replicates cached DataFrames both on memory and disk.\nCorrect, this statement is wrong. Spark prioritizes storage in memory, and will only store data on disk\nthat does not fit into memory.\nDISK_ONLY will not use the worker node's memory.\nWrong, this statement is correct. DISK_ONLY keeps data only on the worker node's disk, but not in\nmemory.\nIn client mode, DataFrames cached with the MEMORY_ONLY_2 level will not be stored in the edge\nnode's memory.\nWrong, this statement is correct. In fact, Spark does not have a provision to cache DataFrames in the\ndriver (which sits on the edge node in client mode). Spark caches DataFrames in the executors'\nmemory.\n\n\nCaching can be undone using the DataFrame.unpersist() operator.\nWrong, this statement is correct. Caching, as achieved via the DataFrame.cache() or\nDataFrame.persist() operators can be undone using the DataFrame.unpersist() operator. This\noperator will remove all of its parts from the executors' memory and disk.\nThe cache operator on DataFrames is evaluated like a transformation.\nWrong, this statement is correct. DataFrame.cache() is evaluated like a transformation: Through lazy\nevaluation. This means that after calling DataFrame.cache() the command will not have any effect\nuntil you call a subsequent action, like DataFrame.cache().count().\nMore info: pyspark.sql.DataFrame.unpersist - PySpark 3.1.2 documentation"
    },
    {
        "question": "Which of the following code blocks produces the following output, given DataFrame\ntransactionsDf?\nOutput:\n1.root\n2. |-- transactionId: integer (nullable = true)\n3. |-- predError: integer (nullable = true)\n4. |-- value: integer (nullable = true)\n5. |-- storeId: integer (nullable = true)\n6. |-- productId: integer (nullable = true)\n7. |-- f: integer (nullable = true)\nDataFrame transactionsDf:\n1.+-------------+---------+-----+-------+---------+----+\n2.|transactionId|predError|value|storeId|productId| f|\n3.+-------------+---------+-----+-------+---------+----+\n4.| 1| 3| 4| 25| 1|null|\n5.| 2| 6| 7| 2| 2|null|\n6.| 3| 3| null| 25| 3|null|\n7.+-------------+---------+-----+-------+---------+----+",
        "options": [
            "A. transactionsDf.schema.print()",
            "B. transactionsDf.rdd.printSchema()",
            "C. transactionsDf.rdd.formatSchema()",
            "D. transactionsDf.printSchema()",
            "E. print(transactionsDf.schema)"
        ],
        "answer": [
            "D"
        ],
        "explanation": "The output is the typical output of a DataFrame.printSchema() call. The DataFrame's RDD\nrepresentation does not have a printSchema or formatSchema method (find available methods in the\nRDD documentation linked below). The output of print(transactionsDf.schema) is this:\nStructType(List(StructField(transactionId,IntegerType,true),StructField(predError,IntegerType,true),St\nructField\n(value,IntegerType,true),StructField(storeId,IntegerType,true),StructField(productId,IntegerType,true\n),StructFiel It includes the same information as the nicely formatted original output, but is not nicely\nformatted itself. Lastly, the DataFrame's schema attribute does not have a print() method.\nMore info:\n- pyspark.RDD: pyspark.RDD - PySpark 3.1.2 documentation\n- DataFrame.printSchema(): pyspark.sql.DataFrame.printSchema - PySpark 3.1.2 documentation Stati\n\n\nc notebook | Dynamic notebook: See test 2"
    },
    {
        "question": "The code block displayed below contains one or more errors. The code block should load\nparquet files at location filePath into a DataFrame, only loading those files that have been modified\nbefore\n2029-03-20 05:44:46. Spark should enforce a schema according to the schema shown below. Find the\nerror.\nSchema:\n1.root\n2. |-- itemId: integer (nullable = true)\n3. |-- attributes: array (nullable = true)\n4. | |-- element: string (containsNull = true)\n5. |-- supplier: string (nullable = true)\nCode block:\n1.schema = StructType([\n2. StructType(\"itemId\", IntegerType(), True),\n3. StructType(\"attributes\", ArrayType(StringType(), True), True),\n4. StructType(\"supplier\", StringType(), True)\n5.])\n6.\n7.spark.read.options(\"modifiedBefore\", \"2029-03-20T05:44:46\").schema(schema).load(filePath)",
        "options": [
            "A. The attributes array is specified incorrectly, Spark cannot identify the file format, and the syntax of\nthe call to Spark's DataFrameReader is incorrect.",
            "B. Columns in the schema definition use the wrong object type and the syntax of the call to Spark's\nDataFrameReader is incorrect.",
            "C. The data type of the schema is incompatible with the schema() operator and the modification date\nthreshold is specified incorrectly.",
            "D. Columns in the schema definition use the wrong object type, the modification date threshold is\nspecified incorrectly, and Spark cannot identify the file format.",
            "E. Columns in the schema are unable to handle empty values and the modification date threshold is\nspecified incorrectly."
        ],
        "answer": [
            "D"
        ],
        "explanation": "Correct code block:\nschema = StructType([\nStructField(\"itemId\", IntegerType(), True),\nStructField(\"attributes\", ArrayType(StringType(), True), True),\nStructField(\"supplier\", StringType(), True)\n])\nspark.read.options(modifiedBefore=\"2029-03-20T05:44:46\").schema(schema).parquet(filePath) This\nquestion is more difficult than what you would encounter in the exam. In the exam, for this question\ntype, only one error needs to be identified and not \"one or multiple\" as in the question.\nColumns in the schema definition use the wrong object type, the modification date threshold is\nspecified incorrectly, and Spark cannot identify the file format.\nCorrect! Columns in the schema definition should use the StructField type. Building a schema from\n\n\npyspark.sql.types, as here using classes like StructType and StructField, is one of multiple ways of\nexpressing a schema in Spark. A StructType always contains a list of StructFields (see documentation\nlinked below). So, nesting StructType and StructType as shown in the question is wrong.\nThe modification date threshold should be specified by a keyword argument like\noptions(modifiedBefore=\"2029-03-20T05:44:46\") and not two consecutive non-keyword arguments\nas in the original code block (see documentation linked below).\nSpark cannot identify the file format correctly, because either it has to be specified by using the\nDataFrameReader.format(), as an argument to DataFrameReader.load(), or directly by calling, for\nexample, DataFrameReader.parquet().\nColumns in the schema are unable to handle empty values and the modification date threshold is\nspecified incorrectly.\nNo. If StructField would be used for the columns instead of StructType (see above), the third\nargument specified whether the column is nullable. The original schema shows that columns should\nbe nullable and this is specified correctly by the third argument being True in the schema in the code\nblock.\nIt is correct, however, that the modification date threshold is specified incorrectly (see above).\nThe attributes array is specified incorrectly, Spark cannot identify the file format, and the syntax of\nthe call to Spark's DataFrameReader is incorrect.\nWrong. The attributes array is specified correctly, following the syntax for ArrayType (see linked\ndocumentation below). That Spark cannot identify the file format is correct, see correct answer\nabove. In addition, the DataFrameReader is called correctly through the SparkSession spark.\nColumns in the schema definition use the wrong object type and the syntax of the call to Spark's\nDataFrameReader is incorrect.\nIncorrect, the object types in the schema definition are correct and syntax of the call to Spark's\nDataFrameReader is correct.\nThe data type of the schema is incompatible with the schema() operator and the modification date\nthreshold is specified incorrectly.\nFalse. The data type of the schema is StructType and an accepted data type for the\nDataFrameReader.schema() method. It is correct however that the modification date threshold is\nspecified incorrectly (see correct answer above)."
    },
    {
        "question": "Which of the following statements about Spark's execution hierarchy is correct?",
        "options": [
            "A. In Spark's execution hierarchy, a job may reach over multiple stage boundaries.",
            "B. In Spark's execution hierarchy, manifests are one layer above jobs.",
            "C. In Spark's execution hierarchy, a stage comprises multiple jobs.",
            "D. In Spark's execution hierarchy, executors are the smallest unit.",
            "E. In Spark's execution hierarchy, tasks are one layer above slots."
        ],
        "answer": [
            "A"
        ],
        "explanation": "In Spark's execution hierarchy, a job may reach over multiple stage boundaries.\nCorrect. A job is a sequence of stages, and thus may reach over multiple stage boundaries.\nIn Spark's execution hierarchy, tasks are one layer above slots.\nIncorrect. Slots are not a part of the execution hierarchy. Tasks are the lowest layer.\nIn Spark's execution hierarchy, a stage comprises multiple jobs.\nNo. It is the other way around - a job consists of one or multiple stages.\nIn Spark's execution hierarchy, executors are the smallest unit.\n\n\nFalse. Executors are not a part of the execution hierarchy. Tasks are the smallest unit!\nIn Spark's execution hierarchy, manifests are one layer above jobs.\nWrong. Manifests are not a part of the Spark ecosystem."
    },
    {
        "question": "The code block displayed below contains multiple errors. The code block should remove\ncolumn transactionDate from DataFrame transactionsDf and add a column transactionTimestamp in\nwhich dates that are expressed as strings in column transactionDate of DataFrame transactionsDf are\nconverted into unix timestamps. Find the errors.\nSample of DataFrame transactionsDf:\n1.+-------------+---------+-----+-------+---------+----+----------------+\n2.|transactionId|predError|value|storeId|productId| f| transactionDate|\n3.+-------------+---------+-----+-------+---------+----+----------------+\n4.| 1| 3| 4| 25| 1|null|2020-04-26 15:35|\n5.| 2| 6| 7| 2| 2|null|2020-04-13 22:01|\n6.| 3| 3| null| 25| 3|null|2020-04-02 10:53|\n7.+-------------+---------+-----+-------+---------+----+----------------+ Code block:\n1.transactionsDf = transactionsDf.drop(\"transactionDate\")\n2.transactionsDf[\"transactionTimestamp\"] = unix_timestamp(\"transactionDate\", \"yyyy-MM-dd\")",
        "options": [
            "A. Column transactionDate should be dropped after transactionTimestamp has been written. The\nstring indicating the date format should be adjusted. The withColumn operator should be used\ninstead of the existing column assignment. Operator to_unixtime() should be used instead of\nunix_timestamp().",
            "B. Column transactionDate should be dropped after transactionTimestamp has been written. The\nwithColumn operator should be used instead of the existing column assignment. Column\ntransactionDate should be wrapped in a col() operator.",
            "C. Column transactionDate should be wrapped in a col() operator.",
            "D. The string indicating the date format should be adjusted. The withColumnReplaced operator\nshould be used instead of the drop and assign pattern in the code block to replace column\ntransactionDate with the new column transactionTimestamp.",
            "E. Column transactionDate should be dropped after transactionTimestamp has been written. The\nstring indicating the date format should be adjusted. The withColumn operator should be used\ninstead of the existing column assignment."
        ],
        "answer": [
            "E"
        ],
        "explanation": "This question requires a lot of thinking to get right. For solving it, you may take advantage of the\ndigital notepad that is provided to you during the test. You have probably seen that the code block\nincludes multiple errors. In the test, you are usually confronted with a code block that only contains a\nsingle error. However, since you are practicing here, this challenging multi-error question will make it\neasier for you to deal with single-error questions in the real exam.\nYou can clearly see that column transactionDate should be dropped only after transactionTimestamp\nhas been written. This is because to generate column transactionTimestamp, Spark needs to read the\nvalues from column transactionDate.\nValues in column transactionDate in the original transactionsDf DataFrame look like 2020-04-26\n15:35. So, to convert those correctly, you would have to pass yyyy-MM-dd HH:mm. In other words:\nThe string indicating the date format should be adjusted.\n\n\nWhile you might be tempted to change unix_timestamp() to to_unixtime() (in line with the\nfrom_unixtime() operator), this function does not exist in Spark. unix_timestamp() is the correct\noperator to use here.\nAlso, there is no DataFrame.withColumnReplaced() operator. A similar operator that exists is\nDataFrame.withColumnRenamed().\nWhether you use col() or not is irrelevant with unix_timestamp() - the command is fine with both.\nFinally, you cannot assign a column like transactionsDf[\"columnName\"] = ... in Spark. This is Pandas\nsyntax (Pandas is a popular Python package for data analysis), but it is not supported in Spark.\nSo, you need to use Spark's DataFrame.withColumn() syntax instead.\nMore info: pyspark.sql.functions.unix_timestamp - PySpark 3.1.2 documentation Static notebook |\nDynamic notebook: See test 3"
    },
    {
        "question": "Which of the following code blocks returns a DataFrame showing the mean value of column\n\"value\" of DataFrame transactionsDf, grouped by its column storeId?",
        "options": [
            "A. transactionsDf.groupBy(col(storeId).avg())",
            "B. transactionsDf.groupBy(\"storeId\").avg(col(\"value\"))",
            "C. transactionsDf.groupBy(\"storeId\").agg(avg(\"value\"))",
            "D. transactionsDf.groupBy(\"storeId\").agg(average(\"value\"))",
            "E. transactionsDf.groupBy(\"value\").average()"
        ],
        "answer": [
            "C"
        ],
        "explanation": "This question tests your knowledge about how to use the groupBy and agg pattern in Spark. Using\nthe documentation, you can find out that there is no average() method in pyspark.sql.functions.\nStatic notebook | Dynamic notebook: See test 2"
    },
    {
        "question": "Which of the following code blocks returns a copy of DataFrame itemsDf where the column\nsupplier has been renamed to manufacturer?",
        "options": [
            "A. itemsDf.withColumn([\"supplier\", \"manufacturer\"])",
            "B. itemsDf.withColumn(\"supplier\").alias(\"manufacturer\")",
            "C. itemsDf.withColumnRenamed(\"supplier\", \"manufacturer\")",
            "D. itemsDf.withColumnRenamed(col(\"manufacturer\"), col(\"supplier\"))",
            "E. itemsDf.withColumnsRenamed(\"supplier\", \"manufacturer\")"
        ],
        "answer": [
            "C"
        ],
        "explanation": "itemsDf.withColumnRenamed(\"supplier\", \"manufacturer\")\nCorrect! This uses the relatively trivial DataFrame method withColumnRenamed for renaming column\nsupplier to column manufacturer.\nNote that the question asks for \"a copy of DataFrame itemsDf\". This may be confusing if you are not\nfamiliar with Spark yet. RDDs (Resilient Distributed Datasets) are the foundation of Spark DataFrames\nand are immutable. As such, DataFrames are immutable, too. Any command that changes anything in\nthe DataFrame therefore necessarily returns a copy, or a new version, of it that has the changes\napplied.\nitemsDf.withColumnsRenamed(\"supplier\", \"manufacturer\")\nIncorrect. Spark's DataFrame API does not have a withColumnsRenamed() method.\nitemsDf.withColumnRenamed(col(\"manufacturer\"), col(\"supplier\"))\n\n\nNo. Watch out - although the col() method works for many methods of the DataFrame API,\nwithColumnRenamed is not one of them. As outlined in the documentation linked below,\nwithColumnRenamed expects strings.\nitemsDf.withColumn([\"supplier\", \"manufacturer\"])\nWrong. While DataFrame.withColumn() exists in Spark, it has a different purpose than renaming\ncolumns.\nwithColumn is typically used to add columns to DataFrames, taking the name of the new column as a\nfirst, and a Column as a second argument. Learn more via the documentation that is linked below.\nitemsDf.withColumn(\"supplier\").alias(\"manufacturer\")\nNo. While DataFrame.withColumn() exists, it requires 2 arguments. Furthermore, the alias() method\non DataFrames would not help the cause of renaming a column much. DataFrame.alias() can be\nuseful in addressing the input of join statements. However, this is far outside of the scope of this\nquestion. If you are curious nevertheless, check out the link below.\nMore info: pyspark.sql.DataFrame.withColumnRenamed - PySpark 3.1.1 documentation,\npyspark.sql.DataFrame.withColumn - PySpark 3.1.1 documentation, and pyspark.sql.DataFrame.alias\n- PySpark 3.1.2 documentation (https://bit.ly/3aSB5tm , https://bit.ly/2Tv4rbE ,\nhttps://bit.ly/2RbhBd2) Static notebook | Dynamic notebook: See test 1\n(https://flrs.github.io/spark_practice_tests_code/#1/31.html ,\nhttps://bit.ly/sparkpracticeexams_import_instructions)"
    },
    {
        "question": "Which of the following code blocks stores DataFrame itemsDf in executor memory and, if\ninsufficient memory is available, serializes it and saves it to disk?",
        "options": [
            "A. itemsDf.persist(StorageLevel.MEMORY_ONLY)",
            "B. itemsDf.cache(StorageLevel.MEMORY_AND_DISK)",
            "C. itemsDf.store()",
            "D. itemsDf.cache()",
            "E. itemsDf.write.option('destination', 'memory').save()"
        ],
        "answer": [
            "D"
        ],
        "explanation": "The key to solving this question is knowing (or reading in the documentation) that, by default, cache()\nstores values to memory and writes any partitions for which there is insufficient memory to disk.\npersist() can achieve the exact same behavior, however not with the StorageLevel.MEMORY_ONLY\noption listed here. It is also worth noting that cache() does not have any arguments.\nIf you have troubles finding the storage level information in the documentation, please also see this\nstudent Q&A thread that sheds some light here.\nStatic notebook | Dynamic notebook: See test 2"
    },
    {
        "question": "Which of the following code blocks applies the boolean-returning Python function\nevaluateTestSuccess to column storeId of DataFrame transactionsDf as a user-defined function?",
        "options": [
            "A. 1.from pyspark.sql import types as T\n2.evaluateTestSuccessUDF = udf(evaluateTestSuccess, T.BooleanType())\n3.transactionsDf.withColumn(\"result\", evaluateTestSuccessUDF(col(\"storeId\")))",
            "B. 1.evaluateTestSuccessUDF = udf(evaluateTestSuccess)\n2.transactionsDf.withColumn(\"result\", evaluateTestSuccessUDF(storeId))",
            "C. 1.from pyspark.sql import types as T\n\n\n2.evaluateTestSuccessUDF = udf(evaluateTestSuccess, T.IntegerType())\n3.transactionsDf.withColumn(\"result\", evaluateTestSuccess(col(\"storeId\")))",
            "D. 1.evaluateTestSuccessUDF = udf(evaluateTestSuccess)\n2.transactionsDf.withColumn(\"result\", evaluateTestSuccessUDF(col(\"storeId\")))",
            "E. 1.from pyspark.sql import types as T\n2.evaluateTestSuccessUDF = udf(evaluateTestSuccess, T.BooleanType())\n3.transactionsDf.withColumn(\"result\", evaluateTestSuccess(col(\"storeId\")))"
        ],
        "answer": [
            "A"
        ],
        "explanation": "Recognizing that the UDF specification requires a return type (unless it is a string, which is the\ndefault) is important for solving this question. In addition, you should make sure that the generated\nUDF (evaluateTestSuccessUDF) and not the Python function (evaluateTestSuccess) is applied to\ncolumn storeId.\nMore info: pyspark.sql.functions.udf - PySpark 3.1.2 documentation\nStatic notebook | Dynamic notebook: See test 2"
    },
    {
        "question": "Which of the following code blocks reads in the JSON file stored at filePath, enforcing the\nschema expressed in JSON format in variable json_schema, shown in the code block below?\nCode block:\n1.json_schema = \"\"\"\n2.{\"type\": \"struct\",\n3. \"fields\": [\n4. {\n5. \"name\": \"itemId\",\n6. \"type\": \"integer\",\n7. \"nullable\": true,\n8. \"metadata\": {}\n9. },\n10. {\n11. \"name\": \"supplier\",\n12. \"type\": \"string\",\n13. \"nullable\": true,\n14. \"metadata\": {}\n15. }\n16. ]\n17.}\n18.\"\"\"",
        "options": [
            "A. spark.read.json(filePath, schema=json_schema)",
            "B. spark.read.schema(json_schema).json(filePath)\n1.schema = StructType.fromJson(json.loads(json_schema))\n2.spark.read.json(filePath, schema=schema)",
            "C. spark.read.json(filePath, schema=schema_of_json(json_schema))",
            "D. spark.read.json(filePath, schema=spark.read.json(json_schema))"
        ],
        "answer": [
            "B"
        ],
        "explanation": "Spark provides a way to digest JSON-formatted strings as schema. However, it is not trivial to use.\nAlthough slightly above exam difficulty, this question is beneficial to your exam preparation, since it\nhelps you to familiarize yourself with the concept of enforcing schemas on data you are reading in - a\ntopic within the scope of the exam.\nThe first answer that jumps out here is the one that uses spark.read.schema instead of\nspark.read.json. Looking at the documentation of spark.read.schema (linked below), we notice that\nthe operator expects types pyspark.sql.types.StructType or str as its first argument. While variable\njson_schema is a string, the documentation states that the str should be \"a DDL-formatted string (For\nexample col0 INT, col1 DOUBLE)\". Variable json_schema does not contain a string in this type of\nformat, so this answer option must be wrong.\nWith four potentially correct answers to go, we now look at the schema parameter of\nspark.read.json() (documentation linked below). Here, too, the schema parameter expects an input\nof type pyspark.sql.types.StructType or \"a DDL-formatted string (For example col0 INT, col1\nDOUBLE)\". We already know that json_schema does not follow this format, so we should focus on\nhow we can transform json_schema into pyspark.sql.types.StructType. Hereby, we also eliminate the\noption where schema=json_schema.\nThe option that includes schema=spark.read.json(json_schema) is also a wrong pick, since\nspark.read.json returns a DataFrame, and not a pyspark.sql.types.StructType type.\nRuling out the option which includes schema_of_json(json_schema) is rather difficult. The operator's\ndocumentation (linked below) states that it \"[p]arses a JSON string and infers its schema in DDL\nformat\". This use case is slightly different from the case at hand: json_schema already is a schema\ndefinition, it does not make sense to \"infer\" a schema from it. In the documentation you can see an\nexample use case which helps you understand the difference better. Here, you pass string '{a: 1}' to\nschema_of_json() and the method infers a DDL-format schema STRUCT<a: BIGINT> from it.\nIn our case, we may end up with the output schema of schema_of_json() describing the schema of\nthe JSON schema, instead of using the schema itself. This is not the right answer option.\nNow you may consider looking at the StructType.fromJson() method. It returns a variable of type\nStructType - exactly the type which the schema parameter of spark.read.json expects.\nAlthough we could have looked at the correct answer option earlier, this explanation is kept as\nexhaustive as necessary to teach you how to systematically eliminate wrong answer options.\nMore info:\n- pyspark.sql.DataFrameReader.schema - PySpark 3.1.2 documentation\n- pyspark.sql.DataFrameReader.json - PySpark 3.1.2 documentation\n- pyspark.sql.functions.schema_of_json - PySpark 3.1.2 documentation\nStatic notebook | Dynamic notebook: See test 3"
    },
    {
        "question": "Which of the following statements about garbage collection in Spark is incorrect?",
        "options": [
            "A. Garbage collection information can be accessed in the Spark UI's stage detail view.",
            "B. Optimizing garbage collection performance in Spark may limit caching ability.",
            "C. Manually persisting RDDs in Spark prevents them from being garbage collected.",
            "D. In Spark, using the G1 garbage collector is an alternative to using the default Parallel garbage\ncollector.",
            "E. Serialized caching is a strategy to increase the performance of garbage collection."
        ],
        "answer": [
            "C"
        ],
        "explanation": "Manually persisting RDDs in Spark prevents them from being garbage collected.\n\n\nThis statement is incorrect, and thus the correct answer to the question. Spark's garbage collector\nwill remove even persisted objects, albeit in an \"LRU\" fashion. LRU stands for least recently used.\nSo, during a garbage collection run, the objects that were used the longest time ago will be garbage\ncollected first.\nSee the linked StackOverflow post below for more information.\nSerialized caching is a strategy to increase the performance of garbage collection.\nThis statement is correct. The more Java objects Spark needs to collect during garbage collection, the\nlonger it takes. Storing a collection of many Java objects, such as a DataFrame with a complex\nschema, through serialization as a single byte array thus increases performance. This means that\ngarbage collection takes less time on a serialized DataFrame than an unserialized DataFrame.\nOptimizing garbage collection performance in Spark may limit caching ability.\nThis statement is correct. A full garbage collection run slows down a Spark application. When taking\nabout\n\"tuning\" garbage collection, we mean reducing the amount or duration of these slowdowns.\nA full garbage collection run is triggered when the Old generation of the Java heap space is almost\nfull. (If you are unfamiliar with this concept, check out the link to the Garbage Collection Tuning docs\nbelow.) Thus, one measure to avoid triggering a garbage collection run is to prevent the Old\ngeneration share of the heap space to be almost full.\nTo achieve this, one may decrease its size. Objects with sizes greater than the Old generation space\nwill then be discarded instead of cached (stored) in the space and helping it to be \"almost full\".\nThis will decrease the number of full garbage collection runs, increasing overall performance.\nInevitably, however, objects will need to be recomputed when they are needed. So, this mechanism\nonly works when a Spark application needs to reuse cached data as little as possible.\nGarbage collection information can be accessed in the Spark UI's stage detail view.\nThis statement is correct. The task table in the Spark UI's stage detail view has a \"GC Time\" column,\nindicating the garbage collection time needed per task.\nIn Spark, using the G1 garbage collector is an alternative to using the default Parallel garbage\ncollector.\nThis statement is correct. The G1 garbage collector, also known as garbage first garbage collector, is\nan alternative to the default Parallel garbage collector.\nWhile the default Parallel garbage collector divides the heap into a few static regions, the G1 garbage\ncollector divides the heap into many small regions that are created dynamically. The G1 garbage\ncollector has certain advantages over the Parallel garbage collector which improve performance\nparticularly for Spark workloads that require high throughput and low latency.\nThe G1 garbage collector is not enabled by default, and you need to explicitly pass an argument to\nSpark to enable it. For more information about the two garbage collectors, check out the Databricks\narticle linked below."
    },
    {
        "question": "Which of the following options describes the responsibility of the executors in Spark?",
        "options": [
            "A. The executors accept jobs from the driver, analyze those jobs, and return results to the driver.",
            "B. The executors accept tasks from the driver, execute those tasks, and return results to the cluster\nmanager.",
            "C. The executors accept tasks from the driver, execute those tasks, and return results to the driver.",
            "D. The executors accept tasks from the cluster manager, execute those tasks, and return results to\nthe driver.",
            "E. The executors accept jobs from the driver, plan those jobs, and return results to the cluster\n\n\nmanager."
        ],
        "answer": [
            "C"
        ],
        "explanation": "More info: Running Spark: an overview of Spark's runtime architecture - Manning\n(https://bit.ly/2RPmJn9)"
    },
    {
        "question": "Which of the following code blocks returns a single-column DataFrame of all entries in Python\nlist throughputRates which contains only float-type values ?",
        "options": [
            "A. spark.createDataFrame((throughputRates), FloatType)",
            "B. spark.createDataFrame(throughputRates, FloatType)",
            "C. spark.DataFrame(throughputRates, FloatType)",
            "D. spark.createDataFrame(throughputRates)",
            "E. spark.createDataFrame(throughputRates, FloatType())"
        ],
        "answer": [
            "E"
        ],
        "explanation": "spark.createDataFrame(throughputRates, FloatType())\nCorrect! spark.createDataFrame is the correct operator to use here and the type FloatType() which is\npassed in for the command's schema argument is correctly instantiated using the parentheses.\nRemember that it is essential in PySpark to instantiate types when passing them to\nSparkSession.createDataFrame. And, in Databricks, spark returns a SparkSession object.\nspark.createDataFrame((throughputRates), FloatType)\nNo. While packing throughputRates in parentheses does not do anything to the execution of this\ncommand, not instantiating the FloatType with parentheses as in the previous answer will make this\ncommand fail.\nspark.createDataFrame(throughputRates, FloatType)\nIncorrect. Given that it does not matter whether you pass throughputRates in parentheses or not, see\nthe explanation of the previous answer for further insights.\nspark.DataFrame(throughputRates, FloatType)\nWrong. There is no SparkSession.DataFrame() method in Spark.\nspark.createDataFrame(throughputRates)\nFalse. Avoiding the schema argument will have PySpark try to infer the schema. However, as you can\nsee in the documentation (linked below), the inference will only work if you pass in an \"RDD of either\nRow, namedtuple, or dict\" for data (the first argument to createDataFrame). But since you are\npassing a Python list, Spark's schema inference will fail.\nMore info: pyspark.sql.SparkSession.createDataFrame - PySpark 3.1.2 documentation Static notebook\n| Dynamic notebook: See test 3"
    },
    {
        "question": "Which of the following code blocks returns a one-column DataFrame for which every row\ncontains an array of all integer numbers from 0 up to and including the number given in column\npredError of DataFrame transactionsDf, and null if predError is null?\nSample of DataFrame transactionsDf:\n1.+-------------+---------+-----+-------+---------+----+\n2.|transactionId|predError|value|storeId|productId| f|\n3.+-------------+---------+-----+-------+---------+----+\n4.| 1| 3| 4| 25| 1|null|\n\n\n5.| 2| 6| 7| 2| 2|null|\n6.| 3| 3| null| 25| 3|null|\n7.| 4| null| null| 3| 2|null|\n8.| 5| null| null| null| 2|null|\n9.| 6| 3| 2| 25| 2|null|\n10.+-------------+---------+-----+-------+---------+----+",
        "options": [
            "A. 1.def count_to_target(target):\n2. if target is None:\n3. return\n4.\n5. result = [range(target)]\n6. return result\n7.\n8.count_to_target_udf = udf(count_to_target, ArrayType[IntegerType])\n9.\n10.transactionsDf.select(count_to_target_udf(col('predError')))",
            "B. 1.def count_to_target(target):\n2. if target is None:\n3. return\n4.\n5. result = list(range(target))\n6. return result\n7.\n8.transactionsDf.select(count_to_target(col('predError')))",
            "C. 1.def count_to_target(target):\n2. if target is None:\n3. return\n4.\n5. result = list(range(target))\n6. return result\n7.\n8.count_to_target_udf = udf(count_to_target, ArrayType(IntegerType()))\n9.\n10.transactionsDf.select(count_to_target_udf('predError'))",
            "D. 1.def count_to_target(target):\n2. result = list(range(target))\n3. return result\n4.\n5.count_to_target_udf = udf(count_to_target, ArrayType(IntegerType()))\n6.\n7.df = transactionsDf.select(count_to_target_udf('predError'))",
            "E. 1.def count_to_target(target):\n2. if target is None:\n3. return\n4.\n\n\n5. result = list(range(target))\n6. return result\n7.\n8.count_to_target_udf = udf(count_to_target)\n9.\n10.transactionsDf.select(count_to_target_udf('predError'))"
        ],
        "answer": [
            "C"
        ],
        "explanation": "Correct code block:\ndef count_to_target(target):\nif target is None:\nreturn\nresult = list(range(target))\nreturn result\ncount_to_target_udf = udf(count_to_target, ArrayType(IntegerType()))\ntransactionsDf.select(count_to_target_udf('predError'))\nOutput of correct code block:\n+--------------------------+\n|count_to_target(predError)|\n+--------------------------+\n| [0, 1, 2]|\n| [0, 1, 2, 3, 4, 5]|\n| [0, 1, 2]|\n| null|\n| null|\n| [0, 1, 2]|\n+--------------------------+\nThis question is not exactly easy. You need to be familiar with the syntax around UDFs (user-defined\nfunctions). Specifically, in this question it is important to pass the correct types to the udf method -\nreturning an array of a specific type rather than just a single type means you need to think harder\nabout type implications than usual.\nRemember that in Spark, you always pass types in an instantiated way like ArrayType(IntegerType()),\nnot like ArrayType(IntegerType). The parentheses () are the key here - make sure you do not forget\nthose.\nYou should also pay attention that you actually pass the UDF count_to_target_udf, and not the\nPython method count_to_target to the select() operator.\nFinally, null values are always a tricky case with UDFs. So, take care that the code can handle them\ncorrectly.\nMore info: How to Turn Python Functions into PySpark Functions (UDF) - Chang Hsin Lee - Committin\ng my thoughts to words.\nStatic notebook | Dynamic notebook: See test 3"
    },
    {
        "question": "Which of the following is not a feature of Adaptive Query Execution?",
        "options": [
            "A. Replace a sort merge join with a broadcast join, where appropriate.",
            "B. Coalesce partitions to accelerate data processing.",
            "C. Split skewed partitions into smaller partitions to avoid differences in partition processing time.",
            "D. Reroute a query in case of an executor failure.",
            "E. Collect runtime statistics during query execution."
        ],
        "answer": [
            "D"
        ],
        "explanation": "Reroute a query in case of an executor failure.\nCorrect. Although this feature exists in Spark, it is not a feature of Adaptive Query Execution. The\ncluster manager keeps track of executors and will work together with the driver to launch an\nexecutor and assign the workload of the failed executor to it (see also link below).\nReplace a sort merge join with a broadcast join, where appropriate.\nNo, this is a feature of Adaptive Query Execution.\nCoalesce partitions to accelerate data processing.\nWrong, Adaptive Query Execution does this.\nCollect runtime statistics during query execution.\nIncorrect, Adaptive Query Execution (AQE) collects these statistics to adjust query plans. This\nfeedback loop is an essential part of accelerating queries via AQE.\nSplit skewed partitions into smaller partitions to avoid differences in partition processing time.\nNo, this is indeed a feature of Adaptive Query Execution. Find more information in the Databricks\nblog post linked below.\nMore info: Learning Spark, 2nd Edition, Chapter 12, On which way does RDD of spark finish fault-\ntolerance?\n- Stack Overflow, How to Speed up SQL Queries with Adaptive Query Execution"
    },
    {
        "question": "The code block displayed below contains an error. The code block is intended to join\nDataFrame itemsDf with the larger DataFrame transactionsDf on column itemId. Find the error.\nCode block:\ntransactionsDf.join(itemsDf, \"itemId\", how=\"broadcast\")",
        "options": [
            "A. The syntax is wrong, how= should be removed from the code block.",
            "B. The join method should be replaced by the broadcast method.",
            "C. Spark will only perform the broadcast operation if this behavior has been enabled on the Spark\ncluster.",
            "D. The larger DataFrame transactionsDf is being broadcasted, rather than the smaller DataFrame\nitemsDf.",
            "E. broadcast is not a valid join type."
        ],
        "answer": [
            "E"
        ],
        "explanation": "broadcast is not a valid join type.\nCorrect! The code block should read transactionsDf.join(broadcast(itemsDf), \"itemId\"). This would\nimply an inner join (this is the default in DataFrame.join()), but since the join type is not given in the\nquestion, this would be a valid choice.\nThe larger DataFrame transactionsDf is being broadcasted, rather than the smaller DataFrame\nitemsDf.\nThis option does not apply here, since the syntax around broadcasting is incorrect.\nSpark will only perform the broadcast operation if this behavior has been enabled on the Spark\ncluster.\nNo, it is enabled by default, since the spark.sql.autoBroadcastJoinThreshold property is set to 10 MB\n\n\nby default. If that property would be set to -1, then broadcast joining would be disabled.\nMore info: Performance Tuning - Spark 3.1.1 Documentation (https://bit.ly/3gCz34r) The join method\nshould be replaced by the broadcast method.\nNo, DataFrame has no broadcast() method.\nThe syntax is wrong, how= should be removed from the code block.\nNo, having the keyword argument how= is totally acceptable."
    },
    {
        "question": "The code block shown below should return a one-column DataFrame where the column\nstoreId is converted to string type. Choose the answer that correctly fills the blanks in the code block\nto accomplish this.\ntransactionsDf.__1__(__2__.__3__(__4__))",
        "options": [
            "A. 1. select\n2. col(\"storeId\")\n3. cast\n4. StringType",
            "B. 1. select\n2. col(\"storeId\")\n3. as\n4. StringType",
            "C. 1. cast\n2. \"storeId\"\n3. as\n4. StringType()",
            "D. 1. select\n2. col(\"storeId\")\n3. cast\n4. StringType()",
            "E. 1. select\n2. storeId\n3. cast\n4. StringType()"
        ],
        "answer": [
            "D"
        ],
        "explanation": "Correct code block:\ntransactionsDf.select(col(\"storeId\").cast(StringType()))\nSolving this question involves understanding that, when using types from the pyspark.sql.types such\nas StringType, these types need to be instantiated when using them in Spark, or, in simple words,\nthey need to be followed by parentheses like so: StringType(). You could also use .cast(\"string\")\ninstead, but that option is not given here.\nMore info: pyspark.sql.Column.cast - PySpark 3.1.2 documentation\nStatic notebook | Dynamic notebook: See test 2"
    },
    {
        "question": "Which of the following code blocks returns the number of unique values in column storeId of\nDataFrame transactionsDf?",
        "options": [
            "A. transactionsDf.select(\"storeId\").dropDuplicates().count()",
            "B. transactionsDf.select(count(\"storeId\")).dropDuplicates()",
            "C. transactionsDf.select(distinct(\"storeId\")).count()",
            "D. transactionsDf.dropDuplicates().agg(count(\"storeId\"))",
            "E. transactionsDf.distinct().select(\"storeId\").count()"
        ],
        "answer": [
            "A"
        ],
        "explanation": "transactionsDf.select(\"storeId\").dropDuplicates().count()\nCorrect! After dropping all duplicates from column storeId, the remaining rows get counted,\nrepresenting the number of unique values in the column.\ntransactionsDf.select(count(\"storeId\")).dropDuplicates()\nNo. transactionsDf.select(count(\"storeId\")) just returns a single-row DataFrame showing the number\nof non-null rows. dropDuplicates() does not have any effect in this context.\ntransactionsDf.dropDuplicates().agg(count(\"storeId\"))\nIncorrect. While transactionsDf.dropDuplicates() removes duplicate rows from transactionsDf, it does\nnot do so taking only column storeId into consideration, but eliminates full row duplicates instead.\ntransactionsDf.distinct().select(\"storeId\").count()\nWrong. transactionsDf.distinct() identifies unique rows across all columns, but not only unique rows\nwith respect to column storeId. This may leave duplicate values in the column, making the count not\nrepresent the number of unique values in that column.\ntransactionsDf.select(distinct(\"storeId\")).count()\nFalse. There is no distinct method in pyspark.sql.functions."
    },
    {
        "question": "Which of the following statements about data skew is incorrect?",
        "options": [
            "A. Spark will not automatically optimize skew joins by default.",
            "B. Broadcast joins are a viable way to increase join performance for skewed data over sort-merge\njoins.",
            "C. In skewed DataFrames, the largest and the smallest partition consume very different amounts of\nmemory.",
            "D. To mitigate skew, Spark automatically disregards null values in keys when joining.",
            "E. Salting can resolve data skew."
        ],
        "answer": [
            "D"
        ],
        "explanation": "To mitigate skew, Spark automatically disregards null values in keys when joining.\nThis statement is incorrect, and thus the correct answer to the question. Joining keys that contain\nnull values is of particular concern with regard to data skew.\nIn real-world applications, a table may contain a great number of records that do not have a value\nassigned to the column used as a join key. During the join, the data is at risk of being heavily skewed.\nThis is because all records with a null-value join key are then evaluated as a single large partition,\nstanding in stark contrast to the potentially diverse key values (and therefore small partitions) of the\nnon-null-key records.\nSpark specifically does not handle this automatically. However, there are several strategies to\nmitigate this problem like discarding null values temporarily, only to merge them back later (see last\nlink below).\nIn skewed DataFrames, the largest and the smallest partition consume very different amounts of\nmemory.\n\n\nThis statement is correct. In fact, having very different partition sizes is the very definition of skew.\nSkew can degrade Spark performance because the largest partition occupies a single executor for a\nlong time. This blocks a Spark job and is an inefficient use of resources, since other executors that\nprocessed smaller partitions need to idle until the large partition is processed.\nSalting can resolve data skew.\nThis statement is correct. The purpose of salting is to provide Spark with an opportunity to repartition\ndata into partitions of similar size, based on a salted partitioning key.\nA salted partitioning key typically is a column that consists of uniformly distributed random numbers.\nThe number of unique entries in the partitioning key column should match the number of your\ndesired number of partitions. After repartitioning by the salted key, all partitions should have roughly\nthe same size.\nSpark does not automatically optimize skew joins by default.\nThis statement is correct. Automatic skew join optimization is a feature of Adaptive Query Execution\n(AQE).\nBy default, AQE is disabled in Spark. To enable it, Spark's spark.sql.adaptive.enabled configuration\noption needs to be set to true instead of leaving it at the default false.\nTo automatically optimize skew joins, Spark's spark.sql.adaptive.skewJoin.enabled options also needs\nto be set to true, which it is by default.\nWhen skew join optimization is enabled, Spark recognizes skew joins and optimizes them by splitting\nthe bigger partitions into smaller partitions which leads to performance increases.\nBroadcast joins are a viable way to increase join performance for skewed data over sort-merge joins.\nThis statement is correct. Broadcast joins can indeed help increase join performance for skewed data,\nunder some conditions. One of the DataFrames to be joined needs to be small enough to fit into each\nexecutor's memory, along a partition from the other DataFrame. If this is the case, a broadcast join\nincreases join performance over a sort-merge join.\nThe reason is that a sort-merge join with skewed data involves excessive shuffling. During shuffling,\ndata is sent around the cluster, ultimately slowing down the Spark application. For skewed data, the\namount of data, and thus the slowdown, is particularly big.\nBroadcast joins, however, help reduce shuffling data. The smaller table is directly stored on all\nexecutors, eliminating a great amount of network traffic, ultimately increasing join performance\nrelative to the sort-merge join.\nIt is worth noting that for optimizing skew join behavior it may make sense to manually adjust Spark's\nspark.sql.autoBroadcastJoinThreshold configuration property if the smaller DataFrame is bigger than\nthe 10 MB set by default.\nMore info:\n- Performance Tuning - Spark 3.0.0 Documentation\n- Data Skew and Garbage Collection to Improve Spark Performance\n- Section 1.2 - Joins on Skewed Data * GitBook"
    },
    {
        "question": "The code block displayed below contains an error. The code block should return a copy of\nDataFrame transactionsDf where the name of column transactionId has been changed to\ntransactionNumber. Find the error.\nCode block:\ntransactionsDf.withColumn(\"transactionNumber\", \"transactionId\")",
        "options": [
            "A. The arguments to the withColumn method need to be reordered.",
            "B. The arguments to the withColumn method need to be reordered and the copy() operator should\n\n\nbe appended to the code block to ensure a copy is returned.",
            "C. The copy() operator should be appended to the code block to ensure a copy is returned.",
            "D. Each column name needs to be wrapped in the col() method and method withColumn should be\nreplaced by method withColumnRenamed.",
            "E. The method withColumn should be replaced by method withColumnRenamed and the arguments\nto the method need to be reordered."
        ],
        "answer": [
            "E"
        ],
        "explanation": "Correct code block:\ntransactionsDf.withColumnRenamed(\"transactionId\", \"transactionNumber\")\nNote that in Spark, a copy is returned by default. So, there is no need to append copy() to the code\nblock.\nMore info: pyspark.sql.DataFrame.withColumnRenamed - PySpark 3.1.2 documentation Static\nnotebook | Dynamic notebook: See test 2"
    },
    {
        "question": "Which of the following code blocks returns a DataFrame with a single column in which all\nitems in column attributes of DataFrame itemsDf are listed that contain the letter i?\nSample of DataFrame itemsDf:\n1.+------+----------------------------------+-----------------------------+-------------------+\n2.|itemId|itemName |attributes |supplier |\n3.+------+----------------------------------+-----------------------------+-------------------+\n4.|1 |Thick Coat for Walking in the Snow|[blue, winter, cozy] |Sports Company Inc.|\n5.|2 |Elegant Outdoors Summer Dress |[red, summer, fresh, cooling]|YetiX |\n6.|3 |Outdoors Backpack |[green, summer, travel] |Sports Company Inc.|\n7.+------+----------------------------------+-----------------------------+-------------------+",
        "options": [
            "A. itemsDf.select(explode(\"attributes\").alias(\"attributes_exploded\")).filter(attributes_exploded.contains\n(\"i\"))",
            "B. itemsDf.explode(attributes).alias(\"attributes_exploded\").filter(col(\"attributes_exploded\").contains(\"i\n\"))",
            "C. itemsDf.select(explode(\"attributes\")).filter(\"attributes_exploded\".contains(\"i\"))",
            "D. itemsDf.select(explode(\"attributes\").alias(\"attributes_exploded\")).filter(col(\"attributes_exploded\").c\nontain",
            "E. itemsDf.select(col(\"attributes\").explode().alias(\"attributes_exploded\")).filter(col(\"attributes_explode\nd\").co"
        ],
        "answer": [
            "D"
        ],
        "explanation": "Result of correct code block:\n+-------------------+\n|attributes_exploded|\n+-------------------+\n| winter|\n\n\n| cooling|\n+-------------------+\nTo solve this question, you need to know about explode(). This operation helps you to split up arrays\ninto single rows. If you did not have a chance to familiarize yourself with this method yet, find more\nexamples in the documentation (link below).\nNote that explode() is a method made available through pyspark.sql.functions - it is not available as a\nmethod of a DataFrame or a Column, as written in some of the answer options.\nMore info: pyspark.sql.functions.explode - PySpark 3.1.2 documentation\nStatic notebook | Dynamic notebook: See test 2"
    },
    {
        "question": "The code block shown below should return the number of columns in the CSV file stored at\nlocation filePath.\nFrom the CSV file, only lines should be read that do not start with a # character. Choose the answer\nthat correctly fills the blanks in the code block to accomplish this.\nCode block:\n__1__(__2__.__3__.csv(filePath, __4__).__5__)",
        "options": [
            "A. 1. size\n2. spark\n3. read()\n4. escape='#'\n5. columns",
            "B. 1. DataFrame\n2. spark\n3. read()\n4. escape='#'\n5. shape[0]",
            "C. 1. len\n2. pyspark\n3. DataFrameReader\n4. comment='#'\n5. columns",
            "D. 1. size\n2. pyspark\n3. DataFrameReader\n4. comment='#'\n5. columns",
            "E. 1. len\n2. spark\n3. read\n4. comment='#'\n5. columns"
        ],
        "answer": [
            "E"
        ],
        "explanation": "Correct code block:\nlen(spark.read.csv(filePath, comment='#').columns)\nThis is a challenging question with difficulties in an unusual context: The boundary between\n\n\nDataFrame and the DataFrameReader. It is unlikely that a question of this difficulty level appears in\nthe exam. However, solving it helps you get more comfortable with the DataFrameReader, a subject\nyou will likely have to deal with in the exam.\nBefore dealing with the inner parentheses, it is easier to figure out the outer parentheses, gaps 1 and\n5. Given the code block, the object in gap 5 would have to be evaluated by the object in gap 1,\nreturning the number of columns in the read-in CSV. One answer option includes DataFrame in gap 1\nand shape[0] in gap 2. Since DataFrame cannot be used to evaluate shape[0], we can discard this\nanswer option.\nOther answer options include size in gap 1. size() is not a built-in Python command, so if we use it, it\nwould have to come from somewhere else. pyspark.sql.functions includes a size() method, but this\nmethod only returns the length of an array or map stored within a column (documentation linked\nbelow).\nSo, using a size() method is not an option here. This leaves us with two potentially valid answers.\nWe have to pick between gaps 2 and 3 being spark.read or pyspark.DataFrameReader. Looking at the\ndocumentation (linked below), the DataFrameReader is actually a child class of pyspark.sql, which\nmeans that we cannot import it using pyspark.DataFrameReader. Moreover, spark.read makes sense\nbecause on Databricks, spark references current Spark session (pyspark.sql.SparkSession) and\nspark.read therefore returns a DataFrameReader (also see documentation below). Finally, there is\nonly one correct answer option remaining.\nMore info:\n- pyspark.sql.functions.size - PySpark 3.1.2 documentation\n- pyspark.sql.DataFrameReader.csv - PySpark 3.1.2 documentation\n- pyspark.sql.SparkSession.read - PySpark 3.1.2 documentation\nStatic notebook | Dynamic notebook: See test 3"
    },
    {
        "question": "Which of the following code blocks prints out in how many rows the expression Inc. appears\nin the string-type column supplier of DataFrame itemsDf?",
        "options": [
            "A. 1.counter = 0\n2.\n3.for index, row in itemsDf.iterrows():\n4. if 'Inc.' in row['supplier']:\n5. counter = counter + 1\n6.\n7.print(counter)",
            "B. 1.counter = 0\n2.\n3.def count(x):\n4. if 'Inc.' in x['supplier']:\n5. counter = counter + 1\n6.\n7.itemsDf.foreach(count)\n8.print(counter)",
            "C. print(itemsDf.foreach(lambda x: 'Inc.' in x))",
            "D. print(itemsDf.foreach(lambda x: 'Inc.' in x).sum())",
            "E. 1.accum=sc.accumulator(0)\n2.\n\n\n3.def check_if_inc_in_supplier(row):\n4. if 'Inc.' in row['supplier']:\n5. accum.add(1)\n6.\n7.itemsDf.foreach(check_if_inc_in_supplier)\n8.print(accum.value)"
        ],
        "answer": [
            "E"
        ],
        "explanation": "Correct code block:\naccum=sc.accumulator(0)\ndef check_if_inc_in_supplier(row):\nif 'Inc.' in row['supplier']:\naccum.add(1)\nitemsDf.foreach(check_if_inc_in_supplier)\nprint(accum.value)\nTo answer this question correctly, you need to know both about the DataFrame.foreach() method\nand accumulators.\nWhen Spark runs the code, it executes it on the executors. The executors do not have any\ninformation about variables outside of their scope. This is whhy simply using a Python variable\ncounter, like in the two examples that start with counter = 0, will not work. You need to tell the\nexecutors explicitly that counter is a special shared variable, an Accumulator, which is managed by\nthe driver and can be accessed by all executors for the purpose of adding to it.\nIf you have used Pandas in the past, you might be familiar with the iterrows() command. Notice that\nthere is no such command in PySpark.\nThe two examples that start with print do not work, since DataFrame.foreach() does not have a\nreturn value.\nMore info: pyspark.sql.DataFrame.foreach - PySpark 3.1.2 documentation\nStatic notebook | Dynamic notebook: See test 3"
    },
    {
        "question": "Which of the following code blocks returns a copy of DataFrame transactionsDf where the\ncolumn storeId has been converted to string type?",
        "options": [
            "A. transactionsDf.withColumn(\"storeId\", convert(\"storeId\", \"string\"))",
            "B. transactionsDf.withColumn(\"storeId\", col(\"storeId\", \"string\"))",
            "C. transactionsDf.withColumn(\"storeId\", col(\"storeId\").convert(\"string\"))",
            "D. transactionsDf.withColumn(\"storeId\", col(\"storeId\").cast(\"string\"))",
            "E. transactionsDf.withColumn(\"storeId\", convert(\"storeId\").as(\"string\"))"
        ],
        "answer": [
            "D"
        ],
        "explanation": "This question asks for your knowledge about the cast syntax. cast is a method of the Column class. It\nis worth noting that one could also convert a column type using the Column.astype() method, which\nis just an alias for cast.\nFind more info in the documentation linked below.\nMore info: pyspark.sql.Column.cast - PySpark 3.1.2 documentation\nStatic notebook | Dynamic notebook: See test 2"
    },
    {
        "question": "Which of the following code blocks returns a single row from DataFrame transactionsDf?\n\n\nFull DataFrame transactionsDf:\n1.+-------------+---------+-----+-------+---------+----+\n2.|transactionId|predError|value|storeId|productId| f|\n3.+-------------+---------+-----+-------+---------+----+\n4.| 1| 3| 4| 25| 1|null|\n5.| 2| 6| 7| 2| 2|null|\n6.| 3| 3| null| 25| 3|null|\n7.| 4| null| null| 3| 2|null|\n8.| 5| null| null| null| 2|null|\n9.| 6| 3| 2| 25| 2|null|\n10.+-------------+---------+-----+-------+---------+----+",
        "options": [
            "A. transactionsDf.where(col(\"storeId\").between(3,25))",
            "B. transactionsDf.filter((col(\"storeId\")!=25) | (col(\"productId\")==2))",
            "C. transactionsDf.filter(col(\"storeId\")==25).select(\"predError\",\"storeId\").distinct()",
            "D. transactionsDf.select(\"productId\", \"storeId\").where(\"storeId == 2 OR storeId != 25\")",
            "E. transactionsDf.where(col(\"value\").isNull()).select(\"productId\", \"storeId\").distinct()"
        ],
        "answer": [
            "C"
        ],
        "explanation": "Output of correct code block:\n+---------+-------+\n|predError|storeId|\n+---------+-------+\n| 3| 25|\n+---------+-------+\nThis question is difficult because it requires you to understand different kinds of commands and\noperators. All answers are valid Spark syntax, but just one expression returns a single-row\nDataFrame.\nFor reference, here is what the incorrect answers return:\ntransactionsDf.filter((col(\"storeId\")!=25) | (col(\"productId\")==2)) returns\n+-------------+---------+-----+-------+---------+----+\n|transactionId|predError|value|storeId|productId| f|\n+-------------+---------+-----+-------+---------+----+\n| 2| 6| 7| 2| 2|null|\n| 4| null| null| 3| 2|null|\n| 5| null| null| null| 2|null|\n| 6| 3| 2| 25| 2|null|\n+-------------+---------+-----+-------+---------+----+\ntransactionsDf.where(col(\"storeId\").between(3,25)) returns\n+-------------+---------+-----+-------+---------+----+\n|transactionId|predError|value|storeId|productId| f|\n+-------------+---------+-----+-------+---------+----+\n| 1| 3| 4| 25| 1|null|\n| 3| 3| null| 25| 3|null|\n| 4| null| null| 3| 2|null|\n| 6| 3| 2| 25| 2|null|\n+-------------+---------+-----+-------+---------+----+\n\n\ntransactionsDf.where(col(\"value\").isNull()).select(\"productId\", \"storeId\").distinct() returns\n+---------+-------+\n|productId|storeId|\n+---------+-------+\n| 3| 25|\n| 2| 3|\n| 2| null|\n+---------+-------+\ntransactionsDf.select(\"productId\", \"storeId\").where(\"storeId == 2 OR storeId != 25\") returns\n+---------+-------+\n|productId|storeId|\n+---------+-------+\n| 2| 2|\n| 2| 3|\n+---------+-------+\nStatic notebook | Dynamic notebook: See test 2"
    },
    {
        "question": "The code block shown below should write DataFrame transactionsDf as a parquet file to path\nstoreDir, using brotli compression and replacing any previously existing file. Choose the answer that\ncorrectly fills the blanks in the code block to accomplish this.\ntransactionsDf.__1__.format(\"parquet\").__2__(__3__).option(__4__, \"brotli\").__5__(storeDir)",
        "options": [
            "A. 1. save\n2. mode\n3. \"ignore\"\n4. \"compression\"\n5. path",
            "B. 1. store\n2. with\n3. \"replacement\"\n4. \"compression\"\n5. path",
            "C. 1. write\n2. mode\n3. \"overwrite\"\n4. \"compression\"\n5. save",
            "D. 1. save\n2. mode\n3. \"replace\"\n4. \"compression\"\n5. path",
            "E. 1. write\n2. mode\n3. \"overwrite\"\n4. compression\n\n\n5. parquet"
        ],
        "answer": [
            "D"
        ],
        "explanation": "Correct code block:\ntransactionsDf.write.format(\"parquet\").mode(\"overwrite\").option(\"compression\",\n\"snappy\").save(storeDir) Solving this question requires you to know how to access the\nDataFrameWriter (link below) from the DataFrame API - through DataFrame.write.\nAnother nuance here is about knowing the different modes available for writing parquet files that\ndetermine Spark's behavior when dealing with existing files. These, together with the compression\noptions are explained in the DataFrameWriter.parquet documentation linked below.\nFinally, bracket __5__ poses a certain challenge. You need to know which command you can use to\npass down the file path to the DataFrameWriter. Both save and parquet are valid options here.\nMore info:\n- DataFrame.write: pyspark.sql.DataFrame.write - PySpark 3.1.1 documentation\n- DataFrameWriter.parquet: pyspark.sql.DataFrameWriter.parquet - PySpark 3.1.1 documentatio\nn Static notebook | Dynamic notebook: See test 1"
    },
    {
        "question": "Which of the following code blocks displays various aggregated statistics of all columns in\nDataFrame transactionsDf, including the standard deviation and minimum of values in each column?",
        "options": [
            "A. transactionsDf.summary()",
            "B. transactionsDf.agg(\"count\", \"mean\", \"stddev\", \"25%\", \"50%\", \"75%\", \"min\")",
            "C. transactionsDf.summary(\"count\", \"mean\", \"stddev\", \"25%\", \"50%\", \"75%\", \"max\").show()",
            "D. transactionsDf.agg(\"count\", \"mean\", \"stddev\", \"25%\", \"50%\", \"75%\", \"min\").show()",
            "E. transactionsDf.summary().show()"
        ],
        "answer": [
            "E"
        ],
        "explanation": "The DataFrame.summary() command is very practical for quickly calculating statistics of a DataFrame.\nYou need to call .show() to display the results of the calculation. By default, the command calculates\nvarious statistics (see documentation linked below), including standard deviation and minimum.\nNote that the answer that lists many options in the summary() parentheses does not include the\nminimum, which is asked for in the question.\nAnswer options that include agg() do not work here as shown, since DataFrame.agg() expects more\ncomplex, column-specific instructions on how to aggregate values.\nMore info:\n- pyspark.sql.DataFrame.summary - PySpark 3.1.2 documentation\n- pyspark.sql.DataFrame.agg - PySpark 3.1.2 documentation\nStatic notebook | Dynamic notebook: See test 3"
    },
    {
        "question": "Which of the following code blocks returns a single-column DataFrame showing the number\nof words in column supplier of DataFrame itemsDf?\nSample of DataFrame itemsDf:\n1.+------+-----------------------------+-------------------+\n2.|itemId|attributes |supplier |\n3.+------+-----------------------------+-------------------+\n4.|1 |[blue, winter, cozy] |Sports Company Inc.|\n5.|2 |[red, summer, fresh, cooling]|YetiX |\n\n\n6.|3 |[green, summer, travel] |Sports Company Inc.|\n7.+------+-----------------------------+-------------------+",
        "options": [
            "A. itemsDf.split(\"supplier\", \" \").count()",
            "B. itemsDf.split(\"supplier\", \" \").size()",
            "C. itemsDf.select(word_count(\"supplier\"))",
            "D. spark.select(size(split(col(supplier), \" \")))",
            "E. itemsDf.select(size(split(\"supplier\", \" \")))"
        ],
        "answer": [
            "E"
        ],
        "explanation": "Output of correct code block:\n+----------------------------+\n|size(split(supplier, , -1))|\n+----------------------------+\n| 3|\n| 1|\n| 3|\n+----------------------------+\nThis question shows a typical use case for the split command: Splitting a string into words. An\nadditional difficulty is that you are asked to count the words. Although it is tempting to use the count\nmethod here, the size method (as in: size of an array) is actually the correct one to use. Familiarize\nyourself with the split and the size methods using the linked documentation below.\nMore info:\nSplit method: pyspark.sql.functions.split - PySpark 3.1.2 documentation Size method:\npyspark.sql.functions.size - PySpark 3.1.2 documentation Static notebook | Dynamic notebook: See\ntest 2"
    },
    {
        "question": "Which of the following code blocks returns only rows from DataFrame transactionsDf in\nwhich values in column productId are unique?",
        "options": [
            "A. transactionsDf.distinct(\"productId\")",
            "B. transactionsDf.dropDuplicates(subset=[\"productId\"])",
            "C. transactionsDf.drop_duplicates(subset=\"productId\")",
            "D. transactionsDf.unique(\"productId\")",
            "E. transactionsDf.dropDuplicates(subset=\"productId\")"
        ],
        "answer": [
            "B"
        ],
        "explanation": "Although the question suggests using a method called unique() here, that method does not actually\nexist in PySpark. In PySpark, it is called distinct(). But then, this method is not the right one to use\nhere, since with distinct() we could filter out unique values in a specific column.\nHowever, we want to return the entire rows here. So the trick is to use dropDuplicates with the\nsubset keyword parameter. In the documentation for dropDuplicates, the examples show that subset\nshould be used with a list. And this is exactly the key to solving this question: The productId column\nneeds to be fed into the subset argument in a list, even though it is just a single column.\nMore info: pyspark.sql.DataFrame.dropDuplicates - PySpark 3.1.1 documentation Static notebook |\nDynamic notebook: See test 1"
    },
    {
        "question": "Which of the following code blocks returns a 2-column DataFrame that shows the distinct\nvalues in column productId and the number of rows with that productId in DataFrame\ntransactionsDf?",
        "options": [
            "A. transactionsDf.count(\"productId\").distinct()",
            "B. transactionsDf.groupBy(\"productId\").agg(col(\"value\").count())",
            "C. transactionsDf.count(\"productId\")",
            "D. transactionsDf.groupBy(\"productId\").count()",
            "E. transactionsDf.groupBy(\"productId\").select(count(\"value\"))"
        ],
        "answer": [
            "D"
        ],
        "explanation": "transactionsDf.groupBy(\"productId\").count()\nCorrect. This code block first groups DataFrame transactionsDf by column productId and then counts\nthe rows in each group.\ntransactionsDf.groupBy(\"productId\").select(count(\"value\"))\nIncorrect. You cannot call select on a GroupedData object (the output of a groupBy) statement.\ntransactionsDf.count(\"productId\")\nNo. DataFrame.count() does not take any arguments.\ntransactionsDf.count(\"productId\").distinct()\nWrong. Since DataFrame.count() does not take any arguments, this option cannot be right.\ntransactionsDf.groupBy(\"productId\").agg(col(\"value\").count())\nFalse. A Column object, as returned by col(\"value\"), does not have a count() method. You can see all\navailable methods for Column object linked in the Spark documentation below.\nMore info: pyspark.sql.DataFrame.count - PySpark 3.1.2 documentation, pyspark.sql.Column - PySpar\nk\n3.1.2 documentation\nStatic notebook | Dynamic notebook: See test 3"
    },
    {
        "question": "Which of the following is the idea behind dynamic partition pruning in Spark?",
        "options": [
            "A. Dynamic partition pruning is intended to skip over the data you do not need in the results of a\nquery.",
            "B. Dynamic partition pruning concatenates columns of similar data types to optimize join\nperformance.",
            "C. Dynamic partition pruning performs wide transformations on disk instead of in memory.",
            "D. Dynamic partition pruning reoptimizes physical plans based on data types and broadcast variables.",
            "E. Dynamic partition pruning reoptimizes query plans based on runtime statistics collected during\nquery execution."
        ],
        "answer": [
            "A"
        ],
        "explanation": "Dynamic partition pruning reoptimizes query plans based on runtime statistics collected during query\nexecution.\nNo - this is what adaptive query execution does, but not dynamic partition pruning.\nDynamic partition pruning concatenates columns of similar data types to optimize join performance.\nWrong, this answer does not make sense, especially related to dynamic partition pruning.\nDynamic partition pruning reoptimizes physical plans based on data types and broadcast variables.\nIt is true that dynamic partition pruning works in joins using broadcast variables. This actually\n\n\nhappens in both the logical optimization and the physical planning stage. However, data types do not\nplay a role for the reoptimization.\nDynamic partition pruning performs wide transformations on disk instead of in memory.\nThis answer does not make sense. Dynamic partition pruning is meant to accelerate Spark -\nperforming any transformation involving disk instead of memory resources would decelerate Spark\nand certainly achieve the opposite effect of what dynamic partition pruning is intended for."
    },
    {
        "question": "The code block shown below should show information about the data type that column\nstoreId of DataFrame transactionsDf contains. Choose the answer that correctly fills the blanks in the\ncode block to accomplish this.\nCode block:\ntransactionsDf.__1__(__2__).__3__",
        "options": [
            "A. 1. select\n2. \"storeId\"\n3. print_schema()",
            "B. 1. limit\n2. 1\n3. columns",
            "C. 1. select\n2. \"storeId\"\n3. printSchema()",
            "D. 1. limit\n2. \"storeId\"\n3. printSchema()",
            "E. 1. select\n2. storeId\n3. dtypes"
        ],
        "answer": [
            "B"
        ],
        "explanation": "Correct code block:\ntransactionsDf.select(\"storeId\").printSchema()\nThe difficulty of this question is that it is hard to solve with the stepwise first-to-last-gap approach\nthat has worked well for similar questions, since the answer options are so different from one\nanother. Instead, you might want to eliminate answers by looking for patterns of frequently wrong\nanswers.\nA first pattern that you may recognize by now is that column names are not expressed in quotes. For\nthis reason, the answer that includes storeId should be eliminated.\nBy now, you may have understood that the DataFrame.limit() is useful for returning a specified\namount of rows. It has nothing to do with specific columns. For this reason, the answer that resolves\nto limit(\"storeId\") can be eliminated.\nGiven that we are interested in information about the data type, you should question whether the\nanswer that resolves to limit(1).columns provides you with this information. While\nDataFrame.columns is a valid call, it will only report back column names, but not column types. So,\nyou can eliminate this option.\nThe two remaining options either use the printSchema() or print_schema() command. You may\nremember that DataFrame.printSchema() is the only valid command of the two. The select(\"storeId\")\n\n\npart just returns the storeId column of transactionsDf - this works here, since we are only interested\nin that column's type anyways.\nMore info: pyspark.sql.DataFrame.printSchema - PySpark 3.1.2 documentation Static notebook |\nDynamic notebook: See test 3"
    },
    {
        "question": "The code block shown below should return a DataFrame with all columns of DataFrame\ntransactionsDf, but only maximum 2 rows in which column productId has at least the value 2. Choose\nthe answer that correctly fills the blanks in the code block to accomplish this.\ntransactionsDf.__1__(__2__).__3__",
        "options": [
            "A. 1. where\n2. \"productId\" > 2\n3. max(2)",
            "B. 1. where\n2. transactionsDf[productId] >= 2\n3. limit(2)",
            "C. 1. filter\n2. productId > 2\n3. max(2)",
            "D. 1. filter\n2. col(\"productId\") >= 2\n3. limit(2)",
            "E. 1. where\n2. productId >= 2\n3. limit(2)"
        ],
        "answer": [
            "D"
        ],
        "explanation": "Correct code block:\ntransactionsDf.filter(col(\"productId\") >= 2).limit(2)\nThe filter and where operators in gap 1 are just aliases of one another, so you cannot use them to\npick the right answer.\nThe column definition in gap 2 is more helpful. The DataFrame.filter() method takes an argument of\ntype Column or str. From all possible answers, only the one including col(\"productId\") >= 2 fits this\nprofile, since it returns a Column type.\nThe answer option using \"productId\" > 2 is invalid, since Spark does not understand that \"productId\"\nrefers to column productId. The answer option using transactionsDf[productId] >= 2 is wrong because\nyou cannot refer to a column using square bracket notation in Spark (if you are coming from Python\nusing Pandas, this is something to watch out for). In all other options, productId is being referred to\nas a Python variable, so they are relatively easy to eliminate.\nAlso note that the question asks for the value in column productId being at least 2. This translates to\na\n\"greater or equal\" sign (>= 2), but not a \"greater\" sign (> 2).\nAnother thing worth noting is that there is no DataFrame.max() method. If you picked any option\nincluding this, you may be confusing it with the pyspark.sql.functions.max method. The correct\nmethod to limit the amount of rows is the DataFrame.limit() method.\nMore info:\n- pyspark.sql.DataFrame.filter - PySpark 3.1.2 documentation\n\n\n- pyspark.sql.DataFrame.limit - PySpark 3.1.2 documentation\nStatic notebook | Dynamic notebook: See test 3"
    },
    {
        "question": "Which of the following code blocks returns DataFrame transactionsDf sorted in descending\norder by column predError, showing missing values last?",
        "options": [
            "A. transactionsDf.sort(asc_nulls_last(\"predError\"))",
            "B. transactionsDf.orderBy(\"predError\").desc_nulls_last()",
            "C. transactionsDf.sort(\"predError\", ascending=False)",
            "D. transactionsDf.desc_nulls_last(\"predError\")",
            "E. transactionsDf.orderBy(\"predError\").asc_nulls_last()"
        ],
        "answer": [
            "C"
        ],
        "explanation": "transactionsDf.sort(\"predError\", ascending=False)\nCorrect! When using DataFrame.sort() and setting ascending=False, the DataFrame will be sorted by\nthe specified column in descending order, putting all missing values last. An alternative, although not\nlisted as an answer here, would be transactionsDf.sort(desc_nulls_last(\"predError\")).\ntransactionsDf.sort(asc_nulls_last(\"predError\"))\nIncorrect. While this is valid syntax, the DataFrame will be sorted on column predError in ascending\norder and not in descending order, putting missing values last.\ntransactionsDf.desc_nulls_last(\"predError\")\nWrong, this is invalid syntax. There is no method DataFrame.desc_nulls_last() in the Spark API. There\nis a Spark function desc_nulls_last() however (link see below).\ntransactionsDf.orderBy(\"predError\").desc_nulls_last()\nNo. While transactionsDf.orderBy(\"predError\") is correct syntax (although it sorts the DataFrame by\ncolumn predError in ascending order) and returns a DataFrame, there is no method\nDataFrame.desc_nulls_last() in the Spark API. There is a Spark function desc_nulls_last() however\n(link see below).\ntransactionsDf.orderBy(\"predError\").asc_nulls_last()\nIncorrect. There is no method DataFrame.asc_nulls_last() in the Spark API (see above).\nMore info: pyspark.sql.functions.desc_nulls_last - PySpark 3.1.2 documentation and\npyspark.sql.DataFrame.sort - PySpark 3.1.2 documentation (https://bit.ly/3g1JtbI ,\nhttps://bit.ly/2R90NCS) Static notebook | Dynamic notebook: See test 1\n(https://flrs.github.io/spark_practice_tests_code/#1/32.html ,\nhttps://bit.ly/sparkpracticeexams_import_instructions)"
    },
    {
        "question": "The code block displayed below contains an error. The code block is intended to perform an\nouter join of DataFrames transactionsDf and itemsDf on columns productId and itemId, respectively.\nFind the error.\nCode block:\ntransactionsDf.join(itemsDf, [itemsDf.itemId, transactionsDf.productId], \"outer\")",
        "options": [
            "A. The \"outer\" argument should be eliminated, since \"outer\" is the default join type.",
            "B. The join type needs to be appended to the join() operator, like join().outer() instead of listing it as\nthe last argument inside the join() call.",
            "C. The term [itemsDf.itemId, transactionsDf.productId] should be replaced by itemsDf.itemId ==\ntransactionsDf.productId.",
            "D. The term [itemsDf.itemId, transactionsDf.productId] should be replaced by itemsDf.col(\"itemId\")\n== transactionsDf.col(\"productId\").",
            "E. The \"outer\" argument should be eliminated from the call and join should be replaced by joinOuter."
        ],
        "answer": [
            "C"
        ],
        "explanation": "Correct code block:\ntransactionsDf.join(itemsDf, itemsDf.itemId == transactionsDf.productId, \"outer\") Static notebook |\nDynamic notebook: See test 1 (https://flrs.github.io/spark_practice_tests_code/#1/33.html ,\nhttps://bit.ly/sparkpracticeexams_import_instructions)"
    },
    {
        "question": "Which of the following code blocks immediately removes the previously cached DataFrame\ntransactionsDf from memory and disk?",
        "options": [
            "A. array_remove(transactionsDf, \"*\")",
            "B. transactionsDf.unpersist()",
            "C. del transactionsDf",
            "D. transactionsDf.clearCache()",
            "E. transactionsDf.persist()"
        ],
        "answer": [
            "B"
        ],
        "explanation": "transactionsDf.unpersist()\nCorrect. The DataFrame.unpersist() command does exactly what the question asks for - it removes all\ncached parts of the DataFrame from memory and disk.\ndel transactionsDf\nFalse. While this option can help remove the DataFrame from memory and disk, it does not do so\nimmediately. The reason is that this command just notifies the Python garbage collector that the\ntransactionsDf now may be deleted from memory. However, the garbage collector does not do so\nimmediately and, if you wanted it to run immediately, would need to be specifically triggered to do\nso. Find more information linked below.\narray_remove(transactionsDf, \"*\")\nIncorrect. The array_remove method from pyspark.sql.functions is used for removing elements from\narrays in columns that match a specific condition. Also, the first argument would be a column, and\nnot a DataFrame as shown in the code block.\ntransactionsDf.persist()\nNo. This code block does exactly the opposite of what is asked for: It caches (writes) DataFrame\ntransactionsDf to memory and disk. Note that even though you do not pass in a specific storage level\nhere, Spark will use the default storage level (MEMORY_AND_DISK).\ntransactionsDf.clearCache()\nWrong. Spark's DataFrame does not have a clearCache() method.\nMore info: pyspark.sql.DataFrame.unpersist - PySpark 3.1.2 documentation, python - How to delete a\nn RDD in PySpark for the purpose of releasing resources? - Stack Overflow Static notebook | Dynamic\nnotebook: See test 3"
    },
    {
        "question": "The code block shown below should return a single-column DataFrame with a column named\nconsonant_ct that, for each row, shows the number of consonants in column itemName of\n\n\nDataFrame itemsDf. Choose the answer that correctly fills the blanks in the code block to accomplish\nthis.\nDataFrame itemsDf:\n1.+------+----------------------------------+-----------------------------+-------------------+\n2.|itemId|itemName |attributes |supplier |\n3.+------+----------------------------------+-----------------------------+-------------------+\n4.|1 |Thick Coat for Walking in the Snow|[blue, winter, cozy] |Sports Company Inc.|\n5.|2 |Elegant Outdoors Summer Dress |[red, summer, fresh, cooling]|YetiX |\n6.|3 |Outdoors Backpack |[green, summer, travel] |Sports Company Inc.|\n7.+------+----------------------------------+-----------------------------+-------------------+ Code block:\nitemsDf.select(__1__(__2__(__3__(__4__), \"a|e|i|o|u|\\s\", \"\")).__5__(\"consonant_ct\"))",
        "options": [
            "A. 1. length\n2. regexp_extract\n3. upper\n4. col(\"itemName\")\n5. as",
            "B. 1. size\n2. regexp_replace\n3. lower\n4. \"itemName\"\n5. alias",
            "C. 1. lower\n2. regexp_replace\n3. length\n4. \"itemName\"\n5. alias",
            "D. 1. length\n2. regexp_replace\n3. lower\n4. col(\"itemName\")\n5. alias",
            "E. 1. size\n2. regexp_extract\n3. lower\n4. col(\"itemName\")\n5. alias"
        ],
        "answer": [
            "D"
        ],
        "explanation": "Correct code block:\nitemsDf.select(length(regexp_replace(lower(col(\"itemName\")), \"a|e|i|o|u|\\s\",\n\"\")).alias(\"consonant_ct\")) Returned DataFrame:\n+------------+\n|consonant_ct|\n+------------+\n| 19|\n\n\n| 16|\n| 10|\n+------------+\nThis question tries to make you think about the string functions Spark provides and in which order\nthey should be applied. Arguably the most difficult part, the regular expression \"a|e|i|o|u|\n\\s\", is not a numbered blank. However, if you are not familiar with the string functions, it may be a\ngood idea to review those before the exam.\nThe size operator and the length operator can easily be confused. size works on arrays, while length\nworks on strings. Luckily, this is something you can read up about in the documentation.\nThe code block works by first converting all uppercase letters in column itemName into lowercase\n(the lower() part). Then, it replaces all vowels by \"nothing\" - an empty character \"\" (the\nregexp_replace() part). Now, only lowercase characters without spaces are included in the\nDataFrame. Then, per row, the length operator counts these remaining characters. Note that column\nitemName in itemsDf does not include any numbers or other characters, so we do not need to make\nany provisions for these. Finally, by using the alias() operator, we rename the resulting column to\nconsonant_ct.\nMore info:\n- lower: pyspark.sql.functions.lower - PySpark 3.1.2 documentation\n- regexp_replace: pyspark.sql.functions.regexp_replace - PySpark 3.1.2 documentation\n- length: pyspark.sql.functions.length - PySpark 3.1.2 documentation\n- alias: pyspark.sql.Column.alias - PySpark 3.1.2 documentation\nStatic notebook | Dynamic notebook: See test 2"
    },
    {
        "question": "Which of the following is a characteristic of the cluster manager?",
        "options": [
            "A. Each cluster manager works on a single partition of data.",
            "B. The cluster manager receives input from the driver through the SparkContext.",
            "C. The cluster manager does not exist in standalone mode.",
            "D. The cluster manager transforms jobs into DAGs.",
            "E. In client mode, the cluster manager runs on the edge node."
        ],
        "answer": [
            "B"
        ],
        "explanation": "The cluster manager receives input from the driver through the SparkContext.\nCorrect. In order for the driver to contact the cluster manager, the driver launches a SparkContext.\nThe driver then asks the cluster manager for resources to launch executors.\nIn client mode, the cluster manager runs on the edge node.\nNo. In client mode, the cluster manager is independent of the edge node and runs in the cluster.\nThe cluster manager does not exist in standalone mode.\nWrong, the cluster manager exists even in standalone mode. Remember, standalone mode is an easy\nmeans to deploy Spark across a whole cluster, with some limitations. For example, in standalone\nmode, no other frameworks can run in parallel with Spark. The cluster manager is part of Spark in\nstandalone deployments however and helps launch and maintain resources across the cluster.\nThe cluster manager transforms jobs into DAGs.\nNo, transforming jobs into DAGs is the task of the Spark driver.\nEach cluster manager works on a single partition of data.\nNo. Cluster managers do not work on partitions directly. Their job is to coordinate cluster resources\nso that they can be requested by and allocated to Spark drivers.\n\n\nMore info: Introduction to Core Spark Concepts * BigData"
    },
    {
        "question": "The code block displayed below contains an error. The code block should return DataFrame\ntransactionsDf, but with the column storeId renamed to storeNumber. Find the error.\nCode block:\ntransactionsDf.withColumn(\"storeNumber\", \"storeId\")",
        "options": [
            "A. Instead of withColumn, the withColumnRenamed method should be used.",
            "B. Arguments \"storeNumber\" and \"storeId\" each need to be wrapped in a col() operator.",
            "C. Argument \"storeId\" should be the first and argument \"storeNumber\" should be the second\nargument to the withColumn method.",
            "D. The withColumn operator should be replaced with the copyDataFrame operator.",
            "E. Instead of withColumn, the withColumnRenamed method should be used and argument \"storeId\"\nshould be the first and argument \"storeNumber\" should be the second argument to that method."
        ],
        "answer": [
            "E"
        ],
        "explanation": "Correct code block:\ntransactionsDf.withColumnRenamed(\"storeId\", \"storeNumber\")\nMore info: pyspark.sql.DataFrame.withColumnRenamed - PySpark 3.1.1 documentation Static\nnotebook | Dynamic notebook: See test 1"
    },
    {
        "question": "Which of the following code blocks performs an inner join of DataFrames transactionsDf and\nitemsDf on columns productId and itemId, respectively, excluding columns value and storeId from\nDataFrame transactionsDf and column attributes from DataFrame itemsDf?",
        "options": [
            "A. transactionsDf.drop('value', 'storeId').join(itemsDf.select('attributes'),\ntransactionsDf.productId==itemsDf.itemId)",
            "B. 1.transactionsDf.createOrReplaceTempView('transactionsDf')\n2.itemsDf.createOrReplaceTempView('itemsDf')\n3.\n4.spark.sql(\"SELECT -value, -storeId FROM transactionsDf INNER JOIN itemsDf ON\nproductId==itemId\").drop(\"attributes\")",
            "C. transactionsDf.drop(\"value\", \"storeId\").join(itemsDf.drop(\"attributes\"),\n\"transactionsDf.productId==itemsDf.itemId\")",
            "D. 1.transactionsDf \\\n2. .drop(col('value'), col('storeId')) \\\n3. .join(itemsDf.drop(col('attributes')), col('productId')==col('itemId'))",
            "E. 1.transactionsDf.createOrReplaceTempView('transactionsDf')\n2.itemsDf.createOrReplaceTempView('itemsDf')\n3.\n4.statement = \"\"\"\n5.SELECT * FROM transactionsDf\n6.INNER JOIN itemsDf\n7.ON transactionsDf.productId==itemsDf.itemId\n8.\"\"\"\n9.spark.sql(statement).drop(\"value\", \"storeId\", \"attributes\")"
        ],
        "answer": [
            "E"
        ],
        "explanation": "This question offers you a wide variety of answers for a seemingly simple question. However, this\nvariety reflects the variety of ways that one can express a join in PySpark. You need to understand\nsome SQL syntax to get to the correct answer here.\ntransactionsDf.createOrReplaceTempView('transactionsDf')\nitemsDf.createOrReplaceTempView('itemsDf')\nstatement = \"\"\"\nSELECT * FROM transactionsDf\nINNER JOIN itemsDf\nON transactionsDf.productId==itemsDf.itemId\n\"\"\"\nspark.sql(statement).drop(\"value\", \"storeId\", \"attributes\")\nCorrect - this answer uses SQL correctly to perform the inner join and afterwards drops the unwanted\ncolumns. This is totally fine. If you are unfamiliar with the triple-quote \"\"\" in Python: This allows you\nto express strings as multiple lines.\ntransactionsDf \\\ndrop(col('value'), col('storeId')) \\\njoin(itemsDf.drop(col('attributes')), col('productId')==col('itemId'))\nNo, this answer option is a trap, since DataFrame.drop() does not accept a list of Column objects. You\ncould use transactionsDf.drop('value', 'storeId') instead.\ntransactionsDf.drop(\"value\", \"storeId\").join(itemsDf.drop(\"attributes\"),\n\"transactionsDf.productId==itemsDf.itemId\")\nIncorrect - Spark does not evaluate \"transactionsDf.productId==itemsDf.itemId\" as a valid join\nexpression.\nThis would work if it would not be a string.\ntransactionsDf.drop('value', 'storeId').join(itemsDf.select('attributes'),\ntransactionsDf.productId==itemsDf.itemId) Wrong, this statement incorrectly uses itemsDf.select\ninstead of itemsDf.drop.\ntransactionsDf.createOrReplaceTempView('transactionsDf')\nitemsDf.createOrReplaceTempView('itemsDf')\nspark.sql(\"SELECT -value, -storeId FROM transactionsDf INNER JOIN itemsDf ON\nproductId==itemId\").drop(\"attributes\") No, here the SQL expression syntax is incorrect. Simply\nspecifying -columnName does not drop a column.\nMore info: pyspark.sql.DataFrame.join - PySpark 3.1.2 documentation\nStatic notebook | Dynamic notebook: See test 3"
    },
    {
        "question": "Which of the following code blocks reorders the values inside the arrays in column attributes\nof DataFrame itemsDf from last to first one in the alphabet?\n1.+------+-----------------------------+-------------------+\n2.|itemId|attributes |supplier |\n3.+------+-----------------------------+-------------------+\n4.|1 |[blue, winter, cozy] |Sports Company Inc.|\n5.|2 |[red, summer, fresh, cooling]|YetiX |\n6.|3 |[green, summer, travel] |Sports Company Inc.|\n7.+------+-----------------------------+-------------------+",
        "options": [
            "A. itemsDf.withColumn('attributes', sort_array(col('attributes').desc()))",
            "B. itemsDf.withColumn('attributes', sort_array(desc('attributes')))",
            "C. itemsDf.withColumn('attributes', sort(col('attributes'), asc=False))",
            "D. itemsDf.withColumn(\"attributes\", sort_array(\"attributes\", asc=False))",
            "E. itemsDf.select(sort_array(\"attributes\"))"
        ],
        "answer": [
            "D"
        ],
        "explanation": "Output of correct code block:\n+------+-----------------------------+-------------------+\n|itemId|attributes |supplier |\n+------+-----------------------------+-------------------+\n|1 |[winter, cozy, blue] |Sports Company Inc.|\n|2 |[summer, red, fresh, cooling]|YetiX |\n|3 |[travel, summer, green] |Sports Company Inc.|\n+------+-----------------------------+-------------------+\nIt can be confusing to differentiate between the different sorting functions in PySpark. In this case, a\nparticularity about sort_array has to be considered: The sort direction is given by the second\nargument, not by the desc method. Luckily, this is documented in the documentation (link below).\nAlso, for solving this question you need to understand the difference between sort and sort_array.\nWith sort, you cannot sort values in arrays. Also, sort is a method of DataFrame, while sort_array is a\nmethod of pyspark.sql.functions.\nMore info: pyspark.sql.functions.sort_array - PySpark 3.1.2 documentation Static notebook | Dynamic\nnotebook: See test 2"
    },
    {
        "question": "Which of the following describes characteristics of the Dataset API?",
        "options": [
            "A. The Dataset API does not support unstructured data.",
            "B. In Python, the Dataset API mainly resembles Pandas' DataFrame API.",
            "C. In Python, the Dataset API's schema is constructed via type hints.",
            "D. The Dataset API is available in Scala, but it is not available in Python.",
            "E. The Dataset API does not provide compile-time type safety."
        ],
        "answer": [
            "D"
        ],
        "explanation": "The Dataset API is available in Scala, but it is not available in Python.\nCorrect. The Dataset API uses fixed typing and is typically used for object-oriented programming. It is\navailable when Spark is used with the Scala programming language, but not for Python. In Python,\nyou use the DataFrame API, which is based on the Dataset API.\nThe Dataset API does not provide compile-time type safety.\nNo - in fact, depending on the use case, the type safety that the Dataset API provides is an advantage.\nThe Dataset API does not support unstructured data.\nWrong, the Dataset API supports structured and unstructured data.\nIn Python, the Dataset API's schema is constructed via type hints.\nNo, this is not applicable since the Dataset API is not available in Python.\nIn Python, the Dataset API mainly resembles Pandas' DataFrame API.\nThe Dataset API does not exist in Python, only in Scala and Java."
    },
    {
        "question": "Which of the following describes a valid concern about partitioning?",
        "options": [
            "A. A shuffle operation returns 200 partitions if not explicitly set.",
            "B. Decreasing the number of partitions reduces the overall runtime of narrow transformations if\nthere are more executors available than partitions.",
            "C. No data is exchanged between executors when coalesce() is run.",
            "D. Short partition processing times are indicative of low skew.",
            "E. The coalesce() method should be used to increase the number of partitions."
        ],
        "answer": [
            "A"
        ],
        "explanation": "A shuffle operation returns 200 partitions if not explicitly set.\nCorrect. 200 is the default value for the Spark property spark.sql.shuffle.partitions. This property\ndetermines how many partitions Spark uses when shuffling data for joins or aggregations.\nThe coalesce() method should be used to increase the number of partitions.\nIncorrect. The coalesce() method can only be used to decrease the number of partitions.\nDecreasing the number of partitions reduces the overall runtime of narrow transformations if there\nare more executors available than partitions.\nNo. For narrow transformations, fewer partitions usually result in a longer overall runtime, if more\nexecutors are available than partitions.\nA narrow transformation does not include a shuffle, so no data need to be exchanged between\nexecutors.\nShuffles are expensive and can be a bottleneck for executing Spark workloads.\nNarrow transformations, however, are executed on a per-partition basis, blocking one executor per\npartition.\nSo, it matters how many executors are available to perform work in parallel relative to the number of\npartitions. If the number of executors is greater than the number of partitions, then some executors\nare idle while other process the partitions. On the flip side, if the number of executors is smaller than\nthe number of partitions, the entire operation can only be finished after some executors have\nprocessed multiple partitions, one after the other. To minimize the overall runtime, one would want\nto have the number of partitions equal to the number of executors (but not more).\nSo, for the scenario at hand, increasing the number of partitions reduces the overall runtime of\nnarrow transformations if there are more executors available than partitions.\nNo data is exchanged between executors when coalesce() is run.\nNo. While coalesce() avoids a full shuffle, it may still cause a partial shuffle, resulting in data exchange\nbetween executors.\nShort partition processing times are indicative of low skew.\nIncorrect. Data skew means that data is distributed unevenly over the partitions of a dataset. Low\nskew therefore means that data is distributed evenly.\nPartition processing time, the time that executors take to process partitions, can be indicative of\nskew if some executors take a long time to process a partition, but others do not. However, a short\nprocessing time is not per se indicative a low skew: It may simply be short because the partition is\nsmall.\nA situation indicative of low skew may be when all executors finish processing their partitions in the\nsame timeframe. High skew may be indicated by some executors taking much longer to finish their\npartitions than others. But the answer does not make any comparison - so by itself it does not provide\nenough information to make any assessment about skew.\nMore info: Spark Repartition & Coalesce - Explained and Performance Tuning - Spark 3.1.\n\n\n2 Documentation"
    },
    {
        "question": "Which of the following code blocks adds a column predErrorSqrt to DataFrame transactionsDf\nthat is the square root of column predError?",
        "options": [
            "A. transactionsDf.withColumn(\"predErrorSqrt\", sqrt(predError))",
            "B. transactionsDf.select(sqrt(predError))",
            "C. transactionsDf.withColumn(\"predErrorSqrt\", col(\"predError\").sqrt())",
            "D. transactionsDf.withColumn(\"predErrorSqrt\", sqrt(col(\"predError\")))",
            "E. transactionsDf.select(sqrt(\"predError\"))"
        ],
        "answer": [
            "D"
        ],
        "explanation": "transactionsDf.withColumn(\"predErrorSqrt\", sqrt(col(\"predError\")))\nCorrect. The DataFrame.withColumn() operator is used to add a new column to a DataFrame. It takes\ntwo arguments: The name of the new column (here: predErrorSqrt) and a Column expression as the\nnew column. In PySpark, a Column expression means referring to a column using the col(\"predError\")\ncommand or by other means, for example by transactionsDf.predError, or even just using the column\nname as a string, \"predError\".\nThe question asks for the square root. sqrt() is a function in pyspark.sql.functions and calculates the\nsquare root. It takes a value or a Column as an input. Here it is the predError column of DataFrame\ntransactionsDf expressed through col(\"predError\").\ntransactionsDf.withColumn(\"predErrorSqrt\", sqrt(predError))\nIncorrect. In this expression, sqrt(predError) is incorrect syntax. You cannot refer to predError in this\nway - to Spark it looks as if you are trying to refer to the non-existent Python variable predError.\nYou could pass transactionsDf.predError, col(\"predError\") (as in the correct solution), or even just\n\"predError\" instead.\ntransactionsDf.select(sqrt(predError))\nWrong. Here, the explanation just above this one about how to refer to predError applies.\ntransactionsDf.select(sqrt(\"predError\"))\nNo. While this is correct syntax, it will return a single-column DataFrame only containing a column\nshowing the square root of column predError. However, the question asks for a column to be added\nto the original DataFrame transactionsDf.\ntransactionsDf.withColumn(\"predErrorSqrt\", col(\"predError\").sqrt())\nNo. The issue with this statement is that column col(\"predError\") has no sqrt() method. sqrt() is a\nmember of pyspark.sql.functions, but not of pyspark.sql.Column.\nMore info: pyspark.sql.DataFrame.withColumn - PySpark 3.1.2 documentation and\npyspark.sql.functions.sqrt - PySpark 3.1.2 documentation Static notebook | Dynamic notebook: See\ntest 2"
    },
    {
        "question": "Which of the following code blocks sorts DataFrame transactionsDf both by column storeId in\nascending and by column productId in descending order, in this priority?",
        "options": [
            "A. transactionsDf.sort(\"storeId\", asc(\"productId\"))",
            "B. transactionsDf.sort(col(storeId)).desc(col(productId))",
            "C. transactionsDf.order_by(col(storeId), desc(col(productId)))",
            "D. transactionsDf.sort(\"storeId\", desc(\"productId\"))",
            "E. transactionsDf.sort(\"storeId\").sort(desc(\"productId\"))"
        ],
        "answer": [
            "D"
        ],
        "explanation": "In this question it is important to realize that you are asked to sort transactionDf by two columns.\nThis means that the sorting of the second column depends on the sorting of the first column.\nSo, any option that sorts the entire DataFrame (through chaining sort statements) will not work. The\ntwo columns need to be channeled through the same call to sort().\nAlso, order_by is not a valid DataFrame API method.\nMore info: pyspark.sql.DataFrame.sort - PySpark 3.1.2 documentation\nStatic notebook | Dynamic notebook: See test 2"
    },
    {
        "question": "The code block displayed below contains an error. The code block should merge the rows of\nDataFrames transactionsDfMonday and transactionsDfTuesday into a new DataFrame, matching\ncolumn names and inserting null values where column names do not appear in both DataFrames.\nFind the error.\nSample of DataFrame transactionsDfMonday:\n1.+-------------+---------+-----+-------+---------+----+\n2.|transactionId|predError|value|storeId|productId| f|\n3.+-------------+---------+-----+-------+---------+----+\n4.| 5| null| null| null| 2|null|\n5.| 6| 3| 2| 25| 2|null|\n6.+-------------+---------+-----+-------+---------+----+\nSample of DataFrame transactionsDfTuesday:\n1.+-------+-------------+---------+-----+\n2.|storeId|transactionId|productId|value|\n3.+-------+-------------+---------+-----+\n4.| 25| 1| 1| 4|\n5.| 2| 2| 2| 7|\n6.| 3| 4| 2| null|\n7.| null| 5| 2| null|\n8.+-------+-------------+---------+-----+\nCode block:\nsc.union([transactionsDfMonday, transactionsDfTuesday])",
        "options": [
            "A. The DataFrames' RDDs need to be passed into the sc.union method instead of the DataFrame\nvariable names.",
            "B. Instead of union, the concat method should be used, making sure to not use its default arguments.",
            "C. Instead of the Spark context, transactionDfMonday should be called with the join method instead\nof the union method, making sure to use its default arguments.",
            "D. Instead of the Spark context, transactionDfMonday should be called with the union method.",
            "E. Instead of the Spark context, transactionDfMonday should be called with the unionByName\nmethod instead of the union method, making sure to not use its default arguments."
        ],
        "answer": [
            "E"
        ],
        "explanation": "Correct code block:\ntransactionsDfMonday.unionByName(transactionsDfTuesday, True)\nOutput of correct code block:\n\n\n+-------------+---------+-----+-------+---------+----+\n|transactionId|predError|value|storeId|productId| f|\n+-------------+---------+-----+-------+---------+----+\n| 5| null| null| null| 2|null|\n| 6| 3| 2| 25| 2|null|\n| 1| null| 4| 25| 1|null|\n| 2| null| 7| 2| 2|null|\n| 4| null| null| 3| 2|null|\n| 5| null| null| null| 2|null|\n+-------------+---------+-----+-------+---------+----+\nFor solving this question, you should be aware of the difference between the DataFrame.union() and\nDataFrame.unionByName() methods. The first one matches columns independent of their names,\njust by their order. The second one matches columns by their name (which is asked for in the\nquestion). It also has a useful optional argument, allowMissingColumns. This allows you to merge\nDataFrames that have different columns - just like in this example.\nsc stands for SparkContext and is automatically provided when executing code on Databricks. While\nsc.union() allows you to join RDDs, it is not the right choice for joining DataFrames. A hint away from\nsc.union() is given where the question talks about joining \"into a new DataFrame\".\nconcat is a method in pyspark.sql.functions. It is great for consolidating values from different\ncolumns, but has no place when trying to join rows of multiple DataFrames.\nFinally, the join method is a contender here. However, the default join defined for that method is an\ninner join which does not get us closer to the goal to match the two DataFrames as instructed,\nespecially given that with the default arguments we cannot define a join condition.\nMore info:\n- pyspark.sql.DataFrame.unionByName - PySpark 3.1.2 documentation\n- pyspark.SparkContext.union - PySpark 3.1.2 documentation\n- pyspark.sql.functions.concat - PySpark 3.1.2 documentation\nStatic notebook | Dynamic notebook: See test 3"
    },
    {
        "question": "In which order should the code blocks shown below be run in order to read a JSON file from\nlocation jsonPath into a DataFrame and return only the rows that do not have value 3 in column\nproductId?\n1. importedDf.createOrReplaceTempView(\"importedDf\")\n2. spark.sql(\"SELECT * FROM importedDf WHERE productId != 3\")\n3. spark.sql(\"FILTER * FROM importedDf WHERE productId != 3\")\n4. importedDf = spark.read.option(\"format\", \"json\").path(jsonPath)\n5. importedDf = spark.read.json(jsonPath)",
        "options": [
            "A. 4, 1, 2",
            "B. 5, 1, 3",
            "C. 5, 2",
            "D. 4, 1, 3",
            "E. 5, 1, 2"
        ],
        "answer": [
            "E"
        ],
        "explanation": "Correct code block:\nimportedDf = spark.read.json(jsonPath)\n\n\nimportedDf.createOrReplaceTempView(\"importedDf\")\nspark.sql(\"SELECT * FROM importedDf WHERE productId != 3\")\nOption 5 is the only correct way listed of reading in a JSON in PySpark. The option(\"format\", \"json\") is\nnot the correct way to tell Spark's DataFrameReader that you want to read a JSON file. You would do\nthis through format(\"json\") instead. Also, you can communicate the specific path of the JSON file to\nthe DataFramReader using the load() method, not the path() method.\nIn order to use a SQL command through the SparkSession spark, you first need to create a temporary\nview through DataFrame.createOrReplaceTempView().\nThe SQL statement should start with the SELECT operator. The FILTER operator SQL provides is not\nthe correct one to use here.\nStatic notebook | Dynamic notebook: See test 2"
    },
    {
        "question": "Which of the following code blocks reduces a DataFrame from 12 to 6 partitions and\nperforms a full shuffle?",
        "options": [
            "A. DataFrame.repartition(12)",
            "B. DataFrame.coalesce(6).shuffle()",
            "C. DataFrame.coalesce(6)",
            "D. DataFrame.coalesce(6, shuffle=True)",
            "E. DataFrame.repartition(6)"
        ],
        "answer": [
            "E"
        ],
        "explanation": "DataFrame.repartition(6)\nCorrect. repartition() always triggers a full shuffle (different from coalesce()).\nDataFrame.repartition(12)\nNo, this would just leave the DataFrame with 12 partitions and not 6.\nDataFrame.coalesce(6)\ncoalesce does not perform a full shuffle of the data. Whenever you see \"full shuffle\", you know that\nyou are not dealing with coalesce(). While coalesce() can perform a partial shuffle when required, it\nwill try to minimize shuffle operations, so the amount of data that is sent between executors.\nHere, 12 partitions can easily be repartitioned to be 6 partitions simply by stitching every two\npartitions into one.\nDataFrame.coalesce(6, shuffle=True) and DataFrame.coalesce(6).shuffle() These statements are not\nvalid Spark API syntax.\nMore info: Spark Repartition & Coalesce - Explained and Repartition vs Coalesce in Apache Spark\n- Rock the JVM Blog"
    },
    {
        "question": "Which of the following describes a difference between Spark's cluster and client execution\nmodes?",
        "options": [
            "A. In cluster mode, the cluster manager resides on a worker node, while it resides on an edge node in\nclient mode.",
            "B. In cluster mode, executor processes run on worker nodes, while they run on gateway nodes in\nclient mode.",
            "C. In cluster mode, the driver resides on a worker node, while it resides on an edge node in client\nmode.",
            "D. In cluster mode, a gateway machine hosts the driver, while it is co-located with the executor in\n\n\nclient mode.",
            "E. In cluster mode, the Spark driver is not co-located with the cluster manager, while it is co-located\nin client mode."
        ],
        "answer": [
            "C"
        ],
        "explanation": "In cluster mode, the driver resides on a worker node, while it resides on an edge node in client mode.\nCorrect. The idea of Spark's client mode is that workloads can be executed from an edge node, also\nknown as gateway machine, from outside the cluster. The most common way to execute Spark\nhowever is in cluster mode, where the driver resides on a worker node.\nIn practice, in client mode, there are tight constraints about the data transfer speed relative to the\ndata transfer speed between worker nodes in the cluster. Also, any job in that is executed in client\nmode will fail if the edge node fails. For these reasons, client mode is usually not used in a production\nenvironment.\nIn cluster mode, the cluster manager resides on a worker node, while it resides on an edge node in\nclient execution mode.\nNo. In both execution modes, the cluster manager may reside on a worker node, but it does not\nreside on an edge node in client mode.\nIn cluster mode, executor processes run on worker nodes, while they run on gateway nodes in client\nmode.\nThis is incorrect. Only the driver runs on gateway nodes (also known as \"edge nodes\") in client mode,\nbut not the executor processes.\nIn cluster mode, the Spark driver is not co-located with the cluster manager, while it is co-located in\nclient mode.\nNo, in client mode, the Spark driver is not co-located with the driver. The whole point of client mode\nis that the driver is outside the cluster and not associated with the resource that manages the cluster\n(the machine that runs the cluster manager).\nIn cluster mode, a gateway machine hosts the driver, while it is co-located with the executor in client\nmode.\nNo, it is exactly the opposite: There are no gateway machines in cluster mode, but in client mode,\nthey host the driver."
    },
    {
        "question": "Which of the following code blocks creates a new one-column, two-row DataFrame dfDates\nwith column date of type timestamp?",
        "options": [
            "A. 1.dfDates = spark.createDataFrame([\"23/01/2022 11:28:12\",\"24/01/2022 10:58:34\"], [\"date\"])\n2.dfDates = dfDates.withColumn(\"date\", to_timestamp(\"dd/MM/yyyy HH:mm:ss\", \"date\"))",
            "B. 1.dfDates = spark.createDataFrame([(\"23/01/2022 11:28:12\",),(\"24/01/2022 10:58:34\",)], [\"date\"]\n)\n2.dfDates = dfDates.withColumnRenamed(\"date\", to_timestamp(\"date\", \"yyyy-MM-dd HH:mm:ss\"))",
            "C. 1.dfDates = spark.createDataFrame([(\"23/01/2022 11:28:12\",),(\"24/01/2022 10:58:34\",)], [\"date\"])\n2.dfDates = dfDates.withColumn(\"date\", to_timestamp(\"date\", \"dd/MM/yyyy HH:mm:ss\"))",
            "D. 1.dfDates = spark.createDataFrame([\"23/01/2022 11:28:12\",\"24/01/2022 10:58:34\"], [\"date\"])\n2.dfDates = dfDates.withColumnRenamed(\"date\", to_datetime(\"date\", \"yyyy-MM-dd HH:mm:ss\"))",
            "E. 1.dfDates = spark.createDataFrame([(\"23/01/2022 11:28:12\",),(\"24/01/2022 10:58:34\",)], [\"date\"])"
        ],
        "answer": [
            "C"
        ],
        "explanation": "This question is tricky. Two things are important to know here:\nFirst, the syntax for createDataFrame: Here you need a list of tuples, like so: [(1,), (2,)]. To define a\ntuple in Python, if you just have a single item in it, it is important to put a comma after the item so\nthat Python interprets it as a tuple and not just a normal parenthesis.\nSecond, you should understand the to_timestamp syntax. You can find out more about it in the\ndocumentation linked below.\nFor good measure, let's examine in detail why the incorrect options are wrong:\ndfDates = spark.createDataFrame([(\"23/01/2022 11:28:12\",),(\"24/01/2022 10:58:34\",)], [\"date\"]) This\ncode snippet does everything the question asks for - except that the data type of the date column is a\nstring and not a timestamp. When no schema is specified, Spark sets the string data type as default.\ndfDates = spark.createDataFrame([\"23/01/2022 11:28:12\",\"24/01/2022 10:58:34\"], [\"date\"]) dfDates\n= dfDates.withColumn(\"date\", to_timestamp(\"dd/MM/yyyy HH:mm:ss\", \"date\")) In the first row of\nthis command, Spark throws the following error: TypeError: Can not infer schema for type:\n<class 'str'>. This is because Spark expects to find row information, but instead finds strings. This is\nwhy you need to specify the data as tuples. Fortunately, the Spark documentation (linked below)\nshows a number of examples for creating DataFrames that should help you get on the right track\nhere.\ndfDates = spark.createDataFrame([(\"23/01/2022 11:28:12\",),(\"24/01/2022 10:58:34\",)], [\"date\"])\ndfDates = dfDates.withColumnRenamed(\"date\", to_timestamp(\"date\", \"yyyy-MM-dd HH:mm:ss\"))\nThe issue with this answer is that the operator withColumnRenamed is used. This operator simply\nrenames a column, but it has no power to modify its actual content. This is why withColumn should\nbe used instead. In addition, the date format yyyy-MM-dd HH:mm:ss does not reflect the format of\nthe actual timestamp: \"23/01/2022 11:28:12\".\ndfDates = spark.createDataFrame([\"23/01/2022 11:28:12\",\"24/01/2022 10:58:34\"], [\"date\"]) dfDates\n= dfDates.withColumnRenamed(\"date\", to_datetime(\"date\", \"yyyy-MM-dd HH:mm:ss\")) Here,\nwithColumnRenamed is used instead of withColumn (see above). In addition, the rows are not\nexpressed correctly - they should be written as tuples, using parentheses. Finally, even the date\nformat is off here (see above).\nMore info: pyspark.sql.functions.to_timestamp - PySpark 3.1.2 documentation and\npyspark.sql.SparkSession.createDataFrame - PySpark 3.1.1 documentation Static notebook | Dynamic\nnotebook: See test 2"
    },
    {
        "question": "Which of the following statements about executors is correct, assuming that one can consider\neach of the JVMs working as executors as a pool of task execution slots?",
        "options": [
            "A. Slot is another name for executor.",
            "B. There must be less executors than tasks.",
            "C. An executor runs on a single core.",
            "D. There must be more slots than tasks.",
            "E. Tasks run in parallel via slots."
        ],
        "answer": [
            "E"
        ],
        "explanation": "Tasks run in parallel via slots.\nCorrect. Given the assumption, an executor then has one or more \"slots\", defined by the equation\nspark.executor.cores / spark.task.cpus. With the executor's resources divided into slots, each task\ntakes up a slot and multiple tasks can be executed in parallel.\nSlot is another name for executor.\n\n\nNo, a slot is part of an executor.\nAn executor runs on a single core.\nNo, an executor can occupy multiple cores. This is set by the spark.executor.cores option.\nThere must be more slots than tasks.\nNo. Slots just process tasks. One could imagine a scenario where there was just a single slot for\nmultiple tasks, processing one task at a time. Granted - this is the opposite of what Spark should be\nused for, which is distributed data processing over multiple cores and machines, performing many\ntasks in parallel.\nThere must be less executors than tasks.\nNo, there is no such requirement.\nMore info: Spark Architecture | Distributed Systems Architecture (https://bit.ly/3x4MZZt)"
    },
    {
        "question": "Which of the following code blocks efficiently converts DataFrame transactionsDf from 12\ninto 24 partitions?",
        "options": [
            "A. transactionsDf.repartition(24, boost=True)",
            "B. transactionsDf.repartition()",
            "C. transactionsDf.repartition(\"itemId\", 24)",
            "D. transactionsDf.coalesce(24)",
            "E. transactionsDf.repartition(24)"
        ],
        "answer": [
            "E"
        ],
        "explanation": "transactionsDf.coalesce(24)\nNo, the coalesce() method can only reduce, but not increase the number of partitions.\ntransactionsDf.repartition()\nNo, repartition() requires a numPartitions argument.\ntransactionsDf.repartition(\"itemId\", 24)\nNo, here the cols and numPartitions argument have been mixed up. If the code block would be\ntransactionsDf.repartition(24, \"itemId\"), this would be a valid solution.\ntransactionsDf.repartition(24, boost=True)\nNo, there is no boost argument in the repartition() method."
    },
    {
        "question": "Which of the following code blocks returns a DataFrame that is an inner join of DataFrame\nitemsDf and DataFrame transactionsDf, on columns itemId and productId, respectively and in which\nevery itemId just appears once?",
        "options": [
            "A. itemsDf.join(transactionsDf, \"itemsDf.itemId==transactionsDf.productId\").distinct(\"itemId\")",
            "B. itemsDf.join(transactionsDf,\nitemsDf.itemId==transactionsDf.productId).dropDuplicates([\"itemId\"])",
            "C. itemsDf.join(transactionsDf, itemsDf.itemId==transactionsDf.productId).dropDuplicates(\"itemId\")",
            "D. itemsDf.join(transactionsDf, itemsDf.itemId==transactionsDf.productId,\nhow=\"inner\").distinct([\"itemId\"])",
            "E. itemsDf.join(transactionsDf, \"itemsDf.itemId==transactionsDf.productId\",\nhow=\"inner\").dropDuplicates([\"itemId\"])"
        ],
        "answer": [
            "B"
        ],
        "explanation": "Filtering out distinct rows based on columns is achieved with the dropDuplicates method, not the\n\n\ndistinct method which does not take any arguments.\nThe second argument of the join() method only accepts strings if they are column names. The SQL-\nlike statement \"itemsDf.itemId==transactionsDf.productId\" is therefore invalid.\nIn addition, it is not necessary to specify how=\"inner\", since the default join type for the join\ncommand is already inner.\nMore info: pyspark.sql.DataFrame.join - PySpark 3.1.2 documentation\nStatic notebook | Dynamic notebook: See test 2"
    },
    {
        "question": "Which of the following describes Spark's way of managing memory?",
        "options": [
            "A. Spark uses a subset of the reserved system memory.",
            "B. Storage memory is used for caching partitions derived from DataFrames.",
            "C. As a general rule for garbage collection, Spark performs better on many small objects than few big\nobjects.",
            "D. Disabling serialization potentially greatly reduces the memory footprint of a Spark application.",
            "E. Spark's memory usage can be divided into three categories: Execution, transaction, and storage."
        ],
        "answer": [
            "B"
        ],
        "explanation": "Spark's memory usage can be divided into three categories: Execution, transaction, and storage.\nNo, it is either execution or storage.\nAs a general rule for garbage collection, Spark performs better on many small objects than few big\nobjects.\nNo, Spark's garbage collection runs faster on fewer big objects than many small objects.\nDisabling serialization potentially greatly reduces the memory footprint of a Spark application.\nThe opposite is true - serialization reduces the memory footprint, but may impact performance in a\nnegative way.\nSpark uses a subset of the reserved system memory.\nNo, the reserved system memory is separate from Spark memory. Reserved memory stores Spark's\ninternal objects.\nMore info: Tuning - Spark 3.1.2 Documentation, Spark Memory Management | Distributed Systems\nArchitecture, Learning Spark, 2nd Edition, Chapter 7"
    },
    {
        "question": "The code block displayed below contains multiple errors. The code block should return a\nDataFrame that contains only columns transactionId, predError, value and storeId of DataFrame\ntransactionsDf. Find the errors.\nCode block:\ntransactionsDf.select([col(productId), col(f)])\nSample of transactionsDf:\n1.+-------------+---------+-----+-------+---------+----+\n2.|transactionId|predError|value|storeId|productId| f|\n3.+-------------+---------+-----+-------+---------+----+\n4.| 1| 3| 4| 25| 1|null|\n5.| 2| 6| 7| 2| 2|null|\n6.| 3| 3| null| 25| 3|null|\n7.+-------------+---------+-----+-------+---------+----+",
        "options": [
            "A. The column names should be listed directly as arguments to the operator and not as a list.",
            "B. The select operator should be replaced by a drop operator, the column names should be listed\ndirectly as arguments to the operator and not as a list, and all column names should be expressed as\nstrings without being wrapped in a col() operator.",
            "C. The select operator should be replaced by a drop operator.",
            "D. The column names should be listed directly as arguments to the operator and not as a list and\nfollowing the pattern of how column names are expressed in the code block, columns productId and f\nshould be replaced by transactionId, predError, value and storeId.",
            "E. The select operator should be replaced by a drop operator, the column names should be listed\ndirectly as arguments to the operator and not as a list, and all col() operators should be removed."
        ],
        "answer": [
            "B"
        ],
        "explanation": "Correct code block: transactionsDf.drop(\"productId\", \"f\")\nThis question requires a lot of thinking to get right. For solving it, you may take advantage of the\ndigital notepad that is provided to you during the test. You have probably seen that the code block\nincludes multiple errors. In the test, you are usually confronted with a code block that only contains a\nsingle error. However, since you are practicing here, this challenging multi-error question will make it\neasier for you to deal with single-error questions in the real exam.\nThe select operator should be replaced by a drop operator, the column names should be listed\ndirectly as arguments to the operator and not as a list, and all column names should be expressed as\nstrings without being wrapped in a col() operator.\nCorrect! Here, you need to figure out the many, many things that are wrong with the initial code\nblock. While the question can be solved by using a select statement, a drop statement, given the\nanswer options, is the correct one. Then, you can read in the documentation that drop does not take\na list as an argument, but just the column names that should be dropped. Finally, the column names\nshould be expressed as strings and not as Python variable names as in the original code block.\nThe column names should be listed directly as arguments to the operator and not as a list.\nIncorrect. While this is a good first step and part of the correct solution (see above), this modification\nis insufficient to solve the question.\nThe column names should be listed directly as arguments to the operator and not as a list and\nfollowing the pattern of how column names are expressed in the code block, columns productId and f\nshould be replaced by transactionId, predError, value and storeId.\nWrong. If you use the same pattern as in the original code block (col(productId), col(f)), you are still\nmaking a mistake. col(productId) will trigger Python to search for the content of a variable named\nproductId instead of telling Spark to use the column productId - for that, you need to express it as a\nstring.\nThe select operator should be replaced by a drop operator, the column names should be listed\ndirectly as arguments to the operator and not as a list, and all col() operators should be removed.\nNo. This still leaves you with Python trying to interpret the column names as Python variables (see\nabove).\nThe select operator should be replaced by a drop operator.\nWrong, this is not enough to solve the question. If you do this, you will still face problems since you\nare passing a Python list to drop and the column names are still interpreted as Python variables (see\nabove).\nMore info: pyspark.sql.DataFrame.drop - PySpark 3.1.2 documentation\nStatic notebook | Dynamic notebook: See test 3"
    },
    {
        "question": "Which of the following describes a narrow transformation?",
        "options": [
            "A. narrow transformation is an operation in which data is exchanged across partitions.",
            "B. A narrow transformation is a process in which data from multiple RDDs is used.",
            "C. A narrow transformation is a process in which 32-bit float variables are cast to smaller float\nvariables, like 16-bit or 8-bit float variables.",
            "D. A narrow transformation is an operation in which data is exchanged across the cluster.",
            "E. A narrow transformation is an operation in which no data is exchanged across the cluster."
        ],
        "answer": [
            "E"
        ],
        "explanation": "A narrow transformation is an operation in which no data is exchanged across the cluster.\nCorrect! In narrow transformations, no data is exchanged across the cluster, since these\ntransformations do not require any data from outside of the partition they are applied on. Typical\nnarrow transformations include filter, drop, and coalesce.\nA narrow transformation is an operation in which data is exchanged across partitions.\nNo, that would be one definition of a wide transformation, but not of a narrow transformation. Wide\ntransformations typically cause a shuffle, in which data is exchanged across partitions, executors, and\nthe cluster.\nA narrow transformation is an operation in which data is exchanged across the cluster.\nNo, see explanation just above this one.\nA narrow transformation is a process in which 32-bit float variables are cast to smaller float variables,\nlike\n16-bit or 8-bit float variables.\nNo, type conversion has nothing to do with narrow transformations in Spark.\nA narrow transformation is a process in which data from multiple RDDs is used.\nNo. A resilient distributed dataset (RDD) can be described as a collection of partitions. In a narrow\ntransformation, no data is exchanged between partitions. Thus, no data is exchanged between RDDs.\nOne could say though that a narrow transformation and, in fact, any transformation results in a new\nRDD being created. This is because a transformation results in a change to an existing RDD (RDDs are\nthe foundation of other Spark data structures, like DataFrames). But, since RDDs are immutable, a\nnew RDD needs to be created to reflect the change caused by the transformation.\nMore info: Spark Transformation and Action: A Deep Dive | by Misbah Uddin | CodeX | Medium"
    },
    {
        "question": "Which of the following describes tasks?",
        "options": [
            "A. A task is a command sent from the driver to the executors in response to a transformation.",
            "B. Tasks transform jobs into DAGs.",
            "C. A task is a collection of slots.",
            "D. A task is a collection of rows.",
            "E. Tasks get assigned to the executors by the driver."
        ],
        "answer": [
            "E"
        ],
        "explanation": "Tasks get assigned to the executors by the driver.\nCorrect! Or, in other words: Executors take the tasks that they were assigned to by the driver, run\nthem over partitions, and report the their outcomes back to the driver.\nTasks transform jobs into DAGs.\nNo, this statement disrespects the order of elements in the Spark hierarchy. The Spark driver\n\n\ntransforms jobs into DAGs. Each job consists of one or more stages. Each stage contains one or more\ntasks.\nA task is a collection of rows.\nWrong. A partition is a collection of rows. Tasks have little to do with a collection of rows. If anything,\na task processes a specific partition.\nA task is a command sent from the driver to the executors in response to a transformation.\nIncorrect. The Spark driver does not send anything to the executors in response to a transformation,\nsince transformations are evaluated lazily. So, the Spark driver would send tasks to executors only in\nresponse to actions.\nA task is a collection of slots.\nNo. Executors have one or more slots to process tasks and each slot can be assigned a task."
    },
    {
        "question": "Which of the following describes Spark actions?",
        "options": [
            "A. Writing data to disk is the primary purpose of actions.",
            "B. Actions are Spark's way of exchanging data between executors.",
            "C. The driver receives data upon request by actions.",
            "D. Stage boundaries are commonly established by actions.",
            "E. Actions are Spark's way of modifying RDDs."
        ],
        "answer": [
            "C"
        ],
        "explanation": "The driver receives data upon request by actions.\nCorrect! Actions trigger the distributed execution of tasks on executors which, upon task completion,\ntransfer result data back to the driver.\nActions are Spark's way of exchanging data between executors.\nNo. In Spark, data is exchanged between executors via shuffles.\nWriting data to disk is the primary purpose of actions.\nNo. The primary purpose of actions is to access data that is stored in Spark's RDDs and return the\ndata, often in aggregated form, back to the driver.\nActions are Spark's way of modifying RDDs.\nIncorrect. Firstly, RDDs are immutable - they cannot be modified. Secondly, Spark generates new\nRDDs via transformations and not actions.\nStage boundaries are commonly established by actions.\nWrong. A stage boundary is commonly established by a shuffle, for example caused by a wide\ntransformation."
    },
    {
        "question": "Which of the following statements about Spark's configuration properties is incorrect?",
        "options": [
            "A. The maximum number of tasks that an executor can process at the same time is controlled by the\nspark.task.cpus property.",
            "B. The maximum number of tasks that an executor can process at the same time is controlled by the\nspark.executor.cores property.",
            "C. The default value for spark.sql.autoBroadcastJoinThreshold is 10MB.",
            "D. The default number of partitions to use when shuffling data for joins or aggregations is 300.",
            "E. The default number of partitions returned from certain transformations can be controlled by the\nspark.default.parallelism property."
        ],
        "answer": [
            "D"
        ],
        "explanation": "The default number of partitions to use when shuffling data for joins or aggregations is 300.\nNo, the default value of the applicable property spark.sql.shuffle.partitions is 200.\nThe maximum number of tasks that an executor can process at the same time is controlled by the\nspark.executor.cores property.\nCorrect, see below.\nThe maximum number of tasks that an executor can process at the same time is controlled by the\nspark.task.cpus property.\nCorrect, the maximum number of tasks that an executor can process in parallel depends on both\nproperties spark.task.cpus and spark.executor.cores. This is because the available number of slots is\ncalculated by dividing the number of cores per executor by the number of cores per task. For more\ninfo specifically to this point, check out Spark Architecture | Distributed Systems Architecture.\nMore info: Configuration - Spark 3.1.2 Documentation"
    },
    {
        "question": "The code block displayed below contains an error. The code block should combine data from\nDataFrames itemsDf and transactionsDf, showing all rows of DataFrame itemsDf that have a\nmatching value in column itemId with a value in column transactionsId of DataFrame transactionsDf.\nFind the error.\nCode block:\nitemsDf.join(itemsDf.itemId==transactionsDf.transactionId)",
        "options": [
            "A. The join statement is incomplete.",
            "B. The union method should be used instead of join.",
            "C. The join method is inappropriate.",
            "D. The merge method should be used instead of join.",
            "E. The join expression is malformed."
        ],
        "answer": [
            "A"
        ],
        "explanation": "Correct code block:\nitemsDf.join(transactionsDf, itemsDf.itemId==transactionsDf.transactionId) The join statement is\nincomplete.\nCorrect! If you look at the documentation of DataFrame.join() (linked below), you see that the very\nfirst argument of join should be the DataFrame that should be joined with. This first argument is\nmissing in the code block.\nThe join method is inappropriate.\nNo. By default, DataFrame.join() uses an inner join. This method is appropriate for the scenario\ndescribed in the question.\nThe join expression is malformed.\nIncorrect. The join expression itemsDf.itemId==transactionsDf.transactionId is correct syntax.\nThe merge method should be used instead of join.\nFalse. There is no DataFrame.merge() method in PySpark.\nThe union method should be used instead of join.\nWrong. DataFrame.union() merges rows, but not columns as requested in the question.\nMore info: pyspark.sql.DataFrame.join - PySpark 3.1.2 documentation, pyspark.sql.DataFrame.union\n- PySpark 3.1.2 documentation Static notebook | Dynamic notebook: See test 3"
    },
    {
        "question": "The code block shown below should return all rows of DataFrame itemsDf that have at least 3\n\n\nitems in column itemNameElements. Choose the answer that correctly fills the blanks in the code\nblock to accomplish this.\nExample of DataFrame itemsDf:\n1.+------+----------------------------------+-------------------+------------------------------------------+\n2.|itemId|itemName |supplier |itemNameElements |\n3.+------+----------------------------------+-------------------+------------------------------------------+\n4.|1 |Thick Coat for Walking in the Snow|Sports Company Inc.|[Thick, Coat, for, Walking, in, the,\nSnow]|\n5.|2 |Elegant Outdoors Summer Dress |YetiX |[Elegant, Outdoors, Summer, Dress] |\n6.|3 |Outdoors Backpack |Sports Company Inc.|[Outdoors, Backpack] |\n7.+------+----------------------------------+-------------------+------------------------------------------+ Code block:\nitemsDf.__1__(__2__(__3__)__4__)",
        "options": [
            "A. 1. select\n2. count\n3. col(\"itemNameElements\")\n4. >3",
            "B. 1. filter\n2. count\n3. itemNameElements\n4. >=3",
            "C. 1. select\n2. count\n3. \"itemNameElements\"\n4. >3",
            "D. 1. filter\n2. size\n3. \"itemNameElements\"\n4. >=3",
            "E. 1. select\n2. size\n3. \"itemNameElements\"\n4. >3"
        ],
        "answer": [
            "D"
        ],
        "explanation": "Correct code block:\nitemsDf.filter(size(\"itemNameElements\")>3)\nOutput of code block:\n+------+----------------------------------+-------------------+------------------------------------------+\n|itemId|itemName |supplier |itemNameElements |\n+------+----------------------------------+-------------------+------------------------------------------+\n|1 |Thick Coat for Walking in the Snow|Sports Company Inc.|[Thick, Coat, for, Walking, in, the,\nSnow]|\n|2 |Elegant Outdoors Summer Dress |YetiX |[Elegant, Outdoors, Summer, Dress] |\n+------+----------------------------------+-------------------+------------------------------------------+ The big difficulty\n\n\nwith this question is in knowing the difference between count and size (refer to documentation\nbelow). size is the correct function to choose here since it returns the number of elements in an array\non a per-row basis.\nThe other consideration for solving this question is the difference between select and filter. Since we\nwant to return the rows in the original DataFrame, filter is the right choice. If we would use select, we\nwould simply get a single-column DataFrame showing which rows match the criteria, like so:\n+----------------------------+\n|(size(itemNameElements) > 3)|\n+----------------------------+\n|true |\n|true |\n|false |\n+----------------------------+\nMore info:\nCount documentation: pyspark.sql.functions.count - PySpark 3.1.1 documentation Size\ndocumentation: pyspark.sql.functions.size - PySpark 3.1.1 documentation Static notebook | Dynamic\nnotebook: See test 1"
    },
    {
        "question": "Which of the following code blocks returns a DataFrame with an added column to DataFrame\ntransactionsDf that shows the unix epoch timestamps in column transactionDate as strings in the\nformat month/day/year in column transactionDateFormatted?\nExcerpt of DataFrame transactionsDf:",
        "options": [
            "A. transactionsDf.withColumn(\"transactionDateFormatted\", from_unixtime(\"transactionDate\",\nformat=\"dd/MM/yyyy\"))",
            "B. transactionsDf.withColumnRenamed(\"transactionDate\", \"transactionDateFormatted\",\nfrom_unixtime(\"transactionDateFormatted\", format=\"MM/dd/yyyy\"))",
            "C. transactionsDf.apply(from_unixtime(format=\"MM/dd/yyyy\")).asColumn(\"transactionDateFormatted\"\n)",
            "D. transactionsDf.withColumn(\"transactionDateFormatted\", from_unixtime(\"transactionDate\",\nformat=\"MM/dd/yyyy\"))",
            "E. transactionsDf.withColumn(\"transactionDateFormatted\", from_unixtime(\"transactionDate\"))"
        ],
        "answer": [
            "D"
        ],
        "explanation": "transactionsDf.withColumn(\"transactionDateFormatted\", from_unixtime(\"transactionDate\",\nformat=\"MM/dd/yyyy\")) Correct. This code block adds a new column with the name\ntransactionDateFormatted to DataFrame transactionsDf, using Spark's from_unixtime method to\ntransform values in column transactionDate into strings, following the format requested in the\nquestion.\ntransactionsDf.withColumn(\"transactionDateFormatted\", from_unixtime(\"transactionDate\",\nformat=\"dd/MM/yyyy\")) No. Although almost correct, this uses the wrong format for the timestamp\nto date conversion: day/month/year instead of month/day/year.\ntransactionsDf.withColumnRenamed(\"transactionDate\", \"transactionDateFormatted\",\nfrom_unixtime(\"transactionDateFormatted\", format=\"MM/dd/yyyy\")) Incorrect. This answer uses\nwrong syntax. The command DataFrame.withColumnRenamed() is for renaming an existing column\n\n\nonly has two string parameters, specifying the old and the new name of the column.\ntransactionsDf.apply(from_unixtime(format=\"MM/dd/yyyy\")).asColumn(\"transactionDateFormatted\"\n) Wrong. Although this answer looks very tempting, it is actually incorrect Spark syntax. In Spark,\nthere is no method DataFrame.apply(). Spark has an apply() method that can be used on grouped\ndata - but this is irrelevant for this question, since we do not deal with grouped data here.\ntransactionsDf.withColumn(\"transactionDateFormatted\", from_unixtime(\"transactionDate\")) No.\nAlthough this is valid Spark syntax, the strings in column transactionDateFormatted would look like\nthis:\n2020-04-26 15:35:32, the default format specified in Spark for from_unixtime and not what is asked\nfor in the question.\nMore info: pyspark.sql.functions.from_unixtime - PySpark 3.1.1 documentation and\npyspark.sql.DataFrame.withColumnRenamed - PySpark 3.1.1 documentation Static notebook |\nDynamic notebook: See test 1"
    },
    {
        "question": "Which of the following describes a way for resizing a DataFrame from 16 to 8 partitions in the\nmost efficient way?",
        "options": [
            "A. Use operation DataFrame.repartition(8) to shuffle the DataFrame and reduce the number of\npartitions.",
            "B. Use operation DataFrame.coalesce(8) to fully shuffle the DataFrame and reduce the number of\npartitions.",
            "C. Use a narrow transformation to reduce the number of partitions.",
            "D. Use a wide transformation to reduce the number of partitions.\nUse operation DataFrame.coalesce(0.5) to halve the number of partitions in the DataFrame."
        ],
        "answer": [
            "C"
        ],
        "explanation": "Use a narrow transformation to reduce the number of partitions.\nCorrect! DataFrame.coalesce(n) is a narrow transformation, and in fact the most efficient way to\nresize the DataFrame of all options listed. One would run DataFrame.coalesce(8) to resize the\nDataFrame.\nUse operation DataFrame.coalesce(8) to fully shuffle the DataFrame and reduce the number of\npartitions.\nWrong. The coalesce operation avoids a full shuffle, but will shuffle data if needed. This answer is\nincorrect because it says \"fully shuffle\" - this is something the coalesce operation will not do. As a\ngeneral rule, it will reduce the number of partitions with the very least movement of data possible.\nMore info:\ndistributed computing - Spark - repartition() vs coalesce() - Stack Overflow Use operati\non DataFrame.coalesce(0.5) to halve the number of partitions in the DataFrame.\nIncorrect, since the num_partitions parameter needs to be an integer number defining the exact\nnumber of partitions desired after the operation. More info: pyspark.sql.DataFrame.coalesce -\nPySpark 3.1.2 documentation Use operation DataFrame.repartition(8) to shuffle the DataFrame and\nreduce the number of partitions.\nNo. The repartition operation will fully shuffle the DataFrame. This is not the most efficient way of\nreducing the number of partitions of all listed options.\nUse a wide transformation to reduce the number of partitions.\nNo. While possible via the DataFrame.repartition(n) command, the resulting full shuffle is not the\nmost efficient way of reducing the number of partitions."
    },
    {
        "question": "Which of the following describes characteristics of the Spark UI?",
        "options": [
            "A. Via the Spark UI, workloads can be manually distributed across executors.",
            "B. Via the Spark UI, stage execution speed can be modified.",
            "C. The Scheduler tab shows how jobs that are run in parallel by multiple users are distributed across\nthe cluster.",
            "D. There is a place in the Spark UI that shows the property spark.executor.memory.",
            "E. Some of the tabs in the Spark UI are named Jobs, Stages, Storage, DAGs, Executors, and SQL."
        ],
        "answer": [
            "D"
        ],
        "explanation": "There is a place in the Spark UI that shows the property spark.executor.memory.\nCorrect, you can see Spark properties such as spark.executor.memory in the Environment tab.\nSome of the tabs in the Spark UI are named Jobs, Stages, Storage, DAGs, Executors, and SQL.\nWrong - Jobs, Stages, Storage, Executors, and SQL are all tabs in the Spark UI. DAGs can be inspected\nin the\n\"Jobs\" tab in the job details or in the Stages or SQL tab, but are not a separate tab.\nVia the Spark UI, workloads can be manually distributed across distributors.\nNo, the Spark UI is meant for inspecting the inner workings of Spark which ultimately helps\nunderstand, debug, and optimize Spark transactions.\nVia the Spark UI, stage execution speed can be modified.\nNo, see above.\nThe Scheduler tab shows how jobs that are run in parallel by multiple users are distributed across the\ncluster.\nNo, there is no Scheduler tab."
    },
    {
        "question": "Which of the following DataFrame operators is never classified as a wide transformation?",
        "options": [
            "A. DataFrame.sort()",
            "B. DataFrame.aggregate()",
            "C. DataFrame.repartition()",
            "D. DataFrame.select()",
            "E. DataFrame.join()"
        ],
        "answer": [
            "D"
        ],
        "explanation": "As a general rule: After having gone through the practice tests you probably have a good feeling for\nwhat classifies as a wide and what classifies as a narrow transformation. If you are unsure, feel free\nto play around in Spark and display the explanation of the Spark execution plan via\nDataFrame.[operation, for example sort()].explain(). If repartitioning is involved, it would count as a\nwide transformation.\nDataFrame.select()\nCorrect! A wide transformation includes a shuffle, meaning that an input partition maps to one or\nmore output partitions. This is expensive and causes traffic across the cluster. With the select()\noperation however, you pass commands to Spark that tell Spark to perform an operation on a specific\nslice of any partition. For this, Spark does not need to exchange data across partitions, each partition\ncan be worked on independently. Thus, you do not cause a wide transformation.\nDataFrame.repartition()\n\n\nIncorrect. When you repartition a DataFrame, you redefine partition boundaries. Data will flow\nacross your cluster and end up in different partitions after the repartitioning is completed. This is\nknown as a shuffle and, in turn, is classified as a wide transformation.\nDataFrame.aggregate()\nNo. When you aggregate, you may compare and summarize data across partitions. In the process,\ndata are exchanged across the cluster, and newly formed output partitions depend on one or more\ninput partitions. This is a typical characteristic of a shuffle, meaning that the aggregate operation may\nclassify as a wide transformation.\nDataFrame.join()\nWrong. Joining multiple DataFrames usually means that large amounts of data are exchanged across\nthe cluster, as new partitions are formed. This is a shuffle and therefore DataFrame.join() counts as a\nwide transformation.\nDataFrame.sort()\nFalse. When sorting, Spark needs to compare many rows across all partitions to each other. This is an\nexpensive operation, since data is exchanged across the cluster and new partitions are formed as\ndata is reordered. This process classifies as a shuffle and, as a result, DataFrame.sort() counts as wide\ntransformation.\nMore info: Understanding Apache Spark Shuffle | Philipp Brunenberg"
    },
    {
        "question": "Which of the following describes slots?",
        "options": [
            "A. Slots are dynamically created and destroyed in accordance with an executor's workload.",
            "B. To optimize I/O performance, Spark stores data on disk in multiple slots.",
            "C. A Java Virtual Machine (JVM) working as an executor can be considered as a pool of slots for task\nexecution.",
            "D. A slot is always limited to a single core.\nSlots are the communication interface for executors and are used for receiving commands and\nsending results to the driver."
        ],
        "answer": [
            "C"
        ],
        "explanation": "Slots are the communication interface for executors and are used for receiving commands and\nsending results to the driver.\nWrong, executors communicate with the driver directly.\nSlots are dynamically created and destroyed in accordance with an executor's workload.\nNo, Spark does not actively create and destroy slots in accordance with the workload. Per executor,\nslots are made available in accordance with how many cores per executor (property\nspark.executor.cores) and how many CPUs per task (property spark.task.cpus) the Spark\nconfiguration calls for.\nA slot is always limited to a single core.\nNo, a slot can span multiple cores. If a task would require multiple cores, it would have to be\nexecuted through a slot that spans multiple cores.\nIn Spark documentation, \"core\" is often used interchangeably with \"thread\", although \"thread\" is the\nmore accurate word. A single physical core may be able to make multiple threads available. So, it is\nbetter to say that a slot can span multiple threads.\nTo optimize I/O performance, Spark stores data on disk in multiple slots.\nNo - Spark stores data on disk in multiple partitions, not slots.\nMore info: Spark Architecture | Distributed Systems Architecture"
    },
    {
        "question": "Which of the following code blocks returns a copy of DataFrame transactionsDf in which\ncolumn productId has been renamed to productNumber?",
        "options": [
            "A. transactionsDf.withColumnRenamed(\"productId\", \"productNumber\")",
            "B. transactionsDf.withColumn(\"productId\", \"productNumber\")",
            "C. transactionsDf.withColumnRenamed(\"productNumber\", \"productId\")",
            "D. transactionsDf.withColumnRenamed(col(productId), col(productNumber))",
            "E. transactionsDf.withColumnRenamed(productId, productNumber)"
        ],
        "answer": [
            "A"
        ],
        "explanation": "More info: pyspark.sql.DataFrame.withColumnRenamed - PySpark 3.1.2 documentation Static\nnotebook | Dynamic notebook: See test 2"
    },
    {
        "question": "In which order should the code blocks shown below be run in order to assign articlesDf a\nDataFrame that lists all items in column attributes ordered by the number of times these items occur,\nfrom most to least often?\nSample of DataFrame articlesDf:\n1.+------+-----------------------------+-------------------+\n2.|itemId|attributes |supplier |\n3.+------+-----------------------------+-------------------+\n4.|1 |[blue, winter, cozy] |Sports Company Inc.|\n5.|2 |[red, summer, fresh, cooling]|YetiX |\n6.|3 |[green, summer, travel] |Sports Company Inc.|\n7.+------+-----------------------------+-------------------+",
        "options": [
            "A. 1. articlesDf = articlesDf.groupby(\"col\")\n2. articlesDf = articlesDf.select(explode(col(\"attributes\")))\n3. articlesDf = articlesDf.orderBy(\"count\").select(\"col\")\n4. articlesDf = articlesDf.sort(\"count\",ascending=False).select(\"col\")\n5. articlesDf = articlesDf.groupby(\"col\").count()",
            "B. 4, 5",
            "C. 2, 5, 3",
            "D. 5, 2",
            "E. 2, 3, 4",
            "F. 2, 5, 4"
        ],
        "answer": [
            "E"
        ],
        "explanation": "Correct code block:\narticlesDf = articlesDf.select(explode(col('attributes')))\narticlesDf = articlesDf.groupby('col').count()\narticlesDf = articlesDf.sort('count',ascending=False).select('col')\nOutput of correct code block:\n+-------+\n| col|\n+-------+\n| summer|\n\n\n| winter|\n| blue|\n| cozy|\n| travel|\n| fresh|\n| red|\n|cooling|\n| green|\n+-------+\nStatic notebook | Dynamic notebook: See test 2"
    },
    {
        "question": "Which of the following code blocks concatenates rows of DataFrames transactionsDf and\ntransactionsNewDf, omitting any duplicates?",
        "options": [
            "A. transactionsDf.concat(transactionsNewDf).unique()",
            "B. transactionsDf.union(transactionsNewDf).distinct()",
            "C. spark.union(transactionsDf, transactionsNewDf).distinct()",
            "D. transactionsDf.join(transactionsNewDf, how=\"union\").distinct()",
            "E. transactionsDf.union(transactionsNewDf).unique()"
        ],
        "answer": [
            "B"
        ],
        "explanation": "DataFrame.unique() and DataFrame.concat() do not exist and union() is not a method of the\nSparkSession. In addition, there is no union option for the join method in the DataFrame.join()\nstatement.\nMore info: pyspark.sql.DataFrame.union - PySpark 3.1.2 documentation\nStatic notebook | Dynamic notebook: See test 2"
    },
    {
        "question": "The code block displayed below contains an error. The code block is intended to write\nDataFrame transactionsDf to disk as a parquet file in location /FileStore/transactions_split, using\ncolumn storeId as key for partitioning. Find the error.\nCode block:\ntransactionsDf.write.format(\"parquet\").partitionOn(\"storeId\").save(\"/FileStore/transactions_split\")A.",
        "options": [
            "A. The format(\"parquet\") expression is inappropriate to use here, \"parquet\" should be passed as first\nargument to the save() operator and \"/FileStore/transactions_split\" as the second argument.",
            "B. Partitioning data by storeId is possible with the partitionBy expression, so partitionOn should be\nreplaced by partitionBy.",
            "C. Partitioning data by storeId is possible with the bucketBy expression, so partitionOn should be\nreplaced by bucketBy.",
            "D. partitionOn(\"storeId\") should be called before the write operation.",
            "E. The format(\"parquet\") expression should be removed and instead, the information should be\nadded to the write expression like so: write(\"parquet\")."
        ],
        "answer": [
            "B"
        ],
        "explanation": "Correct code block:\ntransactionsDf.write.format(\"parquet\").partitionBy(\"storeId\").save(\"/FileStore/transactions_split\")\nMore info: partition by - Reading files which are written using PartitionBy or BucketBy in Spark - Stac\n\n\nk Overflow Static notebook | Dynamic notebook: See test 1"
    },
    {
        "question": "The code block displayed below contains an error. The code block should return a DataFrame\nin which column predErrorAdded contains the results of Python function add_2_if_geq_3 as applied\nto numeric and nullable column predError in DataFrame transactionsDf. Find the error.\nCode block:\n1.def add_2_if_geq_3(x):\n2. if x is None:\n3. return x\n4. elif x >= 3:\n5. return x+2\n6. return x\n7.\n8.add_2_if_geq_3_udf = udf(add_2_if_geq_3)\n9.\n10.transactionsDf.withColumnRenamed(\"predErrorAdded\", add_2_if_geq_3_udf(col(\"predError\")))",
        "options": [
            "A. The operator used to adding the column does not add column predErrorAdded to the DataFrame.",
            "B. Instead of col(\"predError\"), the actual DataFrame with the column needs to be passed, like so\ntransactionsDf.predError.",
            "C. The udf() method does not declare a return type.",
            "D. UDFs are only available through the SQL API, but not in the Python API as shown in the code block.",
            "E. The Python function is unable to handle null values, resulting in the code block crashing on\nexecution."
        ],
        "answer": [
            "A"
        ],
        "explanation": "Correct code block:\ndef add_2_if_geq_3(x):\nif x is None:\nreturn x\nelif x >= 3:\nreturn x+2\nreturn x\nadd_2_if_geq_3_udf = udf(add_2_if_geq_3)\ntransactionsDf.withColumn(\"predErrorAdded\", add_2_if_geq_3_udf(col(\"predError\"))).show()\nInstead of withColumnRenamed, you should use the withColumn operator.\nThe udf() method does not declare a return type.\nIt is fine that the udf() method does not declare a return type, this is not a required argument.\nHowever, the default return type is StringType. This may not be the ideal return type for numeric,\nnullable data - but the code will run without specified return type nevertheless.\nThe Python function is unable to handle null values, resulting in the code block crashing on execution.\nThe Python function is able to handle null values, this is what the statement if x is None does.\nUDFs are only available through the SQL API, but not in the Python API as shown in the code block.\nNo, they are available through the Python API. The code in the code block that concerns UDFs is\ncorrect.\nInstead of col(\"predError\"), the actual DataFrame with the column needs to be passed, like so\n\n\ntransactionsDf.predError.\nYou may choose to use the transactionsDf.predError syntax, but the col(\"predError\") syntax is fine."
    },
    {
        "question": "Which of the following code blocks returns a copy of DataFrame transactionsDf that only\nincludes columns transactionId, storeId, productId and f?\nSample of DataFrame transactionsDf:\n1.+-------------+---------+-----+-------+---------+----+\n2.|transactionId|predError|value|storeId|productId| f|\n3.+-------------+---------+-----+-------+---------+----+\n4.| 1| 3| 4| 25| 1|null|\n5.| 2| 6| 7| 2| 2|null|\n6.| 3| 3| null| 25| 3|null|\n7.+-------------+---------+-----+-------+---------+----+",
        "options": [
            "A. transactionsDf.drop(col(\"value\"), col(\"predError\"))",
            "B. transactionsDf.drop(\"predError\", \"value\")",
            "C. transactionsDf.drop(value, predError)",
            "D. transactionsDf.drop([\"predError\", \"value\"])",
            "E. transactionsDf.drop([col(\"predError\"), col(\"value\")])"
        ],
        "answer": [
            "B"
        ],
        "explanation": "Output of correct code block:\n+-------------+-------+---------+----+\n|transactionId|storeId|productId| f|\n+-------------+-------+---------+----+\n| 1| 25| 1|null|\n| 2| 2| 2|null|\n| 3| 25| 3|null|\n+-------------+-------+---------+----+\nTo solve this question, you should be fmailiar with the drop() API. The order of column names does\nnot matter\n- in this question the order differs in some answers just to confuse you. Also, drop() does not take a\nlist. The *cols operator in the documentation means that all arguments passed to drop() are\ninterpreted as column names.\nMore info: pyspark.sql.DataFrame.drop - PySpark 3.1.2 documentation\nStatic notebook | Dynamic notebook: See test 2"
    },
    {
        "question": "Which of the following code blocks returns a new DataFrame with only columns predError\nand values of every second row of DataFrame transactionsDf?\nEntire DataFrame transactionsDf:\n1.+-------------+---------+-----+-------+---------+----+\n2.|transactionId|predError|value|storeId|productId| f|\n3.+-------------+---------+-----+-------+---------+----+\n4.| 1| 3| 4| 25| 1|null|\n5.| 2| 6| 7| 2| 2|null|\n6.| 3| 3| null| 25| 3|null|\n7.| 4| null| null| 3| 2|null|\n\n\n8.| 5| null| null| null| 2|null|\n9.| 6| 3| 2| 25| 2|null|\n10.+-------------+---------+-----+-------+---------+----+",
        "options": [
            "A. transactionsDf.filter(col(\"transactionId\").isin([3,4,6])).select([predError, value])",
            "B. transactionsDf.select(col(\"transactionId\").isin([3,4,6]), \"predError\", \"value\")",
            "C. transactionsDf.filter(\"transactionId\" % 2 == 0).select(\"predError\", \"value\")",
            "D. transactionsDf.filter(col(\"transactionId\") % 2 == 0).select(\"predError\", \"value\")",
            "E. 1.transactionsDf.createOrReplaceTempView(\"transactionsDf\")\n2.spark.sql(\"FROM transactionsDf SELECT predError, value WHERE transactionId % 2 = 2\")",
            "F. transactionsDf.filter(col(transactionId).isin([3,4,6]))"
        ],
        "answer": [
            "D"
        ],
        "explanation": "Output of correct code block:\n+---------+-----+\n|predError|value|\n+---------+-----+\n| 6| 7|\n| null| null|\n| 3| 2|\n+---------+-----+\nThis is not an easy question to solve. You need to know that % stands for the module operator in\nPython. % 2 will return true for every second row. The statement using spark.sql gets it almost right\n(the modulo operator exists in SQL as well), but % 2 = 2 will never yield true, since modulo 2 is either\n0 or 1.\nOther answers are wrong since they are missing quotes around the column names and/or use filter or\nselect incorrectly.\nIf you have any doubts about SparkSQL and answer options 3 and 4 in this question, check out the\nnotebook I created as a response to a related student question.\nStatic notebook | Dynamic notebook: See test 1"
    },
    {
        "question": "Which of the following describes the difference between client and cluster execution modes?",
        "options": [
            "A. In cluster mode, the driver runs on the worker nodes, while the client mode runs the driver on the\nclient machine.",
            "B. In cluster mode, the driver runs on the edge node, while the client mode runs the driver in a\nworker node.",
            "C. In cluster mode, each node will launch its own executor, while in client mode, executors will\nexclusively run on the client machine.",
            "D. In client mode, the cluster manager runs on the same host as the driver, while in cluster mode, the\ncluster manager runs on a separate node.",
            "E. In cluster mode, the driver runs on the master node, while in client mode, the driver runs on a\nvirtual machine in the cloud."
        ],
        "answer": [
            "A"
        ],
        "explanation": "In cluster mode, the driver runs on the master node, while in client mode, the driver runs on a virtual\nmachine in the cloud.\n\n\nThis is wrong, since execution modes do not specify whether workloads are run in the cloud or on-\npremise.\nIn cluster mode, each node will launch its own executor, while in client mode, executors will\nexclusively run on the client machine.\nWrong, since in both cases executors run on worker nodes.\nIn cluster mode, the driver runs on the edge node, while the client mode runs the driver in a worker\nnode.\nWrong - in cluster mode, the driver runs on a worker node. In client mode, the driver runs on the\nclient machine.\nIn client mode, the cluster manager runs on the same host as the driver, while in cluster mode, the\ncluster manager runs on a separate node.\nNo. In both modes, the cluster manager is typically on a separate node - not on the same host as the\ndriver. It only runs on the same host as the driver in local execution mode.\nMore info: Learning Spark, 2nd Edition, Chapter 1, and Spark: The Definitive Guide, Chapter 15. ()"
    },
    {
        "question": "Which of the following code blocks returns all unique values across all values in columns\nvalue and productId in DataFrame transactionsDf in a one-column DataFrame?",
        "options": [
            "A. tranactionsDf.select('value').join(transactionsDf.select('productId'), col('value')==col('productId'),\n'outer')",
            "B. transactionsDf.select(col('value'), col('productId')).agg({'*': 'count'})",
            "C. transactionsDf.select('value', 'productId').distinct()",
            "D. transactionsDf.select('value').union(transactionsDf.select('productId')).distinct()",
            "E. transactionsDf.agg({'value': 'collect_set', 'productId': 'collect_set'})"
        ],
        "answer": [
            "D"
        ],
        "explanation": "transactionsDf.select('value').union(transactionsDf.select('productId')).distinct() Correct. This code\nblock uses a common pattern for finding the unique values across multiple columns: union and\ndistinct. In fact, it is so common that it is even mentioned in the Spark documentation for the union\ncommand (link below).\ntransactionsDf.select('value', 'productId').distinct()\nWrong. This code block returns unique rows, but not unique values.\ntransactionsDf.agg({'value': 'collect_set', 'productId': 'collect_set'}) Incorrect. This code block will\noutput a one-row, two-column DataFrame where each cell has an array of unique values in the\nrespective column (even omitting any nulls).\ntransactionsDf.select(col('value'), col('productId')).agg({'*': 'count'}) No. This command will count the\nnumber of rows, but will not return unique values.\ntransactionsDf.select('value').join(transactionsDf.select('productId'), col('value')==col('productId'),\n'outer') Wrong. This command will perform an outer join of the value and productId columns. As\nsuch, it will return a two-column DataFrame. If you picked this answer, it might be a good idea for\nyou to read up on the difference between union and join, a link is posted below.\nMore info: pyspark.sql.DataFrame.union - PySpark 3.1.2 documentation, sql - What is the differenc\ne between JOIN and UNION? - Stack Overflow Static notebook | Dynamic notebook: See test 3"
    },
    {
        "question": "Which of the following is a viable way to improve Spark's performance when dealing with\nlarge amounts of data, given that there is only a single application running on the cluster?",
        "options": [
            "A. Increase values for the properties spark.default.parallelism and spark.sql.shuffle.partitions",
            "B. Decrease values for the properties spark.default.parallelism and spark.sql.partitions",
            "C. Increase values for the properties spark.sql.parallelism and spark.sql.partitions",
            "D. Increase values for the properties spark.sql.parallelism and spark.sql.shuffle.partitions",
            "E. Increase values for the properties spark.dynamicAllocation.maxExecutors,\nspark.default.parallelism, and spark.sql.shuffle.partitions"
        ],
        "answer": [
            "A"
        ],
        "explanation": "Decrease values for the properties spark.default.parallelism and spark.sql.partitions No, these values\nneed to be increased.\nIncrease values for the properties spark.sql.parallelism and spark.sql.partitions Wrong, there is no\nproperty spark.sql.parallelism.\nIncrease values for the properties spark.sql.parallelism and spark.sql.shuffle.partitions See above.\nIncrease values for the properties spark.dynamicAllocation.maxExecutors, spark.default.parallelism,\nand spark.sql.shuffle.partitions The property spark.dynamicAllocation.maxExecutors is only in effect\nif dynamic allocation is enabled, using the spark.dynamicAllocation.enabled property. It is disabled by\ndefault. Dynamic allocation can be useful when to run multiple applications on the same cluster in\nparallel. However, in this case there is only a single application running on the cluster, so enabling\ndynamic allocation would not yield a performance benefit.\nMore info: Practical Spark Tips For Data Scientists | Experfy.com and Basics of Apache Spark\nConfiguration Settings | by Halil Ertan | Towards Data Science (https://bit.ly/3gA0A6w ,\nhttps://bit.ly/2QxhNTr)"
    },
    {
        "question": "Which of the following code blocks performs an inner join between DataFrame itemsDf and\nDataFrame transactionsDf, using columns itemId and transactionId as join keys, respectively?",
        "options": [
            "A. itemsDf.join(transactionsDf, \"inner\", itemsDf.itemId == transactionsDf.transactionId)",
            "B. itemsDf.join(transactionsDf, itemId == transactionId)",
            "C. itemsDf.join(transactionsDf, itemsDf.itemId == transactionsDf.transactionId, \"inner\")",
            "D. itemsDf.join(transactionsDf, \"itemsDf.itemId == transactionsDf.transactionId\", \"inner\")",
            "E. itemsDf.join(transactionsDf, col(itemsDf.itemId) == col(transactionsDf.transactionId))"
        ],
        "answer": [
            "C"
        ],
        "explanation": "More info: pyspark.sql.DataFrame.join - PySpark 3.1.2 documentation\nStatic notebook | Dynamic notebook: See test 2"
    },
    {
        "question": "The code block displayed below contains an error. The code block should save DataFrame transactionsDf at path path as a parquet file, appending to any existing parquet file. Find the error.\n\nCode block:\ntransactionsDf.format(\"parquet\").option(\"mode\", \"append\").save(path)",
        "options": [
            "A. The code block is missing a reference to the DataFrameWriter.",
            "B. save() is evaluated lazily and needs to be followed by an action.",
            "C. The mode option should be omitted so that the command uses the default mode.",
            "D. The code block is missing a bucketBy command that takes care of partitions.",
            "E. Given that the DataFrame should be saved as parquet file, path is being passed to the wrong method."
        ],
        "answer": [
            "A"
        ],
        "explanation": "Correct code block:\ntransactionsDf.write.format(\"parquet\").option(\"mode\", \"append\").save(path)"
    },
    {
        "question": "In which order should the code blocks shown below be run in order to create a table of all\nvalues in column attributes next to the respective values in column supplier in DataFrame itemsDf?\n1. itemsDf.createOrReplaceView(\"itemsDf\")\n2. spark.sql(\"FROM itemsDf SELECT 'supplier', explode('Attributes')\")\n3. spark.sql(\"FROM itemsDf SELECT supplier, explode(attributes)\")\n4. itemsDf.createOrReplaceTempView(\"itemsDf\")",
        "options": [
            "A. 4, 3",
            "B. 1, 3",
            "C. 2",
            "D. 4, 2",
            "E. 1, 2"
        ],
        "answer": [
            "A"
        ],
        "explanation": "Static notebook | Dynamic notebook: See test 1"
    },
    {
        "question": "In which order should the code blocks shown below be run in order to create a DataFrame\nthat shows the mean of column predError of DataFrame transactionsDf per column storeId and\nproductId, where productId should be either 2 or 3 and the returned DataFrame should be sorted in\nascending order by column storeId, leaving out any nulls in that column?\nDataFrame transactionsDf:\n1.+-------------+---------+-----+-------+---------+----+\n2.|transactionId|predError|value|storeId|productId| f|\n3.+-------------+---------+-----+-------+---------+----+\n4.| 1| 3| 4| 25| 1|null|\n5.| 2| 6| 7| 2| 2|null|\n6.| 3| 3| null| 25| 3|null|\n7.| 4| null| null| 3| 2|null|\n8.| 5| null| null| null| 2|null|\n9.| 6| 3| 2| 25| 2|null|\n10.+-------------+---------+-----+-------+---------+----+\n1. .mean(\"predError\")\n2. .groupBy(\"storeId\")\n3. .orderBy(\"storeId\")\n4. transactionsDf.filter(transactionsDf.storeId.isNotNull())\n5. .pivot(\"productId\", [2, 3])",
        "options": [
            "A. 4, 5, 2, 3, 1",
            "B. 4, 2, 1",
            "C. 4, 1, 5, 2, 3",
            "D. 4, 2, 5, 1, 3",
            "E. 4, 3, 2, 5, 1"
        ],
        "answer": [
            "D"
        ],
        "explanation": "Correct code block:\ntransactionsDf.filter(transactionsDf.storeId.isNotNull()).groupBy(\"storeId\").pivot(\"productId\", [2,\n3]).mean(\"predError\").orderBy(\"storeId\")\nOutput of correct code block:\n+-------+----+----+\n|storeId| 2| 3|\n+-------+----+----+\n| 2| 6.0|null|\n| 3|null|null|\n| 25| 3.0| 3.0|\n+-------+----+----+\nThis question is quite convoluted and requires you to think hard about the correct order of\noperations.\nThe pivot method also makes an appearance - a method that you may not know all that much about\n(yet).\nAt the first position in all answers is code block 4, so the question is essentially just about the\nordering of the remaining 4 code blocks.\nThe question states that the returned DataFrame should be sorted by column storeId. So, it should\nmake sense to have code block 3 which includes the orderBy operator at the very end of the code\nblock. This leaves you with only two answer options.\nNow, it is useful to know more about the context of pivot in PySpark. A common pattern is groupBy,\npivot, and then another aggregating function, like mean. In the documentation linked below you can\nsee that pivot is a method of pyspark.sql.GroupedData - meaning that before pivoting, you have to\nuse groupBy. The only answer option matching this requirement is the one in which code block 2\n(which includes groupBy) is stated before code block 5 (which includes pivot).\nMore info: pyspark.sql.GroupedData.pivot - PySpark 3.1.2 documentation\nStatic notebook | Dynamic notebook: See test 3"
    },
    {
        "question": "The code block displayed below contains an error. The code block should arrange the rows of\nDataFrame transactionsDf using information from two columns in an ordered fashion, arranging first\nby column value, showing smaller numbers at the top and greater numbers at the bottom, and then\nby column predError, for which all values should be arranged in the inverse way of the order of items\nin column value. Find the error.\nCode block:\ntransactionsDf.orderBy('value', asc_nulls_first(col('predError')))",
        "options": [
            "A. Two orderBy statements with calls to the individual columns should be chained, instead of having\nboth columns in one orderBy statement.",
            "B. Column value should be wrapped by the col() operator.",
            "C. Column predError should be sorted in a descending way, putting nulls last.",
            "D. Column predError should be sorted by desc_nulls_first() instead.",
            "E. Instead of orderBy, sort should be used."
        ],
        "answer": [
            "C"
        ],
        "explanation": "Correct code block:\ntransactionsDf.orderBy('value', desc_nulls_last('predError'))\nColumn predError should be sorted in a descending way, putting nulls last.\nCorrect! By default, Spark sorts ascending, putting nulls first. So, the inverse sort of the default sort is\nindeed desc_nulls_last.\nInstead of orderBy, sort should be used.\nNo. DataFrame.sort() orders data per partition, it does not guarantee a global order. This is why\norderBy is the more appropriate operator here.\nColumn value should be wrapped by the col() operator.\nIncorrect. DataFrame.sort() accepts both string and Column objects.\nColumn predError should be sorted by desc_nulls_first() instead.\nWrong. Since Spark's default sort order matches asc_nulls_first(), nulls would have to come last when\ninverted.\nTwo orderBy statements with calls to the individual columns should be chained, instead of having\nboth columns in one orderBy statement.\nNo, this would just sort the DataFrame by the very last column, but would not take information from\nboth columns into account, as noted in the question.\nMore info: pyspark.sql.DataFrame.orderBy - PySpark 3.1.2 documentation,\npyspark.sql.functions.desc_nulls_last - PySpark 3.1.2 documentation, sort() vs orderBy() in Spark |\nTowards Data Science Static notebook | Dynamic notebook: See test 3"
    },
    {
        "question": "Which of the following code blocks creates a new 6-column DataFrame by appending the\nrows of the\n6-column DataFrame yesterdayTransactionsDf to the rows of the 6-column DataFrame\ntodayTransactionsDf, ignoring that both DataFrames have different column names?",
        "options": [
            "A. union(todayTransactionsDf, yesterdayTransactionsDf)",
            "B. todayTransactionsDf.unionByName(yesterdayTransactionsDf, allowMissingColumns=True)",
            "C. todayTransactionsDf.unionByName(yesterdayTransactionsDf)",
            "D. todayTransactionsDf.concat(yesterdayTransactionsDf)",
            "E. todayTransactionsDf.union(yesterdayTransactionsDf)"
        ],
        "answer": [
            "E"
        ],
        "explanation": "todayTransactionsDf.union(yesterdayTransactionsDf)\nCorrect. The union command appends rows of yesterdayTransactionsDf to the rows of\ntodayTransactionsDf, ignoring that both DataFrames have different column names. The resulting\nDataFrame will have the column names of DataFrame todayTransactionsDf.\ntodayTransactionsDf.unionByName(yesterdayTransactionsDf)\nNo. unionByName specifically tries to match columns in the two DataFrames by name and only\nappends values in columns with identical names across the two DataFrames. In the form presented\nabove, the command is a great fit for joining DataFrames that have exactly the same columns, but in\na different order. In this case though, the command will fail because the two DataFrames have\ndifferent columns.\ntodayTransactionsDf.unionByName(yesterdayTransactionsDf, allowMissingColumns=True) No. The\nunionByName command is described in the previous explanation. However, with the\nallowMissingColumns argument set to True, it is no longer an issue that the two DataFrames have\ndifferent column names. Any columns that do not have a match in the other DataFrame will be filled\n\n\nwith null where there is no value. In the case at hand, the resulting DataFrame will have 7 or more\ncolumns though, so it this command is not the right answer.\nunion(todayTransactionsDf, yesterdayTransactionsDf)\nNo, there is no union method in pyspark.sql.functions.\ntodayTransactionsDf.concat(yesterdayTransactionsDf)\nWrong, the DataFrame class does not have a concat method.\nMore info: pyspark.sql.DataFrame.union - PySpark 3.1.2 documentation,\npyspark.sql.DataFrame.unionByName - PySpark 3.1.2 documentation\nStatic notebook | Dynamic notebook: See test 3"
    },
    {
        "question": "The code block displayed below contains an error. The code block should use Python\nmethod find_most_freq_letter to find the letter present most in column itemName of DataFrame\nitemsDf and return it in a new column most_frequent_letter. Find the error.\nCode block:\n1. find_most_freq_letter_udf = udf(find_most_freq_letter)\n2. itemsDf.withColumn(\"most_frequent_letter\", find_most_freq_letter(\"itemName\"))",
        "options": [
            "A. Spark is not using the UDF method correctly.",
            "B. The UDF method is not registered correctly, since the return type is missing.",
            "C. The \"itemName\" expression should be wrapped in col().",
            "D. UDFs do not exist in PySpark.",
            "E. Spark is not adding a column."
        ],
        "answer": [
            "A"
        ],
        "explanation": "Correct code block:\nfind_most_freq_letter_udf = udf(find_most_frequent_letter)\nitemsDf.withColumn(\"most_frequent_letter\", find_most_freq_letter_udf(\"itemName\")) Spark should\nuse the previously registered find_most_freq_letter_udf method here - but it is not doing that in the\noriginal codeblock. There, it just uses the non-UDF version of the Python method.\nNote that typically, we would have to specify a return type for udf(). Except in this case, since the\ndefault return type for udf() is a string which is what we are expecting here. If we wanted to return\nan integer variable instead, we would have to register the Python function as UDF using\nfind_most_freq_letter_udf = udf(find_most_freq_letter, IntegerType()).\nMore info: pyspark.sql.functions.udf - PySpark 3.1.1 documentation"
    },
    {
        "question": "The code block shown below should return a column that indicates through boolean\nvariables whether rows in DataFrame transactionsDf have values greater or equal to 20 and smaller\nor equal to\n30 in column storeId and have the value 2 in column productId. Choose the answer that correctly fills\nthe blanks in the code block to accomplish this.\ntransactionsDf.__1__((__2__.__3__) __4__ (__5__))",
        "options": [
            "A. 1. select\n2. col(\"storeId\")\n3. between(20, 30)\n4. and\n5. col(\"productId\")==2",
            "B. 1. where\n2. col(\"storeId\")\n3. geq(20).leq(30)\n4. &\n5. col(\"productId\")==2",
            "C. 1. select\n2. \"storeId\"\n3. between(20, 30)\n4. &&\n5. col(\"productId\")==2",
            "D. 1. select\n2. col(\"storeId\")\n3. between(20, 30)\n4. &&\n5. col(\"productId\")=2",
            "E. 1. select\n2. col(\"storeId\")\n3. between(20, 30)\n4. &\n5. col(\"productId\")==2"
        ],
        "answer": [
            "D"
        ],
        "explanation": "Correct code block:\ntransactionsDf.select((col(\"storeId\").between(20, 30)) & (col(\"productId\")==2)) Although this\nquestion may make you think that it asks for a filter or where statement, it does not. It asks explicity\nto return a column with booleans - this should point you to the select statement.\nAnother trick here is the rarely used between() method. It exists and resolves to ((storeId >= 20) AND\n(storeId\n<= 30)) in SQL. geq() and leq() do not exist.\nAnother riddle here is how to chain the two conditions. The only valid answer here is &. Operators\nlike && or and are not valid. Other boolean operators that would be valid in Spark are | and.\nStatic notebook | Dynamic notebook: See test 1"
    },
    {
        "question": "The code block displayed below contains an error. The code block should return all rows of\nDataFrame transactionsDf, but including only columns storeId and predError. Find the error.\nCode block:\nspark.collect(transactionsDf.select(\"storeId\", \"predError\"))",
        "options": [
            "A. Instead of select, DataFrame transactionsDf needs to be filtered using the filter operator.",
            "B. Columns storeId and predError need to be represented as a Python list, so they need to be\nwrapped in brackets ([]).",
            "C. The take method should be used instead of the collect method.",
            "D. Instead of collect, collectAsRows needs to be called.",
            "E. The collect method is not a method of the SparkSession object."
        ],
        "answer": [
            "E"
        ],
        "explanation": "Correct code block:\ntransactionsDf.select(\"storeId\", \"predError\").collect()\ncollect() is a method of the DataFrame object.\nMore info: pyspark.sql.DataFrame.collect - PySpark 3.1.2 documentation\nStatic notebook | Dynamic notebook: See test 2"
    },
    {
        "question": "Which of the following code blocks returns a DataFrame that matches the multi-column\nDataFrame itemsDf, except that integer column itemId has been converted into a string column?",
        "options": [
            "A. itemsDf.withColumn(\"itemId\", convert(\"itemId\", \"string\"))",
            "B. itemsDf.withColumn(\"itemId\", col(\"itemId\").cast(\"string\"))",
            "C. itemsDf.select(cast(\"itemId\", \"string\"))",
            "D. itemsDf.withColumn(\"itemId\", col(\"itemId\").convert(\"string\"))",
            "E. spark.cast(itemsDf, \"itemId\", \"string\")"
        ],
        "answer": [
            "B"
        ],
        "explanation": "itemsDf.withColumn(\"itemId\", col(\"itemId\").cast(\"string\"))\nCorrect. You can convert the data type of a column using the cast method of the Column class. Also\nnote that you will have to use the withColumn method on itemsDf for replacing the existing itemId\ncolumn with the new version that contains strings.\nitemsDf.withColumn(\"itemId\", col(\"itemId\").convert(\"string\"))\nIncorrect. The Column object that col(\"itemId\") returns does not have a convert method.\nitemsDf.withColumn(\"itemId\", convert(\"itemId\", \"string\"))\nWrong. Spark's spark.sql.functions module does not have a convert method. The question is trying to\nmislead you by using the word \"converted\". Type conversion is also called \"type casting\". This may\nhelp you remember to look for a cast method instead of a convert method (see correct answer).\nitemsDf.select(astype(\"itemId\", \"string\"))\nFalse. While astype is a method of Column (and an alias of Column.cast), it is not a method of\npyspark.sql.functions (what the code block implies). In addition, the question asks to return a full\nDataFrame that matches the multi-column DataFrame itemsDf. Selecting just one column from\nitemsDf as in the code block would just return a single-column DataFrame.\nspark.cast(itemsDf, \"itemId\", \"string\")\nNo, the Spark session (called by spark) does not have a cast method. You can find a list of all methods\navailable for the Spark session linked in the documentation below.\nMore info:\n- pyspark.sql.Column.cast - PySpark 3.1.2 documentation\n- pyspark.sql.Column.astype - PySpark 3.1.2 documentation\n- pyspark.sql.SparkSession - PySpark 3.1.2 documentation\nStatic notebook | Dynamic notebook: See test 3"
    },
    {
        "question": "Which of the following statements about executors is correct?",
        "options": [
            "A. Executors are launched by the driver.",
            "B. Executors stop upon application completion by default.",
            "C. Each node hosts a single executor.",
            "D. Executors store data in memory only.",
            "E. An executor can serve multiple applications."
        ],
        "answer": [
            "B"
        ],
        "explanation": "Executors stop upon application completion by default.\nCorrect. Executors only persist during the lifetime of an application.\nA notable exception to that is when Dynamic Resource Allocation is enabled (which it is not by\ndefault). With Dynamic Resource Allocation enabled, executors are terminated when they are idle,\nindependent of whether the application has been completed or not.\nAn executor can serve multiple applications.\nWrong. An executor is always specific to the application. It is terminated when the application\ncompletes (exception see above).\nEach node hosts a single executor.\nNo. Each node can host one or more executors.\nExecutors store data in memory only.\nNo. Executors can store data in memory or on disk.\nExecutors are launched by the driver.\nIncorrect. Executors are launched by the cluster manager on behalf of the driver.\nMore info: Job Scheduling - Spark 3.1.2 Documentation, How Applications are Executed on a Spark\nCluster | Anatomy of a Spark Application | InformIT, and Spark Jargon for Starters. This blog is to\nclear some of the... | by Mageswaran D | Medium"
    },
    {
        "question": "The code block displayed below contains an error. The code block should produce a\nDataFrame with color as the only column and three rows with color values of red, blue, and green,\nrespectively.\nFind the error.\nCode block:\n1.spark.createDataFrame([(\"red\",), (\"blue\",), (\"green\",)], \"color\")",
        "options": [
            "A. Instead of calling spark.createDataFrame, just DataFrame should be called.",
            "B. The commas in the tuples with the colors should be eliminated.",
            "C. The colors red, blue, and green should be expressed as a simple Python list, and not a list of tuples\n.",
            "D. Instead of color, a data type should be specified.",
            "E. The \"color\" expression needs to be wrapped in brackets, so it reads [\"color\"]."
        ],
        "answer": [
            "E"
        ],
        "explanation": "Correct code block:\nspark.createDataFrame([(\"red\",), (\"blue\",), (\"green\",)], [\"color\"])\nThe createDataFrame syntax is not exactly straightforward, but luckily the documentation (linked\nbelow) provides several examples on how to use it. It also shows an example very similar to the code\nblock presented here which should help you answer this question correctly.\nMore info: pyspark.sql.SparkSession.createDataFrame - PySpark 3.1.2 documentation Static notebook\n| Dynamic notebook: See test 2"
    },
    {
        "question": "Which of the following code blocks returns a DataFrame where columns predError and\nproductId are removed from DataFrame transactionsDf?\n\n\nSample of DataFrame transactionsDf:\n1.+-------------+---------+-----+-------+---------+----+\n2.|transactionId|predError|value|storeId|productId|f |\n3.+-------------+---------+-----+-------+---------+----+\n4.|1 |3 |4 |25 |1 |null|\n5.|2 |6 |7 |2 |2 |null|\n6.|3 |3 |null |25 |3 |null|\n7.+-------------+---------+-----+-------+---------+----+",
        "options": [
            "A. transactionsDf.withColumnRemoved(\"predError\", \"productId\")",
            "B. transactionsDf.drop([\"predError\", \"productId\", \"associateId\"])",
            "C. transactionsDf.drop(\"predError\", \"productId\", \"associateId\")",
            "D. transactionsDf.dropColumns(\"predError\", \"productId\", \"associateId\")",
            "E. transactionsDf.drop(col(\"predError\", \"productId\"))"
        ],
        "answer": [
            "D"
        ],
        "explanation": "The key here is to understand that columns that are passed to DataFrame.drop() are ignored if they\ndo not exist in the DataFrame. So, passing column name associateId to transactionsDf.drop() does\nnot have any effect.\nPassing a list to transactionsDf.drop() is not valid. The documentation (link below) shows the call\nstructure as DataFrame.drop(*cols). The * means that all arguments that are passed to\nDataFrame.drop() are read as columns. However, since a list of columns, for example [\"predError\",\n\"productId\", \"associateId\"] is not a column, Spark will run into an error.\nMore info: pyspark.sql.DataFrame.drop - PySpark 3.1.1 documentation\nStatic notebook | Dynamic notebook: See test 1"
    },
    {
        "question": "Which of the following code blocks performs a join in which the small DataFrame\ntransactionsDf is sent to all executors where it is joined with DataFrame itemsDf on columns storeId\nand itemId, respectively?",
        "options": [
            "A. itemsDf.join(transactionsDf, itemsDf.itemId == transactionsDf.storeId, \"right_outer\")",
            "B. itemsDf.join(transactionsDf, itemsDf.itemId == transactionsDf.storeId, \"broadcast\")",
            "C. itemsDf.merge(transactionsDf, \"itemsDf.itemId == transactionsDf.storeId\", \"broadcast\")",
            "D. itemsDf.join(broadcast(transactionsDf), itemsDf.itemId == transactionsDf.storeId)",
            "E. itemsDf.join(transactionsDf, broadcast(itemsDf.itemId == transactionsDf.storeId))"
        ],
        "answer": [
            "D"
        ],
        "explanation": "The issue with all answers that have \"broadcast\" as very last argument is that \"broadcast\" is not a\nvalid join type. While the entry with \"right_outer\" is a valid statement, it is not a broadcast join. The\nitem where broadcast() is wrapped around the equality condition is not valid code in Spark.\nbroadcast() needs to be wrapped around the name of the small DataFrame that should be broadcast.\nMore info: Learning Spark, 2nd Edition, Chapter 7\nStatic notebook | Dynamic notebook: See test 1\ntion and explanation?"
    },
    {
        "question": "Which of the following code blocks returns a new DataFrame with the same columns as\nDataFrame transactionsDf, except for columns predError and value which should be removed?",
        "options": [
            "A. transactionsDf.drop([\"predError\", \"value\"])",
            "B. transactionsDf.drop(\"predError\", \"value\")",
            "C. transactionsDf.drop(col(\"predError\"), col(\"value\"))",
            "D. transactionsDf.drop(predError, value)",
            "E. transactionsDf.drop(\"predError & value\")"
        ],
        "answer": [
            "B"
        ],
        "explanation": "More info: pyspark.sql.DataFrame.drop - PySpark 3.1.2 documentation\nStatic notebook | Dynamic notebook: See test 2"
    },
    {
        "question": "Which of the following code blocks uses a schema fileSchema to read a parquet file at\nlocation filePath into a DataFrame?",
        "options": [
            "A. spark.read.schema(fileSchema).format(\"parquet\").load(filePath)",
            "B. spark.read.schema(\"fileSchema\").format(\"parquet\").load(filePath)",
            "C. spark.read().schema(fileSchema).parquet(filePath)",
            "D. spark.read().schema(fileSchema).format(parquet).load(filePath)",
            "E. spark.read.schema(fileSchema).open(filePath)"
        ],
        "answer": [
            "A"
        ],
        "explanation": "Pay attention here to which variables are quoted. fileSchema is a variable and thus should not be in\nquotes.\nparquet is not a variable and therefore should be in quotes.\nSparkSession.read (here referenced as spark.read) returns a DataFrameReader which all subsequent\ncalls reference - the DataFrameReader is not callable, so you should not use parentheses here.\nFinally, there is no open method in PySpark. The method name is load.\nStatic notebook | Dynamic notebook: See test 1"
    },
    {
        "question": "The code block displayed below contains an error. The code block should return the average\nof rows in column value grouped by unique storeId. Find the error.\nCode block:\ntransactionsDf.agg(\"storeId\").avg(\"value\")",
        "options": [
            "A. Instead of avg(\"value\"), avg(col(\"value\")) should be used.",
            "B. The avg(\"value\") should be specified as a second argument to agg() instead of being appended to\nit.",
            "C. All column names should be wrapped in col() operators.",
            "D. agg should be replaced by groupBy.",
            "E. \"storeId\" and \"value\" should be swapped."
        ],
        "answer": [
            "D"
        ],
        "explanation": "Static notebook | Dynamic notebook: See test 1\n(https://flrs.github.io/spark_practice_tests_code/#1/30.html ,\nhttps://bit.ly/sparkpracticeexams_import_instructions)"
    },
    {
        "question": "The code block shown below should write DataFrame transactionsDf to disk at path csvPath\n\n\nas a single CSV file, using tabs (\\t characters) as separators between columns, expressing missing\nvalues as string n/a, and omitting a header row with column names. Choose the answer that correctly\nfills the blanks in the code block to accomplish this.\ntransactionsDf.__1__.write.__2__(__3__, \" \").__4__.__5__(csvPath)",
        "options": [
            "A. 1. coalesce(1)\n2. option\n3. \"sep\"\n4. option(\"header\", True)\n5. path",
            "B. 1. coalesce(1)\n2. option\n3. \"colsep\"\n4. option(\"nullValue\", \"n/a\")\n5. path",
            "C. 1. repartition(1)\n2. option\n3. \"sep\"\n4. option(\"nullValue\", \"n/a\")\n5. csv",
            "D. 1. csv\n2. option\n3. \"sep\"\n4. option(\"emptyValue\", \"n/a\")\n5. path\n* 1. repartition(1)\n2. mode\n3. \"sep\"\n4. mode(\"nullValue\", \"n/a\")\n5. csv"
        ],
        "answer": [
            "C"
        ],
        "explanation": "Correct code block:\ntransactionsDf.repartition(1).write.option(\"sep\", \"\\t\").option(\"nullValue\", \"n/a\").csv(csvPath) It is\nimportant here to understand that the question specifically asks for writing the DataFrame as a single\nCSV file. This should trigger you to think about partitions. By default, every partition is written as a\nseparate file, so you need to include repatition(1) into your call. coalesce(1) works here, too!\nSecondly, the question is very much an invitation to search through the parameters in the Spark\ndocumentation that work with DataFrameWriter.csv (link below). You will also need to know that you\nneed an option() statement to apply these parameters.\nThe final concern is about the general call structure. Once you have called accessed write of your\nDataFrame, options follow and then you write the DataFrame with csv. Instead of csv(csvPath), you\ncould also use save(csvPath, format='csv') here.\nMore info: pyspark.sql.DataFrameWriter.csv - PySpark 3.1.1 documentation Static notebook |\nDynamic notebook: See test 1"
    },
    {
        "question": "Which of the following code blocks reads the parquet file stored at filePath into DataFrame\nitemsDf, using a valid schema for the sample of itemsDf shown below?\nSample of itemsDf:\n1.+------+-----------------------------+-------------------+\n2.|itemId|attributes |supplier |\n3.+------+-----------------------------+-------------------+\n4.|1 |[blue, winter, cozy] |Sports Company Inc.|\n5.|2 |[red, summer, fresh, cooling]|YetiX |\n6.|3 |[green, summer, travel] |Sports Company Inc.|\n7.+------+-----------------------------+-------------------+",
        "options": [
            "A. 1.itemsDfSchema = StructType([\n2. StructField(\"itemId\", IntegerType()),\n3. StructField(\"attributes\", StringType()),\n4. StructField(\"supplier\", StringType())])\n5.\n6.itemsDf = spark.read.schema(itemsDfSchema).parquet(filePath)",
            "B. 1.itemsDfSchema = StructType([\n2. StructField(\"itemId\", IntegerType),\n3. StructField(\"attributes\", ArrayType(StringType)),\n4. StructField(\"supplier\", StringType)])\n5.\n6.itemsDf = spark.read.schema(itemsDfSchema).parquet(filePath)",
            "C. 1.itemsDf = spark.read.schema('itemId integer, attributes <string>, supplier\nstring').parquet(filePath)",
            "D. 1.itemsDfSchema = StructType([\n2. StructField(\"itemId\", IntegerType()),\n3. StructField(\"attributes\", ArrayType(StringType())),\n4. StructField(\"supplier\", StringType())])\n5.\n6.itemsDf = spark.read.schema(itemsDfSchema).parquet(filePath)",
            "E. 1.itemsDfSchema = StructType([\n2. StructField(\"itemId\", IntegerType()),\n3. StructField(\"attributes\", ArrayType([StringType()])),\n4. StructField(\"supplier\", StringType())])\n5.\n6.itemsDf = spark.read(schema=itemsDfSchema).parquet(filePath)"
        ],
        "answer": [
            "D"
        ],
        "explanation": "The challenge in this question comes from there being an array variable in the schema. In addition,\nyou should know how to pass a schema to the DataFrameReader that is invoked by spark.read.\nThe correct way to define an array of strings in a schema is through ArrayType(StringType()). A\nschema can be passed to the DataFrameReader by simply appending schema(structType) to the\nread() operator. Alternatively, you can also define a schema as a string. For example, for the schema\nof itemsDf, the following string would make sense: itemId integer, attributes array<string>, supplier\nstring.\n\n\nA thing to keep in mind is that in schema definitions, you always need to instantiate the types, like so:\nStringType(). Just using StringType does not work in pySpark and will fail.\nAnother concern with schemas is whether columns should be nullable, so allowed to have null values.\nIn the case at hand, this is not a concern however, since the question just asks for a\n\"valid\"\nschema. Both non-nullable and nullable column schemas would be valid here, since no null value\nappears in the DataFrame sample.\nMore info: Learning Spark, 2nd Edition, Chapter 3\nStatic notebook | Dynamic notebook: See test 3"
    },
    {
        "question": "Which of the elements in the labeled panels represent the operation performed for broadcast variables?\n\nThe diagram shows 5 labeled panels illustrating different communication patterns between a driver node (top) and executor nodes (bottom):\nPanel 1: Bi-directional arrows between driver and each executor (two-way communication)\nPanel 2: Driver sends to executors, executors also share with each other (torrent-like distribution)\nPanel 3: Driver sends to all executors in a star pattern (driver is the single source/bottleneck)\nPanel 4: All executors send data up to the driver (reverse direction, like accumulators)\nPanel 5: Bi-directional arrows between driver and executors, with inter-executor communication",
        "options": [
            "A. 2, 5",
            "B. 3",
            "C. 2, 3",
            "D. 1, 2",
            "E. 1, 3, 4"
        ],
        "answer": [
            "C"
        ],
        "explanation": "2,3\nCorrect! Both panels 2 and 3 represent the operation performed for broadcast variables. While a\nbroadcast operation may look like panel 3, with the driver being the bottleneck, it most probably\nlooks like panel 2.\nThis is because the torrent protocol sits behind Spark's broadcast implementation. In the torrent\nprotocol, each executor will try to fetch missing broadcast variables from the driver or other nodes,\npreventing the driver from being the bottleneck.\n1,2\nWrong. While panel 2 may represent broadcasting, panel 1 shows bi-directional communication\nwhich does not occur in broadcast operations.\n3\nNo. While broadcasting may materialize like shown in panel 3, its use of the torrent protocol also\nenables communciation as shown in panel 2 (see first explanation).\n1,3,4\nNo. While panel 2 shows broadcasting, panel 1 shows bi-directional communication - not a\ncharacteristic of broadcasting. Panel 4 shows uni-directional communication, but in the wrong\ndirection.\nPanel 4 resembles more an accumulator variable than a broadcast variable.\n2,5\nIncorrect. While panel 2 shows broadcasting, panel 5 includes bi-directional communication - not a\ncharacteristic of broadcasting.\nMore info: Broadcast Join with Spark - henning.kropponline.de"
    },
    {
        "question": "The code block shown below should return a DataFrame with only columns from DataFrame\ntransactionsDf for which there is a corresponding transactionId in DataFrame itemsDf. DataFrame\nitemsDf is very small and much smaller than DataFrame transactionsDf. The query should be\n\n\nexecuted in an optimized way. Choose the answer that correctly fills the blanks in the code block to\naccomplish this.\n__1__.__2__(__3__, __4__, __5__)",
        "options": [
            "A. 1. transactionsDf\n2. join\n3. broadcast(itemsDf)\n4. transactionsDf.transactionId==itemsDf.transactionId\n5. \"outer\"",
            "B. 1. transactionsDf\n2. join\n3. itemsDf\n4. transactionsDf.transactionId==itemsDf.transactionId\n5. \"anti\"",
            "C. 1. transactionsDf\n2. join\n3. broadcast(itemsDf)\n4. \"transactionId\"\n5. \"left_semi\"",
            "D. 1. itemsDf\n2. broadcast\n3. transactionsDf\n4. \"transactionId\"\n5. \"left_semi\"",
            "E. 1. itemsDf\n2. join\n3. broadcast(transactionsDf)\n4. \"transactionId\"\n5. \"left_semi\""
        ],
        "answer": [
            "C"
        ],
        "explanation": "Correct code block:\ntransactionsDf.join(broadcast(itemsDf), \"transactionId\", \"left_semi\")\nThis question is extremely difficult and exceeds the difficulty of questions in the exam by far.\nA first indication of what is asked from you here is the remark that \"the query should be executed in\nan optimized way\". You also have qualitative information about the size of itemsDf and\ntransactionsDf. Given that itemsDf is \"very small\" and that the execution should be optimized, you\nshould consider instructing Spark to perform a broadcast join, broadcasting the \"very small\"\nDataFrame itemsDf to all executors. You can explicitly suggest this to Spark via wrapping itemsDf into\na broadcast() operator. One answer option does not include this operator, so you can disregard it.\nAnother answer option wraps the broadcast() operator around transactionsDf - the bigger of the two\nDataFrames. This answer option does not make sense in the optimization context and can likewise be\ndisregarded.\nWhen thinking about the broadcast() operator, you may also remember that it is a method of\npyspark.sql.functions. One answer option, however, resolves to itemsDf.broadcast([...]). The\nDataFrame class has no broadcast() method, so this answer option can be eliminated as well.\n\n\nAll two remaining answer options resolve to transactionsDf.join([...]) in the first 2 gaps, so you will\nhave to figure out the details of the join now. You can pick between an outer and a left semi join. An\nouter join would include columns from both DataFrames, where a left semi join only includes\ncolumns from the \"left\" table, here transactionsDf, just as asked for by the question. So, the correct\nanswer is the one that uses the left_semi join."
    },
    {
        "question": "The code block displayed below contains an error. When the code block below has\nexecuted, it should have divided DataFrame transactionsDf into 14 parts, based on columns storeId\nand transactionDate (in this order). Find the error.\nCode block:\ntransactionsDf.coalesce(14, (\"storeId\", \"transactionDate\"))",
        "options": [
            "A. The parentheses around the column names need to be removed and .select() needs to be\nappended to the code block.",
            "B. Operator coalesce needs to be replaced by repartition, the parentheses around the column names\nneed to be removed, and .count() needs to be appended to the code block.",
            "C. Operator coalesce needs to be replaced by repartition, the parentheses around the column names\nneed to be removed, and .select() needs to be appended to the code block.",
            "D. Operator coalesce needs to be replaced by repartition and the parentheses around the column\nnames need to be replaced by square brackets.",
            "E. Operator coalesce needs to be replaced by repartition."
        ],
        "answer": [
            "B"
        ],
        "explanation": "Correct code block:\ntransactionsDf.repartition(14, \"storeId\", \"transactionDate\").count()\nSince we do not know how many partitions DataFrame transactionsDf has, we cannot safely use\ncoalesce, since it would not make any change if the current number of partitions is smaller than 14.\nSo, we need to use repartition.\nIn the Spark documentation, the call structure for repartition is shown like this:\nDataFrame.repartition(numPartitions, *cols). The * operator means that any argument after\nnumPartitions will be interpreted as column. Therefore, the brackets need to be removed.\nFinally, the question specifies that after the execution the DataFrame should be divided. So, indirectly\nthis question is asking us to append an action to the code block. Since .select() is a transformation.\nthe only possible choice here is .count().\nMore info: pyspark.sql.DataFrame.repartition - PySpark 3.1.1 documentation Static notebook |\nDynamic notebook: See test 1"
    },
    {
        "question": "The code block displayed below contains an error. The code block should trigger Spark to\ncache DataFrame transactionsDf in executor memory where available, writing to disk where\ninsufficient executor memory is available, in a fault-tolerant way. Find the error.\nCode block:\ntransactionsDf.persist(StorageLevel.MEMORY_AND_DISK)",
        "options": [
            "A. Caching is not supported in Spark, data are always recomputed.",
            "B. Data caching capabilities can be accessed through the spark object, but not through the\nDataFrame API.",
            "C. The storage level is inappropriate for fault-tolerant storage.",
            "D. The code block uses the wrong operator for caching.",
            "E. The DataFrameWriter needs to be invoked."
        ],
        "answer": [
            "C"
        ],
        "explanation": "The storage level is inappropriate for fault-tolerant storage.\nCorrect. Typically, when thinking about fault tolerance and storage levels, you would want to store\nredundant copies of the dataset. This can be achieved by using a storage level such as\nStorageLevel.MEMORY_AND_DISK_2.\nThe code block uses the wrong command for caching.\nWrong. In this case, DataFrame.persist() needs to be used, since this operator supports passing a\nstorage level.\nDataFrame.cache() does not support passing a storage level.\nCaching is not supported in Spark, data are always recomputed.\nIncorrect. Caching is an important component of Spark, since it can help to accelerate Spark\nprograms to great extent. Caching is often a good idea for datasets that need to be accessed\nrepeatedly.\nData caching capabilities can be accessed through the spark object, but not through the DataFrame\nAPI.\nNo. Caching is either accessed through DataFrame.cache() or DataFrame.persist().\nThe DataFrameWriter needs to be invoked.\nWrong. The DataFrameWriter can be accessed via DataFrame.write and is used to write data to\nexternal data stores, mostly on disk. Here, we find keywords such as \"cache\" and \"executor memory\"\nthat point us away from using external data stores. We aim to save data to memory to accelerate the\nreading process, since reading from disk is comparatively slower. The DataFrameWriter does not\nwrite to memory, so we cannot use it here.\nMore info: Best practices for caching in Spark SQL | by David Vrba | Towards Data Science"
    },
    {
        "question": "Which of the following code blocks reads in the parquet file stored at location filePath, given\nthat all columns in the parquet file contain only whole numbers and are stored in the most\nappropriate format for this kind of data?",
        "options": [
            "A. 1.spark.read.schema(\n2. StructType(\n3. StructField(\"transactionId\", IntegerType(), True),\n4. StructField(\"predError\", IntegerType(), True)\n5. )).load(filePath)",
            "B. 1.spark.read.schema([\n2. StructField(\"transactionId\", NumberType(), True),\n3. StructField(\"predError\", IntegerType(), True)\n4. ]).load(filePath)",
            "C. 1.spark.read.schema(\n2. StructType([\n3. StructField(\"transactionId\", StringType(), True),\n4. StructField(\"predError\", IntegerType(), True)]\n5. )).parquet(filePath)",
            "D. 1.spark.read.schema(\n2. StructType([\n3. StructField(\"transactionId\", IntegerType(), True),\n4. StructField(\"predError\", IntegerType(), True)]\n5. )).format(\"parquet\").load(filePath)",
            "E. 1.spark.read.schema([\n2. StructField(\"transactionId\", IntegerType(), True),\n3. StructField(\"predError\", IntegerType(), True)\n4. ]).load(filePath, format=\"parquet\")"
        ],
        "answer": [
            "D"
        ],
        "explanation": "The schema passed into schema should be of type StructType or a string, so all entries in which a list\nis passed are incorrect.\nIn addition, since all numbers are whole numbers, the IntegerType() data type is the correct option\nhere.\nNumberType() is not a valid data type and StringType() would fail, since the parquet file is stored in\nthe \"most appropriate format for this kind of data\", meaning that it is most likely an IntegerType, and\nSpark does not convert data types if a schema is provided.\nAlso note that StructType accepts only a single argument (a list of StructFields). So, passing multiple\narguments is invalid.\nFinally, Spark needs to know which format the file is in. However, all of the options listed are valid\nhere, since Spark assumes parquet as a default when no file format is specifically passed.\nMore info: pyspark.sql.DataFrameReader.schema - PySpark 3.1.2 documentation and StructType\n- PySpark 3.1.2 documentation"
    },
    {
        "question": "Which of the following code blocks generally causes a great amount of network traffic?",
        "options": [
            "A. DataFrame.select()",
            "B. DataFrame.coalesce()",
            "C. DataFrame.collect()",
            "D. DataFrame.rdd.map()",
            "E. DataFrame.count()"
        ],
        "answer": [
            "C"
        ],
        "explanation": "DataFrame.collect() sends all data in a DataFrame from executors to the driver, so this generally\ncauses a great amount of network traffic in comparison to the other options listed.\nDataFrame.coalesce() just reduces the number of partitions and generally aims to reduce network\ntraffic in comparison to a full shuffle.\nDataFrame.select() is evaluated lazily and, unless followed by an action, does not cause significant\nnetwork traffic.\nDataFrame.rdd.map() is evaluated lazily, it does therefore not cause great amounts of network\ntraffic.\nDataFrame.count() is an action. While it does cause some network traffic, for the same DataFrame,\ncollecting all data in the driver would generally be considered to cause a greater amount of network\ntraffic."
    },
    {
        "question": "Which of the following code blocks creates a new DataFrame with 3 columns, productId,\nhighest, and lowest, that shows the biggest and smallest values of column value per value in column\nproductId from DataFrame transactionsDf?\nSample of DataFrame transactionsDf:\n1.+-------------+---------+-----+-------+---------+----+\n2.|transactionId|predError|value|storeId|productId| f|\n3.+-------------+---------+-----+-------+---------+----+\n4.| 1| 3| 4| 25| 1|null|\n5.| 2| 6| 7| 2| 2|null|\n6.| 3| 3| null| 25| 3|null|\n7.| 4| null| null| 3| 2|null|\n8.| 5| null| null| null| 2|null|\n9.| 6| 3| 2| 25| 2|null|\n10.+-------------+---------+-----+-------+---------+----+",
        "options": [
            "A. transactionsDf.max('value').min('value')",
            "B. transactionsDf.agg(max('value').alias('highest'), min('value').alias('lowest'))",
            "C. transactionsDf.groupby(col(productId)).agg(max(col(value)).alias(\"highest\"),\nmin(col(value)).alias(\"lowest\"))",
            "D. transactionsDf.groupby('productId').agg(max('value').alias('highest'), min('value').alias('lowest'))",
            "E. transactionsDf.groupby(\"productId\").agg({\"highest\": max(\"value\"), \"lowest\": min(\"value\")})"
        ],
        "answer": [
            "D"
        ],
        "explanation": "transactionsDf.groupby('productId').agg(max('value').alias('highest'), min('value').alias('lowest'))\nCorrect. groupby and aggregate is a common pattern to investigate aggregated values of groups.\ntransactionsDf.groupby(\"productId\").agg({\"highest\": max(\"value\"), \"lowest\": min(\"value\")}) Wrong.\nWhile DataFrame.agg() accepts dictionaries, the syntax of the dictionary in this code block is wrong.\nIf you use a dictionary, the syntax should be like {\"value\": \"max\"}, so using the column name as the\nkey and the aggregating function as value.\ntransactionsDf.agg(max('value').alias('highest'), min('value').alias('lowest')) Incorrect. While this is\nvalid Spark syntax, it does not achieve what the question asks for. The question specifically asks for\nvalues to be aggregated per value in column productId - this column is not considered here. Instead,\nthe max() and min() values are calculated as if the entire DataFrame was a group.\ntransactionsDf.max('value').min('value')\nWrong. There is no DataFrame.max() method in Spark, so this command will fail.\ntransactionsDf.groupby(col(productId)).agg(max(col(value)).alias(\"highest\"),\nmin(col(value)).alias(\"lowest\")) No. While this may work if the column names are expressed as\nstrings, this will not work as is. Python will interpret the column names as variables and, as a result,\npySpark will not understand which columns you want to aggregate.\nMore info: pyspark.sql.DataFrame.agg - PySpark 3.1.2 documentation\nStatic notebook | Dynamic notebook: See test 3"
    },
    {
        "question": "Which of the following describes the conversion of a computational query into an execution\nplan in Spark?",
        "options": [
            "A. Spark uses the catalog to resolve the optimized logical plan.",
            "B. The catalog assigns specific resources to the optimized memory plan.",
            "C. The executed physical plan depends on a cost optimization from a previous stage.",
            "D. Depending on whether DataFrame API or SQL API are used, the physical plan may differ.",
            "E. The catalog assigns specific resources to the physical plan."
        ],
        "answer": [
            "C"
        ],
        "explanation": "The executed physical plan depends on a cost optimization from a previous stage.\nCorrect! Spark considers multiple physical plans on which it performs a cost analysis and selects the\nfinal physical plan in accordance with the lowest-cost outcome of that analysis. That final physical\nplan is then executed by Spark.\nSpark uses the catalog to resolve the optimized logical plan.\nNo. Spark uses the catalog to resolve the unresolved logical plan, but not the optimized logical plan.\nOnce the unresolved logical plan is resolved, it is then optimized using the Catalyst Optimizer.\nThe optimized logical plan is the input for physical planning.\nThe catalog assigns specific resources to the physical plan.\nNo. The catalog stores metadata, such as a list of names of columns, data types, functions, and\ndatabases.\nSpark consults the catalog for resolving the references in a logical plan at the beginning of the\nconversion of the query into an execution plan. The result is then an optimized logical plan.\nDepending on whether DataFrame API or SQL API are used, the physical plan may differ.\nWrong - the physical plan is independent of which API was used. And this is one of the great strengths\nof Spark!\nThe catalog assigns specific resources to the optimized memory plan.\nThere is no specific \"memory plan\" on the journey of a Spark computation.\nMore info: Spark's Logical and Physical plans ... When, Why, How and Beyond. | by Laurent Leturgez |\ndatalex | Medium"
    },
    {
        "question": "Which of the elements that are labeled with a circle and a number contain an error or are\nmisrepresented?",
        "options": [
            "A. 1, 10",
            "B. 1, 8",
            "C. 10",
            "D. 7, 9, 10",
            "E. 1, 4, 6, 9"
        ],
        "answer": [
            "B"
        ],
        "explanation": "1: Correct - This should just read \"API\" or \"DataFrame API\". The DataFrame is not part of the SQL API.\nTo make a DataFrame accessible via SQL, you first need to create a DataFrame view. That view can\nthen be accessed via SQL.\n4: Although \"K_38_INU\" looks odd, it is a completely valid name for a DataFrame column.\n6: No, StringType is a correct type.\n7: Although a StringType may not be the most efficient way to store a phone number, there is\nnothing fundamentally wrong with using this type here.\n8: Correct - TreeType is not a type that Spark supports.\n9: No, Spark DataFrames support ArrayType variables. In this case, the variable would represent a\nsequence of elements with type LongType, which is also a valid type for Spark DataFrames.\n\n\n10: There is nothing wrong with this row.\nMore info: Data Types - Spark 3.1.1 Documentation (https://bit.ly/3aAPKJT)"
    },
    {
        "question": "The code block displayed below contains an error. The code block should display the\nschema of DataFrame transactionsDf. Find the error.\nCode block:\ntransactionsDf.rdd.printSchema",
        "options": [
            "A. There is no way to print a schema directly in Spark, since the schema can be printed easily through\nusing print(transactionsDf.columns), so that should be used instead.",
            "B. The code block should be wrapped into a print() operation.",
            "C. printSchema is only accessible through the spark session, so the code block should be rewritten as\nspark.printSchema(transactionsDf).",
            "D. printSchema is a method and should be written as printSchema(). It is also not callable through\ntransactionsDf.rdd, but should be called directly from transactionsDf.",
            "E. printSchema is a not a method of transactionsDf.rdd. Instead, the schema should be printed via\ntransactionsDf.print_schema()."
        ],
        "answer": [
            "D"
        ],
        "explanation": "Correct code block:\ntransactionsDf.printSchema()\nThis is more of a knowledge question that you should just memorize or look up in the provided\ndocumentation during the exam. You can get more info about DataFrame.printSchema() in the\ndocumentation (link below). However - it is a plain simple method without any arguments.\nOne answer points to an alternative of printing the schema: You could also use\nprint(transactionsDf.schema).\nThis will give you readable, but not nicely formatted, description of the schema.\nMore info: pyspark.sql.DataFrame.printSchema - PySpark 3.1.1 documentation Static notebook |\nDynamic notebook: See test 1"
    },
    {
        "question": "Which of the following is a problem with using accumulators?",
        "options": [
            "A. Only unnamed accumulators can be inspected in the Spark UI.",
            "B. Only numeric values can be used in accumulators.",
            "C. Accumulator values can only be read by the driver, but not by executors.",
            "D. Accumulators do not obey lazy evaluation.",
            "E. Accumulators are difficult to use for debugging because they will only be updated once,\nindependent if a task has to be re-run due to hardware failure."
        ],
        "answer": [
            "C"
        ],
        "explanation": "Accumulator values can only be read by the driver, but not by executors.\nCorrect. So, for example, you cannot use an accumulator variable for coordinating workloads\nbetween executors. The typical, canonical, use case of an accumulator value is to report data, for\nexample for debugging purposes, back to the driver. For example, if you wanted to count values that\nmatch a specific condition in a UDF for debugging purposes, an accumulator provides a good way to\ndo that.\n\n\nOnly numeric values can be used in accumulators.\nNo. While pySpark's Accumulator only supports numeric values (think int and float), you can define\naccumulators for custom types via the AccumulatorParam interface (documentation linked below).\nAccumulators do not obey lazy evaluation.\nIncorrect - accumulators do obey lazy evaluation. This has implications in practice: When an\naccumulator is encapsulated in a transformation, that accumulator will not be modified until a\nsubsequent action is run.\nAccumulators are difficult to use for debugging because they will only be updated once, independent\nif a task has to be re-run due to hardware failure.\nWrong. A concern with accumulators is in fact that under certain conditions they can run for each\ntask more than once. For example, if a hardware failure occurs during a task after an accumulator\nvariable has been increased but before a task has finished and Spark launches the task on a different\nworker in response to the failure, already executed accumulator variable increases will be repeated.\nOnly unnamed accumulators can be inspected in the Spark UI.\nNo. Currently, in PySpark, no accumulators can be inspected in the Spark UI. In the Scala interface of\nSpark, only named accumulators can be inspected in the Spark UI.\nMore info: Aggregating Results with Spark Accumulators | Sparkour, RDD Programming Guide - Spark\n3.1.2 Documentation, pyspark.Accumulator - PySpark 3.1.2 documentation, and\npyspark.AccumulatorParam - PySpark 3.1.2 documentation"
    },
    {
        "question": "Which of the following code blocks returns a new DataFrame in which column attributes of\nDataFrame itemsDf is renamed to feature0 and column supplier to feature1?",
        "options": [
            "A. itemsDf.withColumnRenamed(attributes, feature0).withColumnRenamed(supplier, feature1)",
            "B. 1.itemsDf.withColumnRenamed(\"attributes\", \"feature0\")\n2.itemsDf.withColumnRenamed(\"supplier\", \"feature1\")",
            "C. itemsDf.withColumnRenamed(col(\"attributes\"), col(\"feature0\"), col(\"supplier\"), col(\"feature1\"))",
            "D. itemsDf.withColumnRenamed(\"attributes\", \"feature0\").withColumnRenamed(\"supplier\",\n\"feature1\")",
            "E. itemsDf.withColumn(\"attributes\", \"feature0\").withColumn(\"supplier\", \"feature1\")"
        ],
        "answer": [
            "D"
        ],
        "explanation": "itemsDf.withColumnRenamed(\"attributes\", \"feature0\").withColumnRenamed(\"supplier\", \"feature1\")\nCorrect! Spark's DataFrame.withColumnRenamed syntax makes it relatively easy to change the name\nof a column.\nitemsDf.withColumnRenamed(attributes, feature0).withColumnRenamed(supplier, feature1)\nIncorrect. In this code block, the Python interpreter will try to use attributes and the other column\nnames as variables. Needless to say, they are undefined, and as a result the block will not run.\nitemsDf.withColumnRenamed(col(\"attributes\"), col(\"feature0\"), col(\"supplier\"), col(\"feature1\"))\nWrong. The DataFrame.withColumnRenamed() operator takes exactly two string arguments. So, in\nthis answer both using col() and using four arguments is wrong.\nitemsDf.withColumnRenamed(\"attributes\", \"feature0\")\nitemsDf.withColumnRenamed(\"supplier\", \"feature1\")\nNo. In this answer, the returned DataFrame will only have column supplier be renamed, since the\nresult of the first line is not written back to itemsDf.\nitemsDf.withColumn(\"attributes\", \"feature0\").withColumn(\"supplier\", \"feature1\") Incorrect. While\nwithColumn works for adding and naming new columns, you cannot use it to rename existing\n\n\ncolumns.\nMore info: pyspark.sql.DataFrame.withColumnRenamed - PySpark 3.1.2 documentation Static\nnotebook | Dynamic notebook: See test 3"
    },
    {
        "question": "The code block displayed below contains at least one error. The code block should return a\nDataFrame with only one column, result. That column should include all values in column value from\nDataFrame transactionsDf raised to the power of 5, and a null value for rows in which there is no\nvalue in column value. Find the error(s).\nCode block:\n1.from pyspark.sql.functions import udf\n2.from pyspark.sql import types as T\n3.\n4.transactionsDf.createOrReplaceTempView('transactions')\n5.\n6.def pow_5(x):\n7. return x**5\n8.\n9.spark.udf.register(pow_5, 'power_5_udf', T.LongType())\n10.spark.sql('SELECT power_5_udf(value) FROM transactions')",
        "options": [
            "A. The pow_5 method is unable to handle empty values in column value and the name of the column\nin the returned DataFrame is not result.",
            "B. The returned DataFrame includes multiple columns instead of just one column.",
            "C. The pow_5 method is unable to handle empty values in column value, the name of the column in\nthe returned DataFrame is not result, and the SparkSession cannot access the transactionsDf\nDataFrame.",
            "D. The pow_5 method is unable to handle empty values in column value, the name of the column in\nthe returned DataFrame is not result, and Spark driver does not call the UDF function appropriately.",
            "E. The pow_5 method is unable to handle empty values in column value, the UDF function is not\nregistered properly with the Spark driver, and the name of the column in the returned DataFrame is\nnot result."
        ],
        "answer": [
            "D"
        ],
        "explanation": "Correct code block:\nfrom pyspark.sql.functions import udf\nfrom pyspark.sql import types as T\ntransactionsDf.createOrReplaceTempView('transactions')\ndef pow_5(x):\nif x:\nreturn x**5\nreturn x\nspark.udf.register('power_5_udf', pow_5, T.LongType())\nspark.sql('SELECT power_5_udf(value) AS result FROM transactions')\nHere it is important to understand how the pow_5 method handles empty values. In the wrong code\nblock above, the pow_5 method is unable to handle empty values and will throw an error, since\nPython's ** operator cannot deal with any null value Spark passes into method pow_5.\n\n\nThe order of arguments for registering the UDF function with Spark via spark.udf.register matters. In\nthe code snippet in the question, the arguments for the SQL method name and the actual Python\nfunction are switched. You can read more about the arguments of spark.udf.register and see some\nexamples of its usage in the documentation (link below).\nFinally, you should recognize that in the original code block, an expression to rename column created\nthrough the UDF function is missing. The renaming is done by SQL's AS result argument.\nOmitting that argument, you end up with the column name power_5_udf(value) and not result.\nMore info: pyspark.sql.functions.udf - PySpark 3.1.1 documentation"
    },
    {
        "question": "Which of the following code blocks reads in the JSON file stored at filePath as a DataFrame?",
        "options": [
            "A. spark.read.json(filePath)",
            "B. spark.read.path(filePath, source=\"json\")",
            "C. spark.read().path(filePath)",
            "D. spark.read().json(filePath)",
            "E. spark.read.path(filePath)"
        ],
        "answer": [
            "A"
        ],
        "explanation": "spark.read.json(filePath)\nCorrect. spark.read accesses Spark's DataFrameReader. Then, Spark identifies the file type to be read\nas JSON type by passing filePath into the DataFrameReader.json() method.\nspark.read.path(filePath)\nIncorrect. Spark's DataFrameReader does not have a path method. A universal way to read in files is\nprovided by the DataFrameReader.load() method (link below).\nspark.read.path(filePath, source=\"json\")\nWrong. A DataFrameReader.path() method does not exist (see above).\nspark.read().json(filePath)\nIncorrect. spark.read is a way to access Spark's DataFrameReader. However, the DataFrameReader is\nnot callable, so calling it via spark.read() will fail.\nspark.read().path(filePath)\nNo, Spark's DataFrameReader is not callable (see above).\nMore info: pyspark.sql.DataFrameReader.json - PySpark 3.1.2 documentation,\npyspark.sql.DataFrameReader.load - PySpark 3.1.2 documentation Static notebook | Dynamic\nnotebook: See test 3"
    },
    {
        "question": "Which of the following describes characteristics of the Spark driver?",
        "options": [
            "A. The Spark driver requests the transformation of operations into DAG computations from the\nworker nodes.",
            "B. If set in the Spark configuration, Spark scales the Spark driver horizontally to improve parallel\nprocessing performance.",
            "C. The Spark driver processes partitions in an optimized, distributed fashion.",
            "D. In a non-interactive Spark application, the Spark driver automatically creates the SparkSession\nobject.",
            "E. The Spark driver's responsibility includes scheduling queries for execution on worker nodes."
        ],
        "answer": [
            "D"
        ],
        "explanation": "The Spark driver requests the transformation of operations into DAG computations from the worker\nnodes.\nNo, the Spark driver transforms operations into DAG computations itself.\nIf set in the Spark configuration, Spark scales the Spark driver horizontally to improve parallel\nprocessing performance.\nNo. There is always a single driver per application, but one or more executors.\nThe Spark driver processes partitions in an optimized, distributed fashion.\nNo, this is what executors do.\nIn a non-interactive Spark application, the Spark driver automatically creates the SparkSession object.\nWrong. In a non-interactive Spark application, you need to create the SparkSession object. In an\ninteractive Spark shell, the Spark driver instantiates the object for you."
    },
    {
        "question": "Which of the following code blocks saves DataFrame transactionsDf in location\n/FileStore/transactions.csv as a CSV file and throws an error if a file already exists in the location?",
        "options": [
            "A. transactionsDf.write.save(\"/FileStore/transactions.csv\")",
            "B. transactionsDf.write.format(\"csv\").mode(\"error\").path(\"/FileStore/transactions.csv\")",
            "C. transactionsDf.write.format(\"csv\").mode(\"ignore\").path(\"/FileStore/transactions.csv\")",
            "D. transactionsDf.write(\"csv\").mode(\"error\").save(\"/FileStore/transactions.csv\")",
            "E. transactionsDf.write.format(\"csv\").mode(\"error\").save(\"/FileStore/transactions.csv\")"
        ],
        "answer": [
            "E"
        ],
        "explanation": "Static notebook | Dynamic notebook: See test 1\n(https://flrs.github.io/spark_practice_tests_code/#1/28.html ,\nhttps://bit.ly/sparkpracticeexams_import_instructions)"
    },
    {
        "question": "The code block displayed below contains an error. The code block should return a\nDataFrame where all entries in column supplier contain the letter combination et in this order. Find\nthe error.\nCode block:\nitemsDf.filter(Column('supplier').isin('et'))",
        "options": [
            "A. The Column operator should be replaced by the col operator and instead of isin, contains should\nbe used.",
            "B. The expression inside the filter parenthesis is malformed and should be replaced by isin('et',\n'supplier').",
            "C. Instead of isin, it should be checked whether column supplier contains the letters et, so isin should\nbe replaced with contains. In addition, the column should be accessed using col['supplier'].",
            "D. The expression only returns a single column and filter should be replaced by select."
        ],
        "answer": [
            "B"
        ],
        "explanation": "Correct code block:\nitemsDf.filter(col('supplier').contains('et'))\nA mixup can easily happen here between isin and contains. Since we want to check whether a colum\nn\n\"contains\" the values et, this is the operator we should use here. Note that both methods are\nmethods of Spark's Column object. See below for documentation links.\n\n\nA specific Column object can be accessed through the col() method and not the Column() method or\nthrough col[], which is an essential thing to know here. In PySpark, Column references a generic\ncolumn object. To use it for queries, you need to link the generic column object to a specific\nDataFrame. This can be achieved, for example, through the col() method.\nMore info:\n- isin documentation: pyspark.sql.Column.isin - PySpark 3.1.1 documentation\n- contains documentation: pyspark.sql.Column.contains - PySpark 3.1.1 documentation Stati\nc notebook | Dynamic notebook: See test 1"
    },
    {
        "question": "The code block displayed below contains an error. The code block should write DataFrame\ntransactionsDf as a parquet file to location filePath after partitioning it on column storeId. Find the\nerror.\nCode block:\ntransactionsDf.write.partitionOn(\"storeId\").parquet(filePath)",
        "options": [
            "A. The partitioning column as well as the file path should be passed to the write() method of\nDataFrame transactionsDf directly and not as appended commands as in the code block.",
            "B. The partitionOn method should be called before the write method.",
            "C. The operator should use the mode() option to configure the DataFrameWriter so that it replaces\nany existing files at location filePath.",
            "D. Column storeId should be wrapped in a col() operator.",
            "E. No method partitionOn() exists for the DataFrame class, partitionBy() should be used instead."
        ],
        "answer": [
            "E"
        ],
        "explanation": "No method partitionOn() exists for the DataFrame class, partitionBy() should be used instead.\nCorrect! Find out more about partitionBy() in the documentation (linked below).\nThe operator should use the mode() option to configure the DataFrameWriter so that it replaces any\nexisting files at location filePath.\nNo. There is no information about whether files should be overwritten in the question.\nThe partitioning column as well as the file path should be passed to the write() method of DataFrame\ntransactionsDf directly and not as appended commands as in the code block.\nIncorrect. To write a DataFrame to disk, you need to work with a DataFrameWriter object which you\nget access to through the DataFrame.writer property - no parentheses involved.\nColumn storeId should be wrapped in a col() operator.\nNo, this is not necessary - the problem is in the partitionOn command (see above).\nThe partitionOn method should be called before the write method.\nWrong. First of all partitionOn is not a valid method of DataFrame. However, even assuming\npartitionOn would be replaced by partitionBy (which is a valid method), this method is a method of\nDataFrameWriter and not of DataFrame. So, you would always have to first call DataFrame.write to\nget access to the DataFrameWriter object and afterwards call partitionBy.\nMore info: pyspark.sql.DataFrameWriter.partitionBy - PySpark 3.1.2 documentation Static notebook |\nDynamic notebook: See test 3"
    },
    {
        "question": "The code block shown below should convert up to 5 rows in DataFrame transactionsDf that\nhave the value 25 in column storeId into a Python list. Choose the answer that correctly fills the\nblanks in the code block to accomplish this.\nCode block:\n\n\ntransactionsDf.__1__(__2__).__3__(__4__)",
        "options": [
            "A. 1. filter\n2. \"storeId\"==25\n3. collect\n4. 5",
            "B. 1. filter\n2. col(\"storeId\")==25\n3. toLocalIterator\n4. 5",
            "C. 1. select\n2. storeId==25\n3. head\n4. 5",
            "D. 1. filter\n2. col(\"storeId\")==25\n3. take\n4. 5",
            "E. 1. filter\n2. col(\"storeId\")==25\n3. collect\n4. 5"
        ],
        "answer": [
            "D"
        ],
        "explanation": "The correct code block is:\ntransactionsDf.filter(col(\"storeId\")==25).take(5)\nAny of the options with collect will not work because collect does not take any arguments, and in\nboth cases the argument 5 is given.\nThe option with toLocalIterator will not work because the only argument to toLocalIterator is\nprefetchPartitions which is a boolean, so passing 5 here does not make sense.\nThe option using head will not work because the expression passed to select is not proper syntax. It\nwould work if the expression would be col(\"storeId\")==25.\nStatic notebook | Dynamic notebook: See test 1\n(https://flrs.github.io/spark_practice_tests_code/#1/24.html ,\nhttps://bit.ly/sparkpracticeexams_import_instructions)"
    },
    {
        "question": "Which of the following code blocks returns a one-column DataFrame of all values in column\nsupplier of DataFrame itemsDf that do not contain the letter X? In the DataFrame, every value should\nonly be listed once.\nSample of DataFrame itemsDf:\n1.+------+--------------------+--------------------+-------------------+\n2.|itemId| itemName| attributes| supplier|\n3.+------+--------------------+--------------------+-------------------+\n4.| 1|Thick Coat for Wa...|[blue, winter, cozy]|Sports Company Inc.|\n5.| 2|Elegant Outdoors ...|[red, summer, fre...| YetiX|\n6.| 3| Outdoors Backpack|[green, summer, t...|Sports Company Inc.|\n7.+------+--------------------+--------------------+-------------------+",
        "options": [
            "A. itemsDf.filter(col(supplier).not_contains('X')).select(supplier).distinct()",
            "B. itemsDf.select(~col('supplier').contains('X')).distinct()",
            "C. itemsDf.filter(not(col('supplier').contains('X'))).select('supplier').unique()",
            "D. itemsDf.filter(~col('supplier').contains('X')).select('supplier').distinct()",
            "E. itemsDf.filter(!col('supplier').contains('X')).select(col('supplier')).unique()"
        ],
        "answer": [
            "D"
        ],
        "explanation": "Output of correct code block:\n+-------------------+\n| supplier|\n+-------------------+\n|Sports Company Inc.|\n+-------------------+\nKey to managing this question is understand which operator to use to do the opposite of an\noperation\n- the ~ (not) operator. In addition, you should know that there is no unique() method.\nStatic notebook | Dynamic notebook: See test 1"
    },
    {
        "question": "Which of the following is one of the big performance advantages that Spark has over\nHadoop?",
        "options": [
            "A. Spark achieves great performance by storing data in the DAG format, whereas Hadoop can only\nuse parquet files.",
            "B. Spark achieves higher resiliency for queries since, different from Hadoop, it can be deployed on\nKubernetes.",
            "C. Spark achieves great performance by storing data and performing computation in memory,\nwhereas large jobs in Hadoop require a large amount of relatively slow disk I/O operations.",
            "D. Spark achieves great performance by storing data in the HDFS format, whereas Hadoop can only\nuse parquet files.",
            "E. Spark achieves performance gains for developers by extending Hadoop's DataFrames with a user-\nfriendly API."
        ],
        "answer": [
            "C"
        ],
        "explanation": "Spark achieves great performance by storing data in the DAG format, whereas Hadoop can only use\nparquet files.\nWrong, there is no \"DAG format\". DAG stands for \"directed acyclic graph\". The DAG is a means of\nrepresenting computational steps in Spark. However, it is true that Hadoop does not use a DAG.\nThe introduction of the DAG in Spark was a result of the limitation of Hadoop's map reduce\nframework in which data had to be written to and read from disk continuously.\nGraph DAG in Apache Spark - DataFlair\nSpark achieves great performance by storing data in the HDFS format, whereas Hadoop can only use\nparquet files.\nNo. Spark can certainly store data in HDFS (as well as other formats), but this is not a key\nperformance advantage over Hadoop. Hadoop can use multiple file formats, not only parquet.\nSpark achieves higher resiliency for queries since, different from Hadoop, it can be deployed on\nKubernetes.\n\n\nNo, resiliency is not asked for in the question. The question is about performance improvements.\nBoth Hadoop and Spark can be deployed on Kubernetes.\nSpark achieves performance gains for developers by extending Hadoop's DataFrames with a user-\nfriendly API.\nNo. DataFrames are a concept in Spark, but not in Hadoop."
    },
    {
        "question": "The code block shown below should return a two-column DataFrame with columns\ntransactionId and supplier, with combined information from DataFrames itemsDf and transactionsDf.\nThe code block should merge rows in which column productId of DataFrame transactionsDf matches\nthe value of column itemId in DataFrame itemsDf, but only where column storeId of DataFrame\ntransactionsDf does not match column itemId of DataFrame itemsDf. Choose the answer that\ncorrectly fills the blanks in the code block to accomplish this.\nCode block:\ntransactionsDf.__1__(itemsDf, __2__).__3__(__4__)",
        "options": [
            "A. 1. join\n2. transactionsDf.productId==itemsDf.itemId, how=\"inner\"\n3. select\n4. \"transactionId\", \"supplier\"",
            "B. 1. select\n2. \"transactionId\", \"supplier\"\n3. join\n4. [transactionsDf.storeId!=itemsDf.itemId, transactionsDf.productId==itemsDf.itemId]",
            "C. 1. join\n2. [transactionsDf.productId==itemsDf.itemId, transactionsDf.storeId!=itemsDf.itemId]\n3. select\n4. \"transactionId\", \"supplier\"",
            "D. 1. filter\n2. \"transactionId\", \"supplier\"\n3. join\n4. \"transactionsDf.storeId!=itemsDf.itemId, transactionsDf.productId==itemsDf.itemId\"",
            "E. 1. join\n2. transactionsDf.productId==itemsDf.itemId, transactionsDf.storeId!=itemsDf.itemId\n3. filter\n4. \"transactionId\", \"supplier\""
        ],
        "answer": [
            "C"
        ],
        "explanation": "This question is pretty complex and, in its complexity, is probably above what you would encounter in\nthe exam. However, reading the question carefully, you can use your logic skills to weed out the\nwrong answers here.\nFirst, you should examine the join statement which is common to all answers. The first argument of\nthe join() operator (documentation linked below) is the DataFrame to be joined with. Where join is in\ngap 3, the first argument of gap 4 should therefore be another DataFrame. For none of the questions\nwhere join is in the third gap, this is the case. So you can immediately discard two answers.\nFor all other answers, join is in gap 1, followed by .(itemsDf, according to the code block. Given how\nthe join() operator is called, there are now three remaining candidates.\n\n\nLooking further at the join() statement, the second argument (on=) expects \"a string for the join\ncolumn name, a list of column names, a join expression (Column), or a list of Columns\", according to\nthe documentation. As one answer option includes a list of join expressions\n(transactionsDf.productId==itemsDf.itemId, transactionsDf.storeId!=itemsDf.itemId) which is\nunsupported according to the documentation, we can discard that answer, leaving us with two\nremaining candidates.\nBoth candidates have valid syntax, but only one of them fulfills the condition in the question \"only\nwhere column storeId of DataFrame transactionsDf does not match column itemId of DataFrame\nitemsDf\". So, this one remaining answer option has to be the correct one!\nAs you can see, although sometimes overwhelming at first, even more complex questions can be\nfigured out by rigorously applying the knowledge you can gain from the documentation during the\nexam.\nMore info: pyspark.sql.DataFrame.join - PySpark 3.1.2 documentation\nStatic notebook | Dynamic notebook: See test 3"
    },
    {
        "question": "Which of the following code blocks returns a single-row DataFrame that only has a column\ncorr which shows the Pearson correlation coefficient between columns predError and value in\nDataFrame transactionsDf?",
        "options": [
            "A. transactionsDf.select(corr([\"predError\", \"value\"]).alias(\"corr\")).first()",
            "B. transactionsDf.select(corr(col(\"predError\"), col(\"value\")).alias(\"corr\")).first()",
            "C. transactionsDf.select(corr(predError, value).alias(\"corr\"))",
            "D. transactionsDf.select(corr(col(\"predError\"), col(\"value\")).alias(\"corr\"))",
            "E. transactionsDf.select(corr(\"predError\", \"value\"))"
        ],
        "answer": [
            "D"
        ],
        "explanation": "In difficulty, this question is above what you can expect from the exam. What this question NO:\nwants to teach you, however, is to pay attention to the useful details included in the documentation.\npyspark.sql.corr is not a very common method, but it deals with Spark's data structure in an\ninteresting way.\nThe command takes two columns over multiple rows and returns a single row - similar to an\naggregation function. When examining the documentation (linked below), you will find this code\nexample:\na = range(20)\nb = [2 * x for x in range(20)]\ndf = spark.createDataFrame(zip(a, b), [\"a\", \"b\"])\ndf.agg(corr(\"a\", \"b\").alias('c')).collect()\n[Row(c=1.0)]\nSee how corr just returns a single row? Once you understand this, you should be suspicious about\nanswers that include first(), since there is no need to just select a single row. A reason to eliminate\nthose answers is that DataFrame.first() returns an object of type Row, but not DataFrame, as\nrequested in the question.\ntransactionsDf.select(corr(col(\"predError\"), col(\"value\")).alias(\"corr\")) Correct! After calculating the\nPearson correlation coefficient, the resulting column is correctly renamed to corr.\ntransactionsDf.select(corr(predError, value).alias(\"corr\"))\nNo. In this answer, Python will interpret column names predError and value as variable names.\ntransactionsDf.select(corr(col(\"predError\"), col(\"value\")).alias(\"corr\")).first() Incorrect. first() returns a\n\n\nrow, not a DataFrame (see above and linked documentation below).\ntransactionsDf.select(corr(\"predError\", \"value\"))\nWrong. Whie this statement returns a DataFrame in the desired shape, the column will have the\nname corr(predError, value) and not corr.\ntransactionsDf.select(corr([\"predError\", \"value\"]).alias(\"corr\")).first() False. In addition to first()\nreturning a row, this code block also uses the wrong call structure for command corr which takes two\narguments (the two columns to correlate).\nMore info:\n- pyspark.sql.functions.corr - PySpark 3.1.2 documentation\n- pyspark.sql.DataFrame.first - PySpark 3.1.2 documentation\nStatic notebook | Dynamic notebook: See test 3"
    },
    {
        "question": "Which of the following code blocks reads in parquet file /FileStore/imports.parquet as a\nDataFrame?",
        "options": [
            "A. spark.mode(\"parquet\").read(\"/FileStore/imports.parquet\")",
            "B. spark.read.path(\"/FileStore/imports.parquet\", source=\"parquet\")",
            "C. spark.read().parquet(\"/FileStore/imports.parquet\")",
            "D. spark.read.parquet(\"/FileStore/imports.parquet\")",
            "E. spark.read().format('parquet').open(\"/FileStore/imports.parquet\")"
        ],
        "answer": [
            "D"
        ],
        "explanation": "Static notebook | Dynamic notebook: See test 1\n(https://flrs.github.io/spark_practice_tests_code/#1/23.html ,\nhttps://bit.ly/sparkpracticeexams_import_instructions)"
    },
    {
        "question": "The code block shown below should return a copy of DataFrame transactionsDf with an\nadded column cos.\nThis column should have the values in column value converted to degrees and having the cosine of\nthose converted values taken, rounded to two decimals. Choose the answer that correctly fills the\nblanks in the code block to accomplish this.\nCode block:\ntransactionsDf.__1__(__2__, round(__3__(__4__(__5__)),2))",
        "options": [
            "A. 1. withColumn\n2. col(\"cos\")\n3. cos\n4. degrees\n5. transactionsDf.value",
            "B. 1. withColumnRenamed\n2. \"cos\"\n3. cos\n4. degrees\n5. \"transactionsDf.value\"",
            "C. 1. withColumn\n2. \"cos\"\n3. cos\n\n\n4. degrees\n5. transactionsDf.value",
            "D. 1. withColumn\n2. col(\"cos\")\n3. cos\n4. degrees\n5. col(\"value\")\nE\n. 1. withColumn\n2. \"cos\"\n3. degrees\n4. cos\n5. col(\"value\")"
        ],
        "answer": [
            "C"
        ],
        "explanation": "Correct code block:\ntransactionsDf.withColumn(\"cos\", round(cos(degrees(transactionsDf.value)),2)) This question is\nespecially confusing because col, \"cos\" are so similar. Similar-looking answer options can also appear\nin the exam and, just like in this question, you need to pay attention to the details to identify what\nthe correct answer option is.\nThe first answer option to throw out is the one that starts with withColumnRenamed: The question\nNO:\nspeaks specifically of adding a column. The withColumnRenamed operator only renames an existing\ncolumn, however, so you cannot use it here.\nNext, you will have to decide what should be in gap 2, the first argument of\ntransactionsDf.withColumn().\nLooking at the documentation (linked below), you can find out that the first argument of withColumn\nactually needs to be a string with the name of the column to be added. So, any answer that includes\ncol(\"cos\") as the option for gap 2 can be disregarded.\nThis leaves you with two possible answers. The real difference between these two answers is where\nthe cos and degree methods are, either in gaps 3 and 4, or vice-versa. From the question you can find\nout that the new column should have \"the values in column value converted to degrees and having\nthe cosine of those converted values taken\". This prescribes you a clear order of operations: First,\nyou convert values from column value to degrees and then you take the cosine of those values. So,\nthe inner parenthesis (gap 4) should contain the degree method and then, logically, gap 3 holds the\ncos method. This leaves you with just one possible correct answer.\nMore info: pyspark.sql.DataFrame.withColumn - PySpark 3.1.2 documentation Static notebook |\nDynamic notebook: See test 3"
    },
    {
        "question": "The code block displayed below contains an error. The code block should configure Spark to\nsplit data in 20 parts when exchanging data between executors for joins or aggregations. Find the\nerror.\nCode block:\nspark.conf.set(spark.sql.shuffle.partitions, 20)",
        "options": [
            "A. The code block uses the wrong command for setting an option.",
            "B. The code block sets the wrong option.",
            "C. The code block expresses the option incorrectly.",
            "D. The code block sets the incorrect number of parts.",
            "E. The code block is missing a parameter."
        ],
        "answer": [
            "C"
        ],
        "explanation": "Correct code block:\nspark.conf.set(\"spark.sql.shuffle.partitions\", 20)\nThe code block expresses the option incorrectly.\nCorrect! The option should be expressed as a string.\nThe code block sets the wrong option.\nNo, spark.sql.shuffle.partitions is the correct option for the use case in the question.\nThe code block sets the incorrect number of parts.\nWrong, the code block correctly states 20 parts.\nThe code block uses the wrong command for setting an option.\nNo, in PySpark spark.conf.set() is the correct command for setting an option.\nThe code block is missing a parameter.\nIncorrect, spark.conf.set() takes two parameters.\nMore info: Configuration - Spark 3.1.2 Documentation"
    },
    {
        "question": "Which of the following code blocks reads all CSV files in directory filePath into a single\nDataFrame, with column names defined in the CSV file headers?\nContent of directory filePath:\n1._SUCCESS\n2._committed_2754546451699747124\n3._started_2754546451699747124\n4.part-00000-tid-2754546451699747124-10eb85bf-8d91-4dd0-b60b-2f3c02eeecaa-298-1-\nc000.csv.gz\n5.part-00001-tid-2754546451699747124-10eb85bf-8d91-4dd0-b60b-2f3c02eeecaa-299-1-\nc000.csv.gz\n6.part-00002-tid-2754546451699747124-10eb85bf-8d91-4dd0-b60b-2f3c02eeecaa-300-1-\nc000.csv.gz\n7.part-00003-tid-2754546451699747124-10eb85bf-8d91-4dd0-b60b-2f3c02eeecaa-301-1-\nc000.csv.gz spark.option(\"header\",True).csv(filePath)",
        "options": [
            "A. spark.read.format(\"csv\").option(\"header\",True).option(\"compression\",\"zip\").load(filePath)",
            "B. spark.read().option(\"header\",True).load(filePath)",
            "C. spark.read.format(\"csv\").option(\"header\",True).load(filePath)",
            "D. spark.read.load(filePath)"
        ],
        "answer": [
            "C"
        ],
        "explanation": "The files in directory filePath are partitions of a DataFrame that have been exported using gzip\ncompression.\nSpark automatically recognizes this situation and imports the CSV files as separate partitions into a\nsingle DataFrame. It is, however, necessary to specify that Spark should load the file headers in the\nCSV with the header option, which is set to False by default."
    },
    {
        "question": "Which of the following is the deepest level in Spark's execution hierarchy?",
        "options": [
            "A. Job",
            "B. Task",
            "C. Executor",
            "D. Slot",
            "E. Stage"
        ],
        "answer": [
            "B"
        ],
        "explanation": "The hierarchy is, from top to bottom: Job, Stage, Task.\nExecutors and slots facilitate the execution of tasks, but they are not directly part of the hierarchy.\nExecutors are launched by the driver on worker nodes for the purpose of running a specific Spark\napplication. Slots help Spark parallelize work. An executor can have multiple slots which enable it to\nprocess multiple tasks in parallel."
    },
    {
        "question": "The code block shown below should return a DataFrame with columns transactionsId,\npredError, value, and f from DataFrame transactionsDf. Choose the answer that correctly fills the\nblanks in the code block to accomplish this.\ntransactionsDf.__1__(__2__)",
        "options": [
            "A. 1. filter\n2. \"transactionId\", \"predError\", \"value\", \"f\"",
            "B. 1. select\n2. \"transactionId, predError, value, f\"",
            "C. 1. select\n2. [\"transactionId\", \"predError\", \"value\", \"f\"]",
            "D. 1. where\n2. col(\"transactionId\"), col(\"predError\"), col(\"value\"), col(\"f\")",
            "E. 1. select\n2. col([\"transactionId\", \"predError\", \"value\", \"f\"])"
        ],
        "answer": [
            "C"
        ],
        "explanation": "Correct code block:\ntransactionsDf.select([\"transactionId\", \"predError\", \"value\", \"f\"])\nThe DataFrame.select returns specific columns from the DataFrame and accepts a list as its only\nargument.\nThus, this is the correct choice here. The option using col([\"transactionId\", \"predError\",\n\"value\", \"f\"]) is invalid, since inside col(), one can only pass a single column name, not a list. Likewise,\nall columns being specified in a single string like \"transactionId, predError, value, f\" is not valid syntax\n.\nfilter and where filter rows based on conditions, they do not control which columns to return.\nStatic notebook | Dynamic notebook: See test 2"
    },
    {
        "question": "The code block shown below should read all files with the file ending .png in directory path\ninto Spark.\nChoose the answer that correctly fills the blanks in the code block to accomplish this.\nspark.__1__.__2__(__3__).option(__4__, \"*.png\").__5__(path)",
        "options": [
            "A. 1. read()\n2. format\n3. \"binaryFile\"\n4. \"recursiveFileLookup\"\n5. load",
            "B. 1. read\n2. format\n3. \"binaryFile\"\n4. \"pathGlobFilter\"\n5. load",
            "C. 1. read\n2. format\n3. binaryFile\n4. pathGlobFilter\n5. load",
            "D. 1. open\n2. format\n3. \"image\"\n4. \"fileType\"\n5. open",
            "E. 1. open\n2. as\n3. \"binaryFile\"\n4. \"pathGlobFilter\"\n5. load"
        ],
        "answer": [
            "B"
        ],
        "explanation": "Correct code block:\nspark.read.format(\"binaryFile\").option(\"recursiveFileLookup\", \"*.png\").load(path) Spark can deal\nwith binary files, like images. Using the binaryFile format specification in the SparkSession's read API\nis the way to read in those files. Remember that, to access the read API, you need to start the\ncommand with spark.read. The pathGlobFilter option is a great way to filter files by name (and\nending). Finally, the path can be specified using the load operator - the open operator shown in one of\nthe answers does not exist."
    },
    {
        "question": "Which of the following code blocks applies the Python function to_limit on column\npredError in table transactionsDf, returning a DataFrame with columns transactionId and result?",
        "options": [
            "A. 1.spark.udf.register(\"LIMIT_FCN\", to_limit)\n2.spark.sql(\"SELECT transactionId, LIMIT_FCN(predError) AS result FROM transactionsDf\")",
            "B. 1.spark.udf.register(\"LIMIT_FCN\", to_limit)\n2.spark.sql(\"SELECT transactionId, LIMIT_FCN(predError) FROM transactionsDf AS result\")",
            "C. 1.spark.udf.register(\"LIMIT_FCN\", to_limit)\n2.spark.sql(\"SELECT transactionId, to_limit(predError) AS result FROM transactionsDf\")\nspark.sql(\"SELECT transactionId, udf(to_limit(predError)) AS result FROM transactionsDf\")",
            "D. 1.spark.udf.register(to_limit, \"LIMIT_FCN\")\n\n\n2.spark.sql(\"SELECT transactionId, LIMIT_FCN(predError) AS result FROM transactionsDf\")"
        ],
        "answer": [
            "A"
        ],
        "explanation": "spark.udf.register(\"LIMIT_FCN\", to_limit)\nspark.sql(\"SELECT transactionId, LIMIT_FCN(predError) AS result FROM transactionsDf\") Correct!\nFirst, you have to register to_limit as UDF to use it in a sql statement. Then, you can use it under the\nLIMIT_FCN name, correctly naming the resulting column result.\nspark.udf.register(to_limit, \"LIMIT_FCN\")\nspark.sql(\"SELECT transactionId, LIMIT_FCN(predError) AS result FROM transactionsDf\") No. In this\nanswer, the arguments to spark.udf.register are flipped.\nspark.udf.register(\"LIMIT_FCN\", to_limit)\nspark.sql(\"SELECT transactionId, to_limit(predError) AS result FROM transactionsDf\") Wrong, this\nanswer does not use the registered LIMIT_FCN in the sql statement, but tries to access the to_limit\nmethod directly. This will fail, since Spark cannot access it.\nspark.sql(\"SELECT transactionId, udf(to_limit(predError)) AS result FROM transactionsDf\") Incorrect,\nthere is no udf method in Spark's SQL.\nspark.udf.register(\"LIMIT_FCN\", to_limit)\nspark.sql(\"SELECT transactionId, LIMIT_FCN(predError) FROM transactionsDf AS result\") False. In this\nanswer, the column that results from applying the UDF is not correctly renamed to result.\nStatic notebook | Dynamic notebook: See test 3"
    },
    {
        "question": "Which of the following describes the role of tasks in the Spark execution hierarchy?",
        "options": [
            "A. Tasks are the smallest element in the execution hierarchy.",
            "B. Within one task, the slots are the unit of work done for each partition of the data.",
            "C. Tasks are the second-smallest element in the execution hierarchy.",
            "D. Stages with narrow dependencies can be grouped into one task.",
            "E. Tasks with wide dependencies can be grouped into one stage."
        ],
        "answer": [
            "A"
        ],
        "explanation": "Stages with narrow dependencies can be grouped into one task.\nWrong, tasks with narrow dependencies can be grouped into one stage.\nTasks with wide dependencies can be grouped into one stage.\nWrong, since a wide transformation causes a shuffle which always marks the boundary of a stage. So,\nyou cannot bundle multiple tasks that have wide dependencies into a stage.\nTasks are the second-smallest element in the execution hierarchy.\nNo, they are the smallest element in the execution hierarchy.\nWithin one task, the slots are the unit of work done for each partition of the data.\nNo, tasks are the unit of work done per partition. Slots help Spark parallelize work. An executor can\nhave multiple slots which enable it to process multiple tasks in parallel."
    },
    {
        "question": "Which of the following code blocks returns a DataFrame that has all columns of DataFrame\ntransactionsDf and an additional column predErrorSquared which is the squared value of column\npredError in DataFrame transactionsDf?",
        "options": [
            "A. transactionsDf.withColumn(\"predError\", pow(col(\"predErrorSquared\"), 2))",
            "B. transactionsDf.withColumnRenamed(\"predErrorSquared\", pow(predError, 2))",
            "C. transactionsDf.withColumn(\"predErrorSquared\", pow(col(\"predError\"), lit(2)))",
            "D. transactionsDf.withColumn(\"predErrorSquared\", pow(predError, lit(2)))",
            "E. transactionsDf.withColumn(\"predErrorSquared\", \"predError\"**2)"
        ],
        "answer": [
            "C"
        ],
        "explanation": "While only one of these code blocks works, the DataFrame API is pretty flexible when it comes to\naccepting columns into the pow() method. The following code blocks would also work:\ntransactionsDf.withColumn(\"predErrorSquared\", pow(\"predError\", 2))\ntransactionsDf.withColumn(\"predErrorSquared\", pow(\"predError\", lit(2))) Static notebook | Dynamic\nnotebook: See test 1 (https://flrs.github.io/spark_practice_tests_code/#1/26.html ,\nhttps://bit.ly/sparkpracticeexams_import_instructions)"
    },
    {
        "question": "Which of the following code blocks can be used to save DataFrame transactionsDf to\nmemory only, recalculating partitions that do not fit in memory when they are needed?",
        "options": [
            "A. from pyspark import StorageLevel\ntransactionsDf.cache(StorageLevel.MEMORY_ONLY)",
            "B. transactionsDf.cache()",
            "C. transactionsDf.storage_level('MEMORY_ONLY')",
            "D. transactionsDf.persist()",
            "E. transactionsDf.clear_persist()",
            "F. from pyspark import StorageLevel\ntransactionsDf.persist(StorageLevel.MEMORY_ONLY)"
        ],
        "answer": [
            "F"
        ],
        "explanation": "from pyspark import StorageLevel transactionsDf.persist(StorageLevel.MEMORY_ONLY) Correct. Note\nthat the storage level MEMORY_ONLY means that all partitions that do not fit into memory will be\nrecomputed when they are needed.\ntransactionsDf.cache()\nThis is wrong because the default storage level of DataFrame.cache() is MEMORY_AND_DISK,\nmeaning that partitions that do not fit into memory are stored on disk.\ntransactionsDf.persist()\nThis is wrong because the default storage level of DataFrame.persist() is MEMORY_AND_DISK.\ntransactionsDf.clear_persist()\nIncorrect, since clear_persist() is not a method of DataFrame.\ntransactionsDf.storage_level('MEMORY_ONLY')\nWrong. storage_level is not a method of DataFrame.\nMore info: RDD Programming Guide - Spark 3.0.0 Documentation, pyspark.sql.DataFrame.persist\n- PySpark\n3.0.0 documentation (https://bit.ly/3sxHLVC , https://bit.ly/3j2N6B9)"
    },
    {
        "question": "The code block shown below should add column transactionDateForm to DataFrame\ntransactionsDf. The column should express the unix-format timestamps in column transactionDate as\nstring type like Apr 26 (Sunday). Choose the answer that correctly fills the blanks in the code block to\naccomplish this.\ntransactionsDf.__1__(__2__, from_unixtime(__3__, __4__))",
        "options": [
            "A. 1. withColumn\n2. \"transactionDateForm\"\n3. \"MMM d (EEEE)\"\n4. \"transactionDate\"",
            "B. 1. select\n2. \"transactionDate\"\n3. \"transactionDateForm\"\n4. \"MMM d (EEEE)\"",
            "C. 1. withColumn\n2. \"transactionDateForm\"\n3. \"transactionDate\"\n4. \"MMM d (EEEE)\"",
            "D. 1. withColumn\n2. \"transactionDateForm\"\n3. \"transactionDate\"\n4. \"MM d (EEE)\"",
            "E. 1. withColumnRenamed\n2. \"transactionDate\"\n3. \"transactionDateForm\"\n4. \"MM d (EEE)\""
        ],
        "answer": [
            "C"
        ],
        "explanation": "Correct code block:\ntransactionsDf.withColumn(\"transactionDateForm\", from_unixtime(\"transactionDate\", \"MMM d\n(EEEE)\")) The question specifically asks about \"adding\" a column. In the context of all presented\nanswers, DataFrame.withColumn() is the correct command for this. In theory, DataFrame.select()\ncould also be used for this purpose, if all existing columns are selected and a new one is added.\nDataFrame.withColumnRenamed() is not the appropriate command, since it can only rename existing\ncolumns, but cannot add a new column or change the value of a column.\nOnce DataFrame.withColumn() is chosen, you can read in the documentation (see below) that the\nfirst input argument to the method should be the column name of the new column.\nThe final difficulty is the date format. The question indicates that the date format Apr 26 (Sunday) is\ndesired. The answers give \"MMM d (EEEE)\" and \"MM d (EEE)\" as options. It can be hard to know the\ndetails of the date format that is used in Spark. Specifically, knowing the differences between MMM\nand MM is probably not something you deal with every day. But, there is an easy way to remember\nthe difference: M (one letter) is usually the shortest form: 4 for April. MM includes padding: 04 for\nApril. MMM (three letters) is the three-letter month abbreviation: Apr for April. And MMMM is the\nlongest possible form: April. Knowing this four-letter sequence helps you select the correct option\nhere.\nMore info: pyspark.sql.DataFrame.withColumn - PySpark 3.1.2 documentation Static notebook |\nDynamic notebook: See test 3"
    },
    {
        "question": "Which of the following code blocks creates a new DataFrame with two columns season and\nwind_speed_ms where column season is of data type string and column wind_speed_ms is of data\ntype double?",
        "options": [
            "A. spark.DataFrame({\"season\": [\"winter\",\"summer\"], \"wind_speed_ms\": [4.5, 7.5]})",
            "B. spark.createDataFrame([(\"summer\", 4.5), (\"winter\", 7.5)], [\"season\", \"wind_speed_ms\"])",
            "C. 1. from pyspark.sql import types as T\n2. spark.createDataFrame(((\"summer\", 4.5), (\"winter\", 7.5)), T.StructType([T.StructField(\"season\",\nT.CharType()), T.StructField(\"season\", T.DoubleType())]))",
            "D. spark.newDataFrame([(\"summer\", 4.5), (\"winter\", 7.5)], [\"season\", \"wind_speed_ms\"])",
            "E. spark.createDataFrame({\"season\": [\"winter\",\"summer\"], \"wind_speed_ms\": [4.5, 7.5]})"
        ],
        "answer": [
            "B"
        ],
        "explanation": "spark.createDataFrame([(\"summer\", 4.5), (\"winter\", 7.5)], [\"season\", \"wind_speed_ms\"]) Correct.\nThis command uses the Spark Session's createDataFrame method to create a new DataFrame. Notice\nhow rows, columns, and column names are passed in here: The rows are specified as a Python list.\nEvery entry in the list is a new row. Columns are specified as Python tuples (for example (\"summer\",\n4.5)). Every column is one entry in the tuple.\nThe column names are specified as the second argument to createDataFrame(). The documentation\n(link below) shows that \"when schema is a list of column names, the type of each column will be\ninferred from data\" (the first argument). Since values 4.5 and 7.5 are both float variables, Spark will\ncorrectly infer the double type for column wind_speed_ms. Given that all values in column\n\"season\" contain only strings, Spark will cast the column appropriately as string.\nFind out more about SparkSession.createDataFrame() via the link below.\nspark.newDataFrame([(\"summer\", 4.5), (\"winter\", 7.5)], [\"season\", \"wind_speed_ms\"]) No, the\nSparkSession does not have a newDataFrame method.\nfrom pyspark.sql import types as T\nspark.createDataFrame(((\"summer\", 4.5), (\"winter\", 7.5)), T.StructType([T.StructField(\"season\",\nT.CharType()), T.StructField(\"season\", T.DoubleType())]))\nNo. pyspark.sql.types does not have a CharType type. See link below for available data types in Spark.\nspark.createDataFrame({\"season\": [\"winter\",\"summer\"], \"wind_speed_ms\": [4.5, 7.5]}) No, this is not\ncorrect Spark syntax. If you have considered this option to be correct, you may have some experience\nwith Python's pandas package, in which this would be correct syntax. To create a Spark DataFrame\nfrom a Pandas DataFrame, you can simply use spark.createDataFrame(pandasDf) where pandasDf is\nthe Pandas DataFrame.\nFind out more about Spark syntax options using the examples in the documentation for\nSparkSession.createDataFrame linked below.\nspark.DataFrame({\"season\": [\"winter\",\"summer\"], \"wind_speed_ms\": [4.5, 7.5]}) No, the Spark\nSession (indicated by spark in the code above) does not have a DataFrame method.\nMore info: pyspark.sql.SparkSession.createDataFrame - PySpark 3.1.1 documentation and Data Types\n- Spark 3.1.2 Documentation Static notebook | Dynamic notebook: See test 1"
    },
    {
        "question": "Which of the following describes Spark's standalone deployment mode?",
        "options": [
            "A. Standalone mode uses a single JVM to run Spark driver and executor processes.",
            "B. Standalone mode means that the cluster does not contain the driver.",
            "C. Standalone mode is how Spark runs on YARN and Mesos clusters.",
            "D. Standalone mode uses only a single executor per worker per application.",
            "E. Standalone mode is a viable solution for clusters that run multiple frameworks, not only Spark."
        ],
        "answer": [
            "D"
        ],
        "explanation": "Standalone mode uses only a single executor per worker per application.\nThis is correct and a limitation of Spark's standalone mode.\nStandalone mode is a viable solution for clusters that run multiple frameworks.\nIncorrect. A limitation of standalone mode is that Apache Spark must be the only framework running\non the cluster. If you would want to run multiple frameworks on the same cluster in parallel, for\nexample Apache Spark and Apache Flink, you would consider the YARN deployment mode.\nStandalone mode uses a single JVM to run Spark driver and executor processes.\nNo, this is what local mode does.\nStandalone mode is how Spark runs on YARN and Mesos clusters.\nNo. YARN and Mesos modes are two deployment modes that are different from standalone mode.\nThese modes allow Spark to run alongside other frameworks on a cluster. When Spark is run in\nstandalone mode, only the Spark framework can run on the cluster.\nStandalone mode means that the cluster does not contain the driver.\nIncorrect, the cluster does not contain the driver in client mode, but in standalone mode the driver\nruns on a node in the cluster.\nMore info: Learning Spark, 2nd Edition, Chapter 1"
    },
    {
        "question": "Which of the following describes a shuffle?",
        "options": [
            "A. A shuffle is a process that is executed during a broadcast hash join.",
            "B. A shuffle is a process that compares data across executors.",
            "C. A shuffle is a process that compares data across partitions.",
            "D. A shuffle is a Spark operation that results from DataFrame.coalesce().",
            "E. A shuffle is a process that allocates partitions to executors."
        ],
        "answer": [
            "C"
        ],
        "explanation": "A shuffle is a Spark operation that results from DataFrame.coalesce().\nNo. DataFrame.coalesce() does not result in a shuffle.\nA shuffle is a process that allocates partitions to executors.\nThis is incorrect.\nA shuffle is a process that is executed during a broadcast hash join.\nNo, broadcast hash joins avoid shuffles and yield performance benefits if at least one of the two\ntables is small in size (<= 10 MB by default). Broadcast hash joins can avoid shuffles because instead\nof exchanging partitions between executors, they broadcast a small table to all executors that then\nperform the rest of the join operation locally.\nA shuffle is a process that compares data across executors.\nNo, in a shuffle, data is compared across partitions, and not executors.\nMore info: Spark Repartition & Coalesce - Explained (https://bit.ly/32KF7zS)"
    },
    {
        "question": "Which of the following code blocks reads JSON file imports.json into a DataFrame?",
        "options": [
            "A. spark.read().mode(\"json\").path(\"/FileStore/imports.json\")",
            "B. spark.read.format(\"json\").path(\"/FileStore/imports.json\")",
            "C. spark.read(\"json\", \"/FileStore/imports.json\")",
            "D. spark.read.json(\"/FileStore/imports.json\")",
            "E. spark.read().json(\"/FileStore/imports.json\")"
        ],
        "answer": [
            "D"
        ],
        "explanation": "Static notebook | Dynamic notebook: See test 1\n(https://flrs.github.io/spark_practice_tests_code/#1/25.html ,\nhttps://bit.ly/sparkpracticeexams_import_instructions)"
    },
    {
        "question": "The code block displayed below contains an error. The code block is intended to return all\ncolumns of DataFrame transactionsDf except for columns predError, productId, and value. Find the\nerror.\nExcerpt of DataFrame transactionsDf:\ntransactionsDf.select(~col(\"predError\"), ~col(\"productId\"), ~col(\"value\"))",
        "options": [
            "A. The select operator should be replaced by the drop operator and the arguments to the drop\noperator should be column names predError, productId and value wrapped in the col operator so\nthey should be expressed like drop(col(predError), col(productId), col(value)).",
            "B. The select operator should be replaced with the deselect operator.",
            "C. The column names in the select operator should not be strings and wrapped in the col operator, so\nthey should be expressed like select(~col(predError), ~col(productId), ~col(value)).",
            "D. The select operator should be replaced by the drop operator.",
            "E. The select operator should be replaced by the drop operator and the arguments to the drop\noperator should be column names predError, productId and value as strings."
        ],
        "answer": [
            "E"
        ],
        "explanation": "Correct code block:\ntransactionsDf.drop(\"predError\", \"productId\", \"value\")\nStatic notebook | Dynamic notebook: See test 1"
    },
    {
        "question": "The code block displayed below contains an error. The code block should return a new\nDataFrame that only contains rows from DataFrame transactionsDf in which the value in column\npredError is at least 5. Find the error.\nCode block:\ntransactionsDf.where(\"col(predError) >= 5\")",
        "options": [
            "A. The argument to the where method should be \"predError >= 5\".",
            "B. Instead of where(), filter() should be used.",
            "C. The expression returns the original DataFrame transactionsDf and not a new DataFrame. To avoid\nthis, the code block should be transactionsDf.toNewDataFrame().where(\"col(predError) >= 5\").",
            "D. The argument to the where method cannot be a string.",
            "E. Instead of >=, the SQL operator GEQ should be used."
        ],
        "answer": [
            "A"
        ],
        "explanation": "The argument to the where method cannot be a string.\nIt can be a string, no problem here.\nInstead of where(), filter() should be used.\nNo, that does not matter. In PySpark, where() and filter() are equivalent.\nInstead of >=, the SQL operator GEQ should be used.\n\n\nIncorrect.\nThe expression returns the original DataFrame transactionsDf and not a new DataFrame. To avoid\nthis, the code block should be transactionsDf.toNewDataFrame().where(\"col(predError) >= 5\").\nNo, Spark returns a new DataFrame.\nStatic notebook | Dynamic notebook: See test 1\n(https://flrs.github.io/spark_practice_tests_code/#1/27.html ,\nhttps://bit.ly/sparkpracticeexams_import_instructions)"
    },
    {
        "question": "Which of the following statements about reducing out-of-memory errors is incorrect?",
        "options": [
            "A. Concatenating multiple string columns into a single column may guard against out-of-memory\nerrors.",
            "B. Reducing partition size can help against out-of-memory errors.",
            "C. Limiting the amount of data being automatically broadcast in joins can help against out-of-\nmemory errors.",
            "D. Setting a limit on the maximum size of serialized data returned to the driver may help prevent out-\nof-memory errors.",
            "E. Decreasing the number of cores available to each executor can help against out-of-memory errors."
        ],
        "answer": [
            "A"
        ],
        "explanation": "Concatenating multiple string columns into a single column may guard against out-of-memory errors.\nExactly, this is an incorrect answer! Concatenating any string columns does not reduce the size of the\ndata, it just structures it a different way. This does little to how Spark processes the data and\ndefinitely does not reduce out-of-memory errors.\nReducing partition size can help against out-of-memory errors.\nNo, this is not incorrect. Reducing partition size is a viable way to aid against out-of-memory errors,\nsince executors need to load partitions into memory before processing them. If the executor does\nnot have enough memory available to do that, it will throw an out-of-memory error. Decreasing\npartition size can therefore be very helpful for preventing that.\nDecreasing the number of cores available to each executor can help against out-of-memory errors.\nNo, this is not incorrect. To process a partition, this partition needs to be loaded into the memory of\nan executor. If you imagine that every core in every executor processes a partition, potentially in\nparallel with other executors, you can imagine that memory on the machine hosting the executors\nfills up quite quickly. So, memory usage of executors is a concern, especially when multiple partitions\nare processed at the same time. To strike a balance between performance and memory usage,\ndecreasing the number of cores may help against out-of-memory errors.\nSetting a limit on the maximum size of serialized data returned to the driver may help prevent out-of-\nmemory errors.\nNo, this is not incorrect. When using commands like collect() that trigger the transmission of\npotentially large amounts of data from the cluster to the driver, the driver may experience out-of-\nmemory errors. One strategy to avoid this is to be careful about using commands like collect() that\nsend back large amounts of data to the driver. Another strategy is setting the parameter\nspark.driver.maxResultSize. If data to be transmitted to the driver exceeds the threshold specified by\nthe parameter, Spark will abort the job and therefore prevent an out-of-memory error.\nLimiting the amount of data being automatically broadcast in joins can help against out-of-memory\nerrors.\nWrong, this is not incorrect. As part of Spark's internal optimization, Spark may choose to speed up\n\n\noperations by broadcasting (usually relatively small) tables to executors. This broadcast is happening\nfrom the driver, so all the broadcast tables are loaded into the driver first. If these tables are\nrelatively big, or multiple mid-size tables are being broadcast, this may lead to an out-of- memory\nerror. The maximum table size for which Spark will consider broadcasting is set by the\nspark.sql.autoBroadcastJoinThreshold parameter.\nMore info: Configuration - Spark 3.1.2 Documentation and Spark OOM Error - Closeup. Does th\ne following look familiar when... | by Amit Singh Rathore | The Startup | Medium"
    },
    {
        "question": "The code block displayed below contains an error. The code block below is intended to add\na column itemNameElements to DataFrame itemsDf that includes an array of all words in column\nitemName. Find the error.\nSample of DataFrame itemsDf:\n1.+------+----------------------------------+-------------------+\n2.|itemId|itemName |supplier |\n3.+------+----------------------------------+-------------------+\n4.|1 |Thick Coat for Walking in the Snow|Sports Company Inc.|\n5.|2 |Elegant Outdoors Summer Dress |YetiX |\n6.|3 |Outdoors Backpack |Sports Company Inc.|\n7.+------+----------------------------------+-------------------+\nCode block:\nitemsDf.withColumnRenamed(\"itemNameElements\", split(\"itemName\"))\nitemsDf.withColumnRenamed(\"itemNameElements\", split(\"itemName\"))",
        "options": [
            "A. All column names need to be wrapped in the col() operator.",
            "B. Operator withColumnRenamed needs to be replaced with operator withColumn and a second\nargument\n\",\" needs to be passed to the split method.",
            "C. Operator withColumnRenamed needs to be replaced with operator withColumn and the split\nmethod needs to be replaced by the splitString method.",
            "D. Operator withColumnRenamed needs to be replaced with operator withColumn and a second\nargument \"\n\" needs to be passed to the split method.",
            "E. The expressions \"itemNameElements\" and split(\"itemName\") need to be swapped."
        ],
        "answer": [
            "D"
        ],
        "explanation": "Correct code block:\nitemsDf.withColumn(\"itemNameElements\", split(\"itemName\",\" \"))\nOutput of code block:\n+------+----------------------------------+-------------------+------------------------------------------+\n|itemId|itemName |supplier |itemNameElements |\n+------+----------------------------------+-------------------+------------------------------------------+\n|1 |Thick Coat for Walking in the Snow|Sports Company Inc.|[Thick, Coat, for, Walking, in, the,\nSnow]|\n|2 |Elegant Outdoors Summer Dress |YetiX |[Elegant, Outdoors, Summer, Dress] |\n|3 |Outdoors Backpack |Sports Company Inc.|[Outdoors, Backpack] |\n+------+----------------------------------+-------------------+------------------------------------------+ The key to solving\n\n\nthis question is that the split method definitely needs a second argument here (also look at the link to\nthe documentation below). Given the values in column itemName in DataFrame itemsDf, this should\nbe a space character \" \". This is the character we need to split the words in the column.\nMore info: pyspark.sql.functions.split - PySpark 3.1.1 documentation\nStatic notebook | Dynamic notebook: See test 1"
    },
    {
        "question": "Which of the following describes how Spark achieves fault tolerance?",
        "options": [
            "A. Spark helps fast recovery of data in case of a worker fault by providing the MEMORY_AND_DISK\nstorage level option.",
            "B. If an executor on a worker node fails while calculating an RDD, that RDD can be recomputed by\nanother executor using the lineage.",
            "C. Spark builds a fault-tolerant layer on top of the legacy RDD data system, which by itself is not fault\ntolerant.",
            "D. Due to the mutability of DataFrames after transformations, Spark reproduces them using\nobserved lineage in case of worker node failure.",
            "E. Spark is only fault-tolerant if this feature is specifically enabled via the\nspark.fault_recovery.enabled property."
        ],
        "answer": [
            "B"
        ],
        "explanation": "Due to the mutability of DataFrames after transformations, Spark reproduces them using observed\nlineage in case of worker node failure.\nWrong - Between transformations, DataFrames are immutable. Given that Spark also records the\nlineage, Spark can reproduce any DataFrame in case of failure. These two aspects are the key to\nunderstanding fault tolerance in Spark.\nSpark builds a fault-tolerant layer on top of the legacy RDD data system, which by itself is not fault\ntolerant.\nWrong. RDD stands for Resilient Distributed Dataset and it is at the core of Spark and not a \"legacy\nsystem\".\nIt is fault-tolerant by design.\nSpark helps fast recovery of data in case of a worker fault by providing the MEMORY_AND_DISK\nstorage level option.\nThis is not true. For supporting recovery in case of worker failures, Spark provides \"_2\", \"_3\", and so\non, storage level options, for example MEMORY_AND_DISK_2. These storage levels are specifically\ndesigned to keep duplicates of the data on multiple nodes. This saves time in case of a worker fault,\nsince a copy of the data can be used immediately, vs. having to recompute it first.\nSpark is only fault-tolerant if this feature is specifically enabled via the spark.fault_recovery.enabled\nproperty.\nNo, Spark is fault-tolerant by design."
    },
    {
        "question": "The code block shown below should return a new 2-column DataFrame that shows one\nattribute from column attributes per row next to the associated itemName, for all suppliers in\ncolumn supplier whose name includes Sports. Choose the answer that correctly fills the blanks in the\ncode block to accomplish this.\nSample of DataFrame itemsDf:\n1.+------+----------------------------------+-----------------------------+-------------------+\n\n\n2.|itemId|itemName |attributes |supplier |\n3.+------+----------------------------------+-----------------------------+-------------------+\n4.|1 |Thick Coat for Walking in the Snow|[blue, winter, cozy] |Sports Company Inc.|\n5.|2 |Elegant Outdoors Summer Dress |[red, summer, fresh, cooling]|YetiX |\n6.|3 |Outdoors Backpack |[green, summer, travel] |Sports Company Inc.|\n7.+------+----------------------------------+-----------------------------+-------------------+ Code block:\nitemsDf.__1__(__2__).select(__3__, __4__)",
        "options": [
            "A. 1. filter\n2. col(\"supplier\").isin(\"Sports\")\n3. \"itemName\"\n4. explode(col(\"attributes\"))",
            "B. 1. where\n2. col(\"supplier\").contains(\"Sports\")\n3. \"itemName\"\n4. \"attributes\"",
            "C. 1. where\n2. col(supplier).contains(\"Sports\")\n3. explode(attributes)\n4. itemName",
            "D. 1. where\n2. \"Sports\".isin(col(\"Supplier\"))\n3. \"itemName\"\n4. array_explode(\"attributes\")",
            "E. 1. filter\n2. col(\"supplier\").contains(\"Sports\")\n3. \"itemName\"\n4. explode(\"attributes\")"
        ],
        "answer": [
            "E"
        ],
        "explanation": "Output of correct code block:\n+----------------------------------+------+\n|itemName |col |\n+----------------------------------+------+\n|Thick Coat for Walking in the Snow|blue |\n|Thick Coat for Walking in the Snow|winter|\n|Thick Coat for Walking in the Snow|cozy |\n|Outdoors Backpack |green |\n|Outdoors Backpack |summer|\n|Outdoors Backpack |travel|\n+----------------------------------+------+\nThe key to solving this question is knowing about Spark's explode operator. Using this operator, you\ncan extract values from arrays into single rows. The following guidance steps through the answers\nsystematically from the first to the last gap. Note that there are many ways to solving the gap\nquestions and filtering out wrong answers, you do not always have to start filtering out from the first\ngap, but can also exclude some answers based on obvious problems you see with them.\nThe answers to the first gap present you with two options: filter and where. These two are actually\n\n\nsynonyms in PySpark, so using either of those is fine. The answer options to this gap therefore do not\nhelp us in selecting the right answer.\nThe second gap is more interesting. One answer option includes \"Sports\".isin(col(\"Supplier\")). This\nconstruct does not work, since Python's string does not have an isin method. Another option contains\ncol(supplier). Here, Python will try to interpret supplier as a variable. We have not set this variable, so\nthis is not a viable answer. Then, you are left with answers options that include col\n(\"supplier\").contains(\"Sports\") and col(\"supplier\").isin(\"Sports\"). The question states that we are\nlooking for suppliers whose name includes Sports, so we have to go for the contains operator here.\nWe would use the isin operator if we wanted to filter out for supplier names that match any entries\nin a list of supplier names.\nFinally, we are left with two answers that fill the third gap both with \"itemName\" and the fourth gap\neither with explode(\"attributes\") or \"attributes\". While both are correct Spark syntax, only explode\n(\"attributes\") will help us achieve our goal. Specifically, the question asks for one attribute from\ncolumn attributes per row - this is what the explode() operator does.\nOne answer option also includes array_explode() which is not a valid operator in PySpark.\nMore info: pyspark.sql.functions.explode - PySpark 3.1.2 documentation\nStatic notebook | Dynamic notebook: See test 3"
    },
    {
        "question": "Which of the following DataFrame methods is classified as a transformation?",
        "options": [
            "A. DataFrame.count()",
            "B. DataFrame.show()",
            "C. DataFrame.select()",
            "D. DataFrame.foreach()",
            "E. DataFrame.first()"
        ],
        "answer": [
            "C"
        ],
        "explanation": "DataFrame.select()\nCorrect, DataFrame.select() is a transformation. When the command is executed, it is evaluated lazily\nand returns an RDD when it is triggered by an action.\nDataFrame.foreach()\nIncorrect, DataFrame.foreach() is not a transformation, but an action. The intention of foreach() is to\napply code to each element of a DataFrame to update accumulator variables or write the elements to\nexternal storage. The process does not return an RDD - it is an action!\nDataFrame.first()\nWrong. As an action, DataFrame.first() executed immediately and returns the first row of a\nDataFrame.\nDataFrame.count()\nIncorrect. DataFrame.count() is an action and returns the number of rows in a DataFrame.\nDataFrame.show()\nNo, DataFrame.show() is an action and displays the DataFrame upon execution of the command."
    },
    {
        "question": "Which of the following statements about RDDs is incorrect?",
        "options": [
            "A. An RDD consists of a single partition.",
            "B. The high-level DataFrame API is built on top of the low-level RDD API.",
            "C. RDDs are immutable.",
            "D. RDD stands for Resilient Distributed Dataset.",
            "E. RDDs are great for precisely instructing Spark on how to do a query."
        ],
        "answer": [
            "A"
        ],
        "explanation": "An RDD consists of a single partition.\nQuite the opposite: Spark partitions RDDs and distributes the partitions across multiple nodes."
    },
    {
        "question": "Which of the following describes properties of a shuffle?",
        "options": [
            "A. Operations involving shuffles are never evaluated lazily.",
            "B. Shuffles involve only single partitions.",
            "C. Shuffles belong to a class known as \"full transformations\".",
            "D. A shuffle is one of many actions in Spark.",
            "E. In a shuffle, Spark writes data to disk."
        ],
        "answer": [
            "E"
        ],
        "explanation": "In a shuffle, Spark writes data to disk.\nCorrect! Spark's architecture dictates that intermediate results during a shuffle are written to disk.\nA shuffle is one of many actions in Spark.\nIncorrect. A shuffle is a transformation, but not an action.\nShuffles involve only single partitions.\nNo, shuffles involve multiple partitions. During a shuffle, Spark generates output partitions from\nmultiple input partitions.\nOperations involving shuffles are never evaluated lazily.\nWrong. A shuffle is a costly operation and Spark will evaluate it as lazily as other transformations.\nThis is, until a subsequent action triggers its evaluation.\nShuffles belong to a class known as \"full transformations\".\nNot quite. Shuffles belong to a class known as \"wide transformations\". \"Full transformation\" is not a\nrelevant term in Spark.\nMore info: Spark - The Definitive Guide, Chapter 2 and Spark: disk I/O on stage boundaries\nexplanation - Stack Overflow"
    },
    {
        "question": "Which of the following statements about Spark's DataFrames is incorrect?",
        "options": [
            "A. Spark's DataFrames are immutable.",
            "B. Spark's DataFrames are equal to Python's DataFrames.",
            "C. Data in DataFrames is organized into named columns.",
            "D. RDDs are at the core of DataFrames.",
            "E. The data in DataFrames may be split into multiple chunks."
        ],
        "answer": [
            "B"
        ],
        "explanation": "Spark's DataFrames are equal to Python's or R's DataFrames.\nNo, they are not equal. They are only similar. A major difference between Spark and Python is that\nSpark's DataFrames are distributed, whereby Python's are not."
    },
    {
        "question": "Which of the following code blocks returns approximately 1000 rows, some of them\npotentially being duplicates, from the 2000-row DataFrame transactionsDf that only has unique\n\n\nrows?",
        "options": [
            "A. transactionsDf.sample(True, 0.5)",
            "B. transactionsDf.take(1000).distinct()",
            "C. transactionsDf.sample(False, 0.5)",
            "D. transactionsDf.take(1000)",
            "E. transactionsDf.sample(True, 0.5, force=True)"
        ],
        "answer": [
            "A"
        ],
        "explanation": "To solve this question, you need to know that DataFrame.sample() is not guaranteed to return the\nexact fraction of the number of rows specified as an argument. Furthermore, since duplicates may be\nreturned, you should understand that the operator's withReplacement argument should be set to\nTrue. A force= argument for the operator does not exist.\nWhile the take argument returns an exact number of rows, it will just take the first specified number\nof rows (1000 in this question) from the DataFrame. Since the DataFrame does not include duplicate\nrows, there is no potential of any of those returned rows being duplicates when using take(), so the\ncorrect answer cannot involve take().\nMore info: pyspark.sql.DataFrame.sample - PySpark 3.1.2 documentation\nStatic notebook | Dynamic notebook: See test 2"
    },
    {
        "question": "The code block displayed below contains an error. The code block should read the csv file\nlocated at path data/transactions.csv into DataFrame transactionsDf, using the first row as column\nheader and casting the columns in the most appropriate type. Find the error.\nFirst 3 rows of transactions.csv:\n1.transactionId;storeId;productId;name\n2.1;23;12;green grass\n3.2;35;31;yellow sun\n4.3;23;12;green grass\nCode block:\ntransactionsDf = spark.read.load(\"data/transactions.csv\", sep=\";\", format=\"csv\", header=True)",
        "options": [
            "A. The DataFrameReader is not accessed correctly.",
            "B. The transaction is evaluated lazily, so no file will be read.",
            "C. Spark is unable to understand the file type.",
            "D. The code block is unable to capture all columns.",
            "E. The resulting DataFrame will not have the appropriate schema."
        ],
        "answer": [
            "E"
        ],
        "explanation": "Correct code block:\ntransactionsDf = spark.read.load(\"data/transactions.csv\", sep=\";\", format=\"csv\", header=True,\ninferSchema=True) By default, Spark does not infer the schema of the CSV (since this usually takes\nsome time). So, you need to add the inferSchema=True option to the code block.\nMore info: pyspark.sql.DataFrameReader.csv - PySpark 3.1.2 documentation"
    },
    {
        "question": "Which of the following code blocks displays the 10 rows with the smallest values of column\nvalue in DataFrame transactionsDf in a nicely formatted way?",
        "options": [
            "A. transactionsDf.sort(asc(value)).show(10)",
            "B. transactionsDf.sort(col(\"value\")).show(10)",
            "C. transactionsDf.sort(col(\"value\").desc()).head()",
            "D. transactionsDf.sort(col(\"value\").asc()).print(10)",
            "E. transactionsDf.orderBy(\"value\").asc().show(10)"
        ],
        "answer": [
            "B"
        ],
        "explanation": "show() is the correct method to look for here, since the question specifically asks for displaying the\nrows in a nicely formatted way. Here is the output of show (only a few rows shown):\n+-------------+---------+-----+-------+---------+----+---------------+\n|transactionId|predError|value|storeId|productId| f|transactionDate|\n+-------------+---------+-----+-------+---------+----+---------------+\n| 3| 3| 1| 25| 3|null| 1585824821|\n| 5| null| 2| null| 2|null| 1575285427|\n| 4| null| 3| 3| 2|null| 1583244275|\n+-------------+---------+-----+-------+---------+----+---------------+\nWith regards to the sorting, specifically in ascending order since the smallest values should be shown\nfirst, the following expressions are valid:\n- transactionsDf.sort(col(\"value\")) (\"ascending\" is the default sort direction in the sort method)\n- transactionsDf.sort(asc(col(\"value\")))\n- transactionsDf.sort(asc(\"value\"))\n- transactionsDf.sort(transactionsDf.value.asc())\n- transactionsDf.sort(transactionsDf.value)\nAlso, orderBy is just an alias of sort, so all of these expressions work equally well using orderBy.\nStatic notebook | Dynamic notebook: See test 1"
    },
    {
        "question": "Which is the highest level in Spark's execution hierarchy?",
        "options": [
            "A. Task",
            "B. Executor",
            "C. Slot",
            "D. Job",
            "E. Stage"
        ],
        "answer": [
            "D"
        ]
    },
    {
        "question": "The code block displayed below contains an error. The code block should configure Spark so that DataFrames up to a size of 20 MB will be broadcast to all worker nodes when performing a join. Find the error.\n\nCode block:\nspark.conf.set(\"spark.sql.autoBroadcastJoinThreshold\", 20)",
        "options": [
            "A. Spark will only broadcast DataFrames that are much smaller than the default value.",
            "B. The correct option to write configurations is through spark.config and not spark.conf.",
            "C. Spark will only apply the limit to threshold joins and not to other joins.",
            "D. The passed limit has the wrong variable type.",
            "E. The command is evaluated lazily and needs to be followed by an action."
        ],
        "answer": [
            "A"
        ],
        "explanation": "This is question is hard. Let's assess the different answers one-by-one.\nSpark will only broadcast DataFrames that are much smaller than the default value.\nThis is correct. The default value is 10 MB (10485760 bytes). Since the configuration for\nspark.sql.autoBroadcastJoinThreshold expects a number in bytes (and not megabytes), the code\nblock sets the limits to merely 20 bytes, instead of the requested 20 * 1024 * 1024 (= 20971520)\nbytes.\nThe command is evaluated lazily and needs to be followed by an action.\nNo, this command is evaluated right away!\nSpark will only apply the limit to threshold joins and not to other joins.\nThere are no \"threshold joins\", so this option does not make any sense.\nThe correct option to write configurations is through spark.config and not spark.conf.\nNo, it is indeed spark.conf!\nThe passed limit has the wrong variable type.\nThe configuration expects the number of bytes, a number, as an input. So, the 20 provided in the\ncode block is fine."
    },
    {
        "question": "Which of the following code blocks returns a DataFrame with approximately 1,000 rows\nfrom the 10,000-row DataFrame itemsDf, without any duplicates, returning the same rows even if\nthe code block is run twice?",
        "options": [
            "A. itemsDf.sampleBy(\"row\", fractions={0: 0.1}, seed=82371)",
            "B. itemsDf.sample(fraction=0.1, seed=87238)",
            "C. itemsDf.sample(fraction=1000, seed=98263)",
            "D. itemsDf.sample(withReplacement=True, fraction=0.1, seed=23536)",
            "E. itemsDf.sample(fraction=0.1)"
        ],
        "answer": [
            "B"
        ],
        "explanation": "itemsDf.sample(fraction=0.1, seed=87238)\nCorrect. If itemsDf has 10,000 rows, this code block returns about 1,000, since DataFrame.sample() is\nnever guaranteed to return an exact amount of rows. To ensure you are not returning duplicates, you\nshould leave the withReplacement parameter at False, which is the default. Since the question\nspecifies that the same rows should be returned even if the code block is run twice, you need to\nspecify a seed. The number passed in the seed does not matter as long as it is an integer.\nitemsDf.sample(withReplacement=True, fraction=0.1, seed=23536)\nIncorrect. While this code block fulfills almost all requirements, it may return duplicates. This is\nbecause withReplacement is set to True.\nHere is how to understand what replacement means: Imagine you have a bucket of 10,000 numbered\nballs and you need to take 1,000 balls at random from the bucket (similar to the problem in the\nquestion). Now, if you would take those balls with replacement, you would take a ball, note its\nnumber, and put it back into the bucket, meaning the next time you take a ball from the bucket there\nwould be a chance you could take the exact same ball again. If you took the balls without\nreplacement, you would leave the ball outside the bucket and not put it back in as you take the next\n999 balls.\nitemsDf.sample(fraction=1000, seed=98263)\nWrong. The fraction parameter needs to have a value between 0 and 1. In this case, it should be 0.1,\nsince\n\n\n1,000/10,000 = 0.1.\nitemsDf.sampleBy(\"row\", fractions={0: 0.1}, seed=82371)\nNo, DataFrame.sampleBy() is meant for stratified sampling. This means that based on the values in a\ncolumn in a DataFrame, you can draw a certain fraction of rows containing those values from the\nDataFrame (more details linked below). In the scenario at hand, sampleBy is not the right operator to\nuse because you do not have any information about any column that the sampling should depend on.\nitemsDf.sample(fraction=0.1)\nIncorrect. This code block checks all the boxes except that it does not ensure that when you run it a\nsecond time, the exact same rows will be returned. In order to achieve this, you would have to\nspecify a seed.\nMore info:\n- pyspark.sql.DataFrame.sample - PySpark 3.1.2 documentation\n- pyspark.sql.DataFrame.sampleBy - PySpark 3.1.2 documentation\n- Types of Samplings in PySpark 3. The explanations of the sampling... | by Pinar Ersoy | Towards Data\nScience"
    },
    {
        "question": "Which of the following statements about DAGs is correct?",
        "options": [
            "A. DAGs help direct how Spark executors process tasks, but are a limitation to the proper execution\nof a query when an executor fails.",
            "B. DAG stands for \"Directing Acyclic Graph\".",
            "C. Spark strategically hides DAGs from developers, since the high degree of automation in Spark\nmeans that developers never need to consider DAG layouts.",
            "D. In contrast to transformations, DAGs are never lazily executed.",
            "E. DAGs can be decomposed into tasks that are executed in parallel."
        ],
        "answer": [
            "E"
        ],
        "explanation": "DAG stands for \"Directing Acyclic Graph\".\nNo, DAG stands for \"Directed Acyclic Graph\".\nSpark strategically hides DAGs from developers, since the high degree of automation in Spark means\nthat developers never need to consider DAG layouts.\nNo, quite the opposite. You can access DAGs through the Spark UI and they can be of great help\nwhen optimizing queries manually.\nIn contrast to transformations, DAGs are never lazily executed.\nDAGs represent the execution plan in Spark and as such are lazily executed when the driver requests\nthe data processed in the DAG."
    },
    {
        "question": "The code block displayed below contains an error. The code block should count the number\nof rows that have a predError of either 3 or 6. Find the error.\nCode block:\ntransactionsDf.filter(col('predError').in([3, 6])).count()",
        "options": [
            "A. The number of rows cannot be determined with the count() operator.",
            "B. Instead of filter, the select method should be used.",
            "C. The method used on column predError is incorrect.",
            "D. Instead of a list, the values need to be passed as single arguments to the in operator.",
            "E. Numbers 3 and 6 need to be passed as string variables."
        ],
        "answer": [
            "C"
        ],
        "explanation": "Correct code block:\ntransactionsDf.filter(col('predError').isin([3, 6])).count()\nThe isin method is the correct one to use here - the in method does not exist for the Column object.\nMore info: pyspark.sql.Column.isin - PySpark 3.1.2 documentation"
    },
    {
        "question": "Which of the following code blocks shuffles DataFrame transactionsDf, which has 8\npartitions, so that it has\n10 partitions?",
        "options": [
            "A. transactionsDf.repartition(transactionsDf.getNumPartitions()+2)",
            "B. transactionsDf.repartition(transactionsDf.rdd.getNumPartitions()+2)",
            "C. transactionsDf.coalesce(10)",
            "D. transactionsDf.coalesce(transactionsDf.getNumPartitions()+2)",
            "E. transactionsDf.repartition(transactionsDf._partitions+2)"
        ],
        "answer": [
            "B"
        ],
        "explanation": "transactionsDf.repartition(transactionsDf.rdd.getNumPartitions()+2)\nCorrect. The repartition operator is the correct one for increasing the number of partitions. calling\ngetNumPartitions() on DataFrame.rdd returns the current number of partitions.\ntransactionsDf.coalesce(10)\nNo, after this command transactionsDf will continue to only have 8 partitions. This is because\ncoalesce() can only decreast the amount of partitions, but not increase it.\ntransactionsDf.repartition(transactionsDf.getNumPartitions()+2)\nIncorrect, there is no getNumPartitions() method for the DataFrame class.\ntransactionsDf.coalesce(transactionsDf.getNumPartitions()+2)\nWrong, coalesce() can only be used for reducing the number of partitions and there is no\ngetNumPartitions() method for the DataFrame class.\ntransactionsDf.repartition(transactionsDf._partitions+2)\nNo, DataFrame has no _partitions attribute. You can find out the current number of partitions of a\nDataFrame with the DataFrame.rdd.getNumPartitions() method.\nMore info: pyspark.sql.DataFrame.repartition - PySpark 3.1.2 documentation,\npyspark.RDD.getNumPartitions - PySpark 3.1.2 documentation Static notebook | Dynamic notebook:\nSee test 3"
    },
    {
        "question": "Which of the following code blocks selects all rows from DataFrame transactionsDf in which\ncolumn productId is zero or smaller or equal to 3?",
        "options": [
            "A. transactionsDf.filter(productId==3 or productId<1)",
            "B. transactionsDf.filter((col(\"productId\")==3) or (col(\"productId\")<1))",
            "C. transactionsDf.filter(col(\"productId\")==3 | col(\"productId\")<1)",
            "D. transactionsDf.where(\"productId\"=3).or(\"productId\"<1))",
            "E. transactionsDf.filter((col(\"productId\")==3) | (col(\"productId\")<1))"
        ],
        "answer": [
            "E"
        ],
        "explanation": "This question targets your knowledge about how to chain filtering conditions. Each filtering condition\n\n\nshould be in parentheses. The correct operator for \"or\" is the pipe character (|) and not the word or.\nAnother operator of concern is the equality operator. For the purpose of comparison, equality is\nexpressed as two equal signs (==).\nStatic notebook | Dynamic notebook: See test 2"
    },
    {
        "question": "Which of the following code blocks silently writes DataFrame itemsDf in avro format to\nlocation fileLocation if a file does not yet exist at that location?",
        "options": [
            "A. itemsDf.write.avro(fileLocation)",
            "B. itemsDf.write.format(\"avro\").mode(\"ignore\").save(fileLocation)",
            "C. itemsDf.write.format(\"avro\").mode(\"errorifexists\").save(fileLocation)",
            "D. itemsDf.save.format(\"avro\").mode(\"ignore\").write(fileLocation)",
            "E. spark.DataFrameWriter(itemsDf).format(\"avro\").write(fileLocation)"
        ],
        "answer": [
            "A"
        ],
        "explanation": "The trick in this question is knowing the \"modes\" of the DataFrameWriter. Mode ignore will ignore if\na file already exists and not replace that file, but also not throw an error. Mode errorifexists will\nthrow an error, and is the default mode of the DataFrameWriter. The question NO:\nexplicitly calls for the DataFrame to be \"silently\" written if it does not exist, so you need to specify\nmode(\"ignore\") here to avoid having Spark communicate any error to you if the file already exists.\nThe `overwrite' mode would not be right here, since, although it would be silent, it would overwrite\nthe already-existing file. This is not what the question asks for.\nIt is worth noting that the option starting with spark.DataFrameWriter(itemsDf) cannot work, since\nspark references the SparkSession object, but that object does not provide the DataFrameWriter.\nAs you can see in the documentation (below), DataFrameWriter is part of PySpark's SQL API, but not\nof its SparkSession API.\nMore info:\nDataFrameWriter: pyspark.sql.DataFrameWriter.save - PySpark 3.1.1 documentation SparkSession\nAPI: Spark SQL - PySpark 3.1.1 documentation Static notebook | Dynamic notebook: See test 1"
    },
    {
        "question": "Which of the following describes the role of the cluster manager?",
        "options": [
            "A. The cluster manager schedules tasks on the cluster in client mode.",
            "B. The cluster manager schedules tasks on the cluster in local mode.",
            "C. The cluster manager allocates resources to Spark applications and maintains the executor\nprocesses in client mode.",
            "D. The cluster manager allocates resources to Spark applications and maintains the executor\nprocesses in remote mode.",
            "E. The cluster manager allocates resources to the DataFrame manager."
        ],
        "answer": [
            "C"
        ],
        "explanation": "The cluster manager allocates resources to Spark applications and maintains the executor processes\nin client mode.\nCorrect. In cluster mode, the cluster manager is located on a node other than the client machine.\nFrom there it starts and ends executor processes on the cluster nodes as required by the Spark\napplication running on the Spark driver.\nThe cluster manager allocates resources to Spark applications and maintains the executor processes\n\n\nin remote mode.\nWrong, there is no \"remote\" execution mode in Spark. Available execution modes are local, client,\nand cluster.\nThe cluster manager allocates resources to the DataFrame manager\nWrong, there is no \"DataFrame manager\" in Spark.\nThe cluster manager schedules tasks on the cluster in client mode.\nNo, in client mode, the Spark driver schedules tasks on the cluster - not the cluster manager.\nThe cluster manager schedules tasks on the cluster in local mode.\nWrong: In local mode, there is no \"cluster\". The Spark application is running on a single machine, not\non a cluster of machines."
    },
    {
        "question": "Which of the following code blocks writes DataFrame itemsDf to disk at storage location\nfilePath, making sure to substitute any existing data at that location?",
        "options": [
            "A. itemsDf.write.mode(\"overwrite\").parquet(filePath)",
            "B. itemsDf.write.option(\"parquet\").mode(\"overwrite\").path(filePath)",
            "C. itemsDf.write(filePath, mode=\"overwrite\")",
            "D. itemsDf.write.mode(\"overwrite\").path(filePath)",
            "E. itemsDf.write().parquet(filePath, mode=\"overwrite\")"
        ],
        "answer": [
            "A"
        ],
        "explanation": "itemsDf.write.mode(\"overwrite\").parquet(filePath)\nCorrect! itemsDf.write returns a pyspark.sql.DataFrameWriter instance whose overwriting behavior\ncan be modified via the mode setting or by passing mode=\"overwrite\" to the parquet() command.\nAlthough the parquet format is not prescribed for solving this question, parquet() is a valid operator\nto initiate Spark to write the data to disk.\nitemsDf.write.mode(\"overwrite\").path(filePath)\nNo. A pyspark.sql.DataFrameWriter instance does not have a path() method.\nitemsDf.write.option(\"parquet\").mode(\"overwrite\").path(filePath)\nIncorrect, see above. In addition, a file format cannot be passed via the option() method.\nitemsDf.write(filePath, mode=\"overwrite\")\nWrong. Unfortunately, this is too simple. You need to obtain access to a DataFrameWriter for the\nDataFrame through calling itemsDf.write upon which you can apply further methods to control how\nSpark data should be written to disk. You cannot, however, pass arguments to itemsDf.write directly.\nitemsDf.write().parquet(filePath, mode=\"overwrite\")\nFalse. See above.\nMore info: pyspark.sql.DataFrameWriter.parquet - PySpark 3.1.2 documentation Static notebook |\nDynamic notebook: See test 3"
    },
    {
        "question": "The code block shown below should add a column itemNameBetweenSeparators to\nDataFrame itemsDf. The column should contain arrays of maximum 4 strings. The arrays should be\ncomposed of the values in column itemsDf which are separated at - or whitespace characters. Choose\nthe answer that correctly fills the blanks in the code block to accomplish this.\nSample of DataFrame itemsDf:\n1.+------+----------------------------------+-------------------+\n2.|itemId|itemName |supplier |\n3.+------+----------------------------------+-------------------+\n\n\n4.|1 |Thick Coat for Walking in the Snow|Sports Company Inc.|\n5.|2 |Elegant Outdoors Summer Dress |YetiX |\n6.|3 |Outdoors Backpack |Sports Company Inc.|\n7.+------+----------------------------------+-------------------+\nCode block:\nitemsDf.__1__(__2__, __3__(__4__, \"[\\s\\-]\", __5__))",
        "options": [
            "A. 1. withColumn\n2. \"itemNameBetweenSeparators\"\n3. split\n4. \"itemName\"\n5. 4",
            "B. 1. withColumnRenamed\n2. \"itemNameBetweenSeparators\"\n3. split\n4. \"itemName\"\n5. 4",
            "C. 1. withColumnRenamed\n2. \"itemName\"\n3. split\n4. \"itemNameBetweenSeparators\"\n5. 4",
            "D. 1. withColumn\n2. \"itemNameBetweenSeparators\"\n3. split\n4. \"itemName\"\n5. 5",
            "E. 1. withColumn\n2. itemNameBetweenSeparators\n3. str_split\n4. \"itemName\"\n5. 5"
        ],
        "answer": [
            "A"
        ],
        "explanation": "This question deals with the parameters of Spark's split operator for strings.\nTo solve this question, you first need to understand the difference between DataFrame.withColumn()\nand DataFrame.withColumnRenamed(). The correct option here is DataFrame.withColumn() since,\naccording to the question, we want to add a column and not rename an existing column. This leaves\nyou with only 3 answers to consider.\nThe second gap should be filled with the name of the new column to be added to the DataFrame.\nOne of the remaining answers states the column name as itemNameBetweenSeparators, while the\nother two state it as \"itemNameBetweenSeparators\". The correct option here is\n\"itemNameBetweenSeparators\", since the other option would let Python try to interpret\nitemNameBetweenSeparators as the name of a variable, which we have not defined. This leaves you\nwith 2 answers to consider.\nThe decision boils down to how to fill gap 5. Either with 4 or with 5. The question asks for arrays of\n\n\nmaximum four strings. The code in gap 5 relates to the limit parameter of Spark's split operator (see\ndocumentation linked below). The documentation states that \"the resulting array's length will not be\nmore than limit\", meaning that we should pick the answer option with 4 as the code in the fifth gap\nhere.\nOn a side note: One answer option includes a function str_split. This function does not exist in\npySpark.\nMore info: pyspark.sql.functions.split - PySpark 3.1.2 documentation\nStatic notebook | Dynamic notebook: See test 3"
    },
    {
        "question": "Which of the following are valid execution modes?",
        "options": [
            "A. Kubernetes, Local, Client",
            "B. Client, Cluster, Local",
            "C. Server, Standalone, Client",
            "D. Cluster, Server, Local",
            "E. Standalone, Client, Cluster"
        ],
        "answer": [
            "B"
        ],
        "explanation": "This is a tricky question to get right, since it is easy to confuse execution modes and deployment\nmodes. Even in literature, both terms are sometimes used interchangeably.\nThere are only 3 valid execution modes in Spark: Client, cluster, and local execution modes. Execution\nmodes do not refer to specific frameworks, but to where infrastructure is located with respect to\neach other.\nIn client mode, the driver sits on a machine outside the cluster. In cluster mode, the driver sits on a\nmachine inside the cluster. Finally, in local mode, all Spark infrastructure is started in a single JVM\n(Java Virtual Machine) in a single computer which then also includes the driver.\nDeployment modes often refer to ways that Spark can be deployed in cluster mode and how it uses\nspecific frameworks outside Spark. Valid deployment modes are standalone, Apache YARN, Apache\nMesos and Kubernetes.\nClient, Cluster, Local\nCorrect, all of these are the valid execution modes in Spark.\nStandalone, Client, Cluster\nNo, standalone is not a valid execution mode. It is a valid deployment mode, though.\nKubernetes, Local, Client\nNo, Kubernetes is a deployment mode, but not an execution mode.\nCluster, Server, Local\nNo, Server is not an execution mode.\nServer, Standalone, Client\nNo, standalone and server are not execution modes.\nMore info: Apache Spark Internals - Learning Journal"
    },
    {
        "question": "Which of the following code blocks shows the structure of a DataFrame in a tree-like way,\ncontaining both column names and types?",
        "options": [
            "A. 1.print(itemsDf.columns)\n2.print(itemsDf.types)",
            "B. itemsDf.printSchema()",
            "C. spark.schema(itemsDf)",
            "D. itemsDf.rdd.printSchema()",
            "E. itemsDf.print.schema()"
        ],
        "answer": [
            "B"
        ],
        "explanation": "itemsDf.printSchema()\nCorrect! Here is an example of what itemsDf.printSchema() shows, you can see the tree-like structure\ncontaining both column names and types:\nroot\n|-- itemId: integer (nullable = true)\n|-- attributes: array (nullable = true)\n| |-- element: string (containsNull = true)\n|-- supplier: string (nullable = true)\nitemsDf.rdd.printSchema()\nNo, the DataFrame's underlying RDD does not have a printSchema() method.\nspark.schema(itemsDf)\nIncorrect, there is no spark.schema command.\nprint(itemsDf.columns)\nprint(itemsDf.dtypes)\nWrong. While the output of this code blocks contains both column names and column types, the\ninformation is not arranges in a tree-like way.\nitemsDf.print.schema()\nNo, DataFrame does not have a print method.\nStatic notebook | Dynamic notebook: See test 3"
    },
    {
        "question": "Which of the following code blocks returns all unique values of column storeId in DataFrame\ntransactionsDf?",
        "options": [
            "A. transactionsDf[\"storeId\"].distinct()",
            "B. transactionsDf.select(\"storeId\").distinct()",
            "C. transactionsDf.filter(\"storeId\").distinct()",
            "D. transactionsDf.select(col(\"storeId\").distinct())",
            "E. transactionsDf.distinct(\"storeId\")"
        ],
        "answer": [
            "B"
        ],
        "explanation": "distinct() is a method of a DataFrame. Knowing this, or recognizing this from the documentation, is\nthe key to solving this question.\nMore info: pyspark.sql.DataFrame.distinct - PySpark 3.1.2 documentation Static notebook | Dynamic\nnotebook: See test 2"
    }
];