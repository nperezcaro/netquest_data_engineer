1. How would you choose to bundle (or not) the code for deployment to the Cloud (any Cloud Provider)?

    I would gather requirements information about the application and based on those requirements I would make the call to bundle or not.

    I would focus on understanding:

    - The execution context: does the workload need to scale dynamically or will it run in a predictable pattern? If it needs to scale dynamically, bundling is a good choice: if you need more capacity, you can just spin up the bundle.

    - Application complexity: the greater the complexity, the harder it is to deploy an application directly.

    - Deployment paradigm: are we deploying to a serverless service, a container, or a VM? If its serverless, then directly deploying the script might be an option. If its a container, then bundling is the way to go.

2. Instead of having around 5 rows per day, let’s imagine that the process has 20 million. What would you have done different designing the transformation job?

    1. Partitioning the data would be critical. Allows smaller chunks of data to be processed in parallel, which can significantly speed up the transformation process.

    2. Using multi-processing can help to utilize multiple CPU cores to process different partitions simultaneously. This parallelism can drastically reduce the overall processing time.

    3. Implementing lazy evaluation, as it ensures that data transformations are executed only when necessary, which can save memory and processing power. It also allows the data processing framework to optimize operations.

    4. Make extensive use of a distributed computing framework such as Spark. It is designed to handle large amounts of data and can distribute the workload across multiple nodes.

3. The output data is currently stored in S3. The Data Analyst in the Finance team wants to have access to the data (they only know SQL). How would you provide the access? What if the data was 20 million rows per day?

    I would go with AWS Athena. It allows the finance team to use SQL and uses Presto under the hood for query execution. My other option was just Presto, but using Athena requires less configuration (no need to set up the connection to read data from S3); and allows us to take advantage of Presto's distributed architecture.

4. The old SoftwareA is still running in Production for some teams, so the mappings must run daily. How would you monitor the quality of the job results? How would you monitor changes to the SoftwareB CustomFiled values? (Changes to the CustomField would make our mapping excel file obsolete)

    - Monitor the quality of the job results.

        - Use a scheduler, such as Airflow.
            - Airflow is designed to track the status of job execution.
            - Logs are centralized, making it easy to trace issues.
            - Log key metrics (rows proccessed, errors encountered) for further analysis.
        - Use a job logging table in a Database.
            - Provides a persistent (and queryable) history of job executions.
            - Allows to log information such as the parameters used, the status of the job and even error messages.

    - Monitor changes to the SoftwareB CustomFiled values.

        - I would go with tracking current CustomFiled values in a SQL table. This gives me with a centralized and persistent view of the current values; that can be compared to the mapping file to detect discrepancies.

        - Use the SQL table along with a validation job, before the main mapping job, and Airflow to ensure:
            - Mapping file remains valid.
            - Prevent cascading failures caused by outdated mappings (we can stop the DAG if discrepancies are detected and send an alert via email, for example).

5. We have a massive PostgreSQL cluster with millions of rows in some tables Suddenly, we detect one slow query as the bottleneck of a pipeline. Please describe the actions you would do to diagnose and optimize the query.

    - Use EXPLAIN (and EXPLAIN ANALYZE) statements to understand exactly what is causing the slowness.

    - Check indexes: make sure that columns heavily used in WHERE, JOIN and ORDER BY clauses are properly indexed.

    - Partition Large Tables: if very large tables are involved in the query, is worth considering partitioning them. This can reduce the amount of data scaned and improve query performance.

    - Use VACUUM ANALYZE on the tables involved. This will reclaim space occupied by dead tuples and update the statistics used by the query planner, allowing it to make informed decisions when planning the query.

6. While working with files in a data lake, which are the main good practices that you would propose to make it robust and cost effective?

    1. Data Organization and Partitioning

        - Organize data hierarchically. Helps to manage and query the files.
        - Partition data by fields that are frequently queried (dimensions). Helps to scan less data when running queries.

    2. Efficient Storage Formats

        Use a file format such as Parquet (over CSV or JSON for example), because its optimized for read-heavy operations and can significantly reduce storage costs and improve query performance. Also, applies compression to reduce the storage footprint (and supports schema evolution).

    3. Lifecycle Management

        - Use data retention policies to automatically delete or archive old data that is no longer needed.
        - Used tiered storage to move infrequently accessed data to cheaper storage tiers.

    4. Metadata Management

        - Helps to track the schema, location and ownership of data.

    5. Security:

        - Encrypt data at rest and in transit to protect sensitive information.
        - Implement granular access control using IAM policies, and bucket policies to ensure that only authorized users can access the data (by granting permissions at the file, folder, or table level).

7. In terms of working with other data-teams, like Data Science, which would be your approach to making the collaboration successful regarding the interaction with them?

    My approach would be to go for a cross-functional team. Including members from data engineering, data science and QA team, we'll be in the right path for a collaborative and knowledge sharing environment.

    - Define clear goals and objectives to make sure we're all moving in the same direction. This, along with strong feedback loops, can create a productive and successful partnership.

    - Develop and build tests together. Data Science input is crucial for testing and code development to build solutions that meet their (DS) needs.
