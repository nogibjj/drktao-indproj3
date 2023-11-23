# Databricks notebook source
# MAGIC %pip install tabulate
# MAGIC dbutils.library.restartPython()

# COMMAND ----------



# Usage of Spark SQL for data transformations
avg_duration_artist = spark.sql("""
    SELECT
    artist_name,
    avg(duration) AS avg_duration
    FROM
    prepared_data
    WHERE
    year>0
    GROUP BY
    artist_name
    ORDER BY
    avg_duration DESC
    LIMIT 20
""").toPandas()

avg_duration_artist

# COMMAND ----------

# Data Validation Check
row = avg_duration_artist.count()[1]
if row>0:
    print(f"Data validation passed. {row} rows available.")
else:
    print("No data queried")

# COMMAND ----------

# Visualization of the transformed data
plt.figure(figsize=(15, 8))
plt.bar(avg_duration_artist["artist_name"], avg_duration_artist["avg_duration"], color='skyblue')
plt.title("Top 20 Artists by Average Song Duration")
plt.xlabel("Artist")
plt.ylabel("Average Duration")
plt.xticks(rotation=90)
plt.show()


# COMMAND ----------

# Spark SQL Query: Which artists released the most songs each year?
top_artists=spark.sql("""
    SELECT
    artist_name,
    count(artist_name) AS num_songs,
    year
    FROM
    prepared_data
    WHERE
    year > 0
    GROUP BY
    artist_name,
    year
    ORDER BY
    num_songs DESC,
    year DESC
    LIMIT 5;
""").toPandas()

top_artists

# COMMAND ----------

# Spark SQL Query: Find songs for your DJ list
top_tempo=spark.sql(""" 
    SELECT
    artist_name,
    title,
    tempo
    FROM
    prepared_data
    WHERE
    time_signature = 4
    AND
    tempo between 100 and 140
    LIMIT 5;
""").toPandas()

top_tempo