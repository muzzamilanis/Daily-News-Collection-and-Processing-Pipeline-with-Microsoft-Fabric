#!/usr/bin/env python
# coding: utf-8

# ## transform_bing_raw_json
# 
# New notebook

# In[2]:


df = spark.read.option("multiline", "true").json("Files/bing-latest-news.json")
# df now is a Spark DataFrame containing JSON data from "Files/bing-latest-news.json".
display(df)


# In[3]:


# ignore all columns except value which will be used in transformation
df = df.select("value")
display(df)


# In[4]:


from pyspark.sql.functions import explode
df_exploded = df.select(explode(df["value"]).alias("bing_news"))
display(df_exploded)


# In[5]:


news_list = df_exploded.toJSON().collect()


# In[6]:


print(news_list[50])


# In[7]:


import json

title=[]
description=[]
category=[]
url=[]
image=[]
provider=[]
datePublished=[]

for news in news_list:
    try:
        #parse the json string into dictionary
        article = json.loads(news)
        
        #only those news are required which have image and category
        if article["bing_news"].get("category") and article["bing_news"].get("image", {}).get("thumbnail", {}).get("contentUrl", {}):

            title.append(article["bing_news"]["name"])
            description.append(article["bing_news"]["description"])
            category.append(article["bing_news"]["category"])
            url.append(article["bing_news"]["url"])
            image.append(article["bing_news"]["image"]["thumbnail"]["contentUrl"])
            provider.append(article["bing_news"]["provider"][0]["name"])
            datePublished.append(article["bing_news"]["datePublished"])
    
    except Exception as e:
        print(f"Error processing json: {e}")


# In[8]:


from pyspark.sql.types import StructField, StructType, StringType

data = list(zip(title, description, category, url, image, provider, datePublished))

#Define Schema
Schema = StructType([
    StructField("title", StringType(), True),
    StructField("description", StringType(), True),
    StructField("category", StringType(), True),
    StructField("url", StringType(), True),
    StructField("image", StringType(), True),
    StructField("provider", StringType(), True),
    StructField("datePublished", StringType(), True),
])

df_cleaned = spark.createDataFrame(data, schema=Schema)
display(df_cleaned)


# In[9]:


from pyspark.sql.functions import to_date, date_format

#transform datePublished column from timestamp to date string
df_cleaned_final = df_cleaned.withColumn("datePublished", date_format(to_date("datePublished"), "dd-MM-yyyy"))
display(df_cleaned_final)


# In[13]:


from pyspark.sql.utils import AnalysisException

try:

    table_name = 'bing_lake_db.tbl_latestBingNews'
    
    df_cleaned_final.write.format("delta").saveAsTable(table_name)

except AnalysisException:

    print('Table already exist')

    df_cleaned_final.createOrReplaceTempView("vw_df_cleaned_final")

    spark.sql(f"""  MERGE INTO {table_name} target_table
                    USING vw_df_cleaned_final source_table

                    ON source_table.url = target_table.url

                    WHEN MATCHED AND
                    source_table.title <> target_table.title OR
                    source_table.description <> target_table.description OR
                    source_table.category <> target_table.category OR
                    source_table.image <> target_table.image OR
                    source_table.provider <> target_table.provider OR
                    source_table.datePublished <> target_table.datePublished

                    THEN UPDATE SET *

                    WHEN NOT MATCHED THEN INSERT *
                """)

