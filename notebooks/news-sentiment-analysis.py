#!/usr/bin/env python
# coding: utf-8

# ## news-sentiment-analysis
# 
# New notebook

# In[1]:


df = spark.sql("SELECT * FROM bing_lake_db.tbl_latestbingnews")
display(df)


# In[2]:


import synapse.ml.core
from synapse.ml.services import AnalyzeText


# In[5]:


model = (AnalyzeText()
        .setTextCol("description") # column on which analysis need to be performed
        .setKind("SentimentAnalysis") # type of analysis need to be performed 
        .setOutputCol("response")
        .setErrorCol("error"))


# In[6]:


sentiment_df = model.transform(df)
display(sentiment_df)


# In[8]:


from pyspark.sql.functions import col

result = sentiment_df.withColumn("sentiment", col("response.documents.sentiment"))
display(result)


# In[10]:


sentiment_df_final = result.drop("error", "response")
display(sentiment_df_final)


# In[12]:


from pyspark.sql.utils import AnalysisException

try:

    table_name = 'bing_lake_db.tbl_sentimentAnalysisBingNews'
    view_name = 'vw_sentiment_df_final'
    
    sentiment_df_final.write.format("delta").saveAsTable(table_name)

except AnalysisException:

    print('Table already exist')

    sentiment_df_final.createOrReplaceTempView(view_name)

    spark.sql(f"""  MERGE INTO {table_name} target_table
                    USING {view_name} source_table

                    ON source_table.url = target_table.url

                    WHEN MATCHED AND
                    source_table.title <> target_table.title OR
                    source_table.description <> target_table.description OR
                    source_table.category <> target_table.category OR
                    source_table.image <> target_table.image OR
                    source_table.provider <> target_table.provider OR
                    source_table.datePublished <> target_table.datePublished
                    source_table.sentiment <> target_table.sentiment

                    THEN UPDATE SET *

                    WHEN NOT MATCHED THEN INSERT *
                """)

