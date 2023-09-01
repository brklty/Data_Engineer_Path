# Potential Customer Return Calculation with Rule-Based Classification


# - TASK 1:

# -- 1 : Read the miuul_gezinomi.xlsx file and show general information about the data set.
import pandas as pd
import numpy as np
pd.set_option('display.max_columns', None)
gezinomi = pd.read_excel("datasets/miuul_gezinomi.xlsx")
df = gezinomi.copy()
df.head()
df.tail()
df.shape
df.info()
df.columns
df.index
df.describe().T
df.isnull().values.any()
df.isnull().sum()

# -- 2: How many unique cities are there? What is their frequency?
df["SaleCityName"].value_counts()

# -- 3: How many unique Concepts are there?
df["ConceptName"].nunique()

# -- 4: How many sales were realised from which Concept?
df["ConceptName"].value_counts()

# -- 5: How much was earned from sales by city?
df.groupby("SaleCityName")['Price'].sum()

# -- 6: How much was earned according to Concept types?
df.groupby("ConceptName")['Price'].sum()

# -- 7: What are the PRICE averages according to cities?
df.groupby("SaleCityName")['Price'].mean()

# -- 8: What are the PRICE averages according to concepts?
df.groupby("ConceptName")['Price'].mean()

# -- 9: What are the PRICE averages by City-Concept?
df.pivot_table('Price','SaleCityName','ConceptName')



# - TASK 2: Change the variable SaleCheckInDayDiff to a categorical variable.
df["SaleCheckInDayDiff"].dtypes
bins = [-1, 7, 30, 90, df['SaleCheckInDayDiff'].max()]
names = ["Last Minuters", "Potential Planners", "Planners", "Early Bookers"]

df["EB_Score"] = pd.cut(df["SaleCheckInDayDiff"], bins, labels=names)

df["EB_Score"].head()
df["EB_Score"].value_counts()
df["EB_Score"].isnull().any()


# - TASK 3: What are the average earnings by COUNTRY, SOURCE, SEX, AGE?
df.groupby(by=["SaleCityName", 'ConceptName', "EB_Score"]).agg({"Price": ["mean", "count"]})

df.groupby(by=["SaleCityName", 'ConceptName', "Seasons"]).agg({"Price": ["mean", "count"]})

df.groupby(by=["SaleCityName", 'ConceptName', "CInDay"]).agg({"Price": ["mean", "count"]})


# - TASK 4: Sort the output of the City-Concept-Season breakdown by PRICE
agg_df = df.groupby(["SaleCityName", "ConceptName", "Seasons"]).agg({"Price": "mean"}).sort_values("Price", ascending=False)
agg_df.head()


# - TASK 5: Convert the names in the index to variable names.
agg_df.reset_index(inplace=True)
agg_df.head()


# - TASK 6: Define new level-based customers (persona)
agg_df["sales_level_based"] = agg_df["SaleCityName"] + "_" + agg_df["ConceptName"] + "_" + agg_df["Seasons"]
agg_df["sales_level_based"] = agg_df["sales_level_based"].str.upper()

#agg_df['sales_level_based'] = agg_df[["SaleCityName", "ConceptName", "Seasons"]].agg(lambda x: '_'.join(x).upper(), axis=1)

agg_df["sales_level_based"].head()


# - TASK 7: Segment new customers (personas).
agg_df["segment"] = pd.qcut(agg_df["Price"], 4, labels=["D", "C", "B", "A"])
agg_df["segment"].value_counts()


agg_df.groupby('segment').agg({'Price': ["mean", "max", "sum"]})



# - TASK 8: Categorise new customers and estimate how much revenue they can generate

# ---- How much income is a person who wants to have an all-inclusive and high season
# holiday in Antalya expected to earn on average?

new_user_antalya = "ANTALYA_HERŞEY DAHIL_HIGH"
agg_df[agg_df["sales_level_based"] == new_user_antalya]
agg_df[agg_df["sales_level_based"] == new_user_antalya]["Price"].mean()

new_user_girne = "GIRNE_YARIM PANSIYON_LOW"
agg_df[agg_df["sales_level_based"] == new_user_girne]["Price"].mean()
