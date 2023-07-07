# Task 1: Using the List Comprehension structure, capitalize the names of the
# numeric variables in the car_crashes data and add NUM to the beginning.
import pandas as pd
import seaborn as sns

df = sns.load_dataset("car_crashes")
df.columns

["NUM_"+col.upper() if df[col].dtype != "O" else col.upper() for col in df.columns]

# Task 2: Using the List Comprehension structure, write "FLAG" after the names of the
# variables that do not contain "number" in the car_crashes data.


[col.upper() if "no" in col else col.upper()+"_FLAG" for col in df.columns]


# Task 3: Using the List Comprehension structure, select the names of the variables that are
# DIFFERENT from the variable names given below and create a new dataframe.

og_list = ["abbrev", "no_previous"]

new_cols = [col for col in df.columns if col not in og_list]

new_df = df[new_cols]
new_df.head()


# Task 4 Pandas:
# Task 4.1: Identify the Titanic dataset from the Seaborn library.
df = sns.load_dataset('titanic')
df.head()

# Task 4.2: Find the number of male and female passengers in the Titanic dataset.
df["sex"].value_counts()

# Task 4.3: Find the number of unique values for each column.
df.nunique()

# Task 4.4: Find the number of unique values of the variable pclass.
df['pclass'].nunique()

# Task 4.5: Find the number of unique values of pclass and parch variables.
df[["pclass", "parch"]].nunique()

# Task 4.6: Check the type of the embarked variable. Change its type to category and check again.
df["embarked"].dtype
df["embarked"] = df["embarked"].astype("category")
df["embarked"].dtype
df.info()



# Task 4.7: Show all the sages of those with embarked value C.
df[df["embarked"] == "C"]


# Task 4.8: Show all the sages of those whose embarked value is not S.
df[df["embarked"] == "S"]

# Task 4.9: Show all information for female passengers younger than 30 years old.
df[(df["sex"] == "female") & (df["age"] < 30)]

# Task 4.10: Show the information of passengers whose Fare is over 500 or 70 years old.
df[(df["fare"] > 500) | (df["age"] > 70)]

# Task 4.11: Find the sum of the null values in each variable.
df.isnull().sum()

# Task 4.12: Extract the variable who from the dataframe.
df.drop("who", axis=1, inplace=True)
#  df = df.iloc[:, ~df.columns.str.contains("who")]
df.columns

# Task 4.13: Fill the empty values in the deck variable with the most repeated value (mode) of the deck variable.
df["deck"].fillna(value=df["deck"].mode()[0], inplace=True)
df["deck"].value_counts()


# Task 4.14: Fill in the blank values in the age variable with the median of the age variable.
df["age"].fillna(value=df["age"].median(), inplace=True)
df["age"].value_counts()

# Task 4.15: Find the sum, count, mean values of the pclass and sex variables of the survived variable.

df.groupby(["pclass","sex"]).agg({"survived": ["sum","count","mean"]})


# Task 4.16: Write a function that returns 1 for those under 30 and 0 for those above or equal to 30.
# Using the function you wrote, create a variable named age_flag in the titanic data set. (use apply and lambda constructs)
# Using the function you wrote, create a variable named age_flag in the titanic data set. (use apply and lambda constructs)
df["new_age"] = df["age"].apply(lambda x: 1 if x >= 30 else 0)
df["new_age"].head()


# Task 4.17: Define the Tips dataset from the Seaborn library.
df_tips = sns.load_dataset("tips")
df_tips.head()


# Task 4.18: Find the sum, min, max and average of the total_bill values according to the categories (Dinner, Lunch) of the Time variable.
df_tips.groupby("time").agg({"total_bill": ["sum", "min", "max", "mean"]})


# Task 4.19: Find the sum, min, max and average of total_bill values by days and time.
df_tips.groupby(["time", "day"]).agg({"total_bill": ["sum", "min", "max"]})

# Task 20: Find the sum, min, max and average of the total_bill and type values of the lunch time and female customers according to the day.

df_tips[(df_tips["time"] == "Lunch") & (df_tips["sex"] == "Female")].groupby("day").agg({"total_bill": ["sum","min","max","mean"],
                                                                           "tip":  ["sum","min","max","mean"]})

# Task 21: What is the average of orders with size less than 3 and total_bill greater than 10? (use loc)
df_tips.loc[(df_tips["size"] < 3) & (df_tips["total_bill"] >10), "total_bill"].mean()

# Task 22: Create a new variable called total_bill_tip_sum. Let him give the sum of the total bill and tip paid by each customer.
df_tips["total_bill_tip_sum"] = df_tips["total_bill"] + df_tips["tip"]
df.head()

# Task 23: Sort according to total_bill_tip_sum variable from largest to smallest and assign the first 30 people to a new dataframe
new_df = df_tips.sort_values("total_bill_tip_sum", ascending=False)[:30]
new_df.shape