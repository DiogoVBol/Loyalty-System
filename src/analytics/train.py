# %%

import pandas as pd
import sqlalchemy

con = sqlalchemy.create_engine("sqlite:///../../data/loyalty-system/database.db")

# %%

df = pd.read_sql("abt_fiel",con)

# %%

df_oot = df[df['dtRef'] == df['dtRef'].max()]

# %%

target = 'flFiel'
features = df.columns.to_list()[3:]

df_train_test = df[df['dtRef'] < df['dtRef'].max()]

y = df_train_test[target]
X = df_train_test[features]

from sklearn import model_selection

X_train, X_test, y_train, y_test = model_selection.train_test_split(
    X, y,
    random_state= 42,
    test_size= 0.2,
    stratify=y
)

