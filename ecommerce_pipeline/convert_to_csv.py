import pandas as pd

df = pd.read_excel("data/Online Retail.xlsx")

df.to_csv("data/online_retail.csv", index=False)

print("conversion complete!")