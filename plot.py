import pandas as pd
from matplotlib import pyplot as plt

df = pd.read_csv('./hashtag_joebiden.csv')
state_count = df['state_code'].value_counts().head(8)
x = state_count.plot(kind='bar')

plt.title("Número de tweets por estado")
plt.xlabel("Estado")
plt.ylabel("Contagem")

fig = x.get_figure()
fig.savefig('./plot.pdf')