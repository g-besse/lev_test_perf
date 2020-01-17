#%%
import random
import string
import pandas as pd
import progressbar
from fuzzywuzzy import process
from multiprocessing.dummy import Pool as ThreadPool

max_thread = 4

#%%
df = pd.DataFrame(index=range(15000),columns=['to_match'])
df_opt = pd.DataFrame(index=range(28000),columns=['option'])

for i, row in progressbar.progressbar(df.iterrows(), max_value=len(df)):
    length = random.randint(5, 10)
    df.at[i, 'to_match'] = ''.join(random.choices(string.ascii_uppercase, k=length))

for i, row in progressbar.progressbar(df_opt.iterrows(), max_value=len(df_opt)):
    length = random.randint(5, 10)
    df_opt.at[i, 'option'] = ''.join(random.choices(string.ascii_uppercase, k=length))

df_opt = df_opt['option']

#%%
df['score'] = 'None'
df['match'] = 'None'
l_tpl_lev = []
res_map = []

def levDistance(data):
    i, destination = data
    levenshtein = process.extractOne
    match = levenshtein(destination, df_opt)
    res_map.append((i, match))
    bar.update(bar.value + 1)

for i, row in df.iterrows():
    l_tpl_lev.append((i, row['to_match']))

with progressbar.ProgressBar(max_value=len(l_tpl_lev)) as bar:
    try:
        pool = ThreadPool(max_thread)
        pool.map(levDistance, l_tpl_lev)
        # print(f'Map Reduce successful for {max_thread} threads.')
        pool.close()
        pool.join()
    except RuntimeError as r:
        print(f'{r}')

for data in res_map:
    index, match = data
    df.at[index, 'score'] = match[0]
    df.at[index, 'match'] = match[1]

#%%
df.head()

#%%