import pandas as pd
import random
import string
import sys

# get the path folder of this file
path = sys.path[0]

# read csv in pandas dataframe
df = pd.read_csv(path + '/Reviews.csv')

random.seed(42)
df_copy = df.copy()

steps = [2, 5]

# funzione che genera prende in input un testo e ne genera uno random composto da un suo sottoinsieme di caratteri
def generate_text(txt):
    generated_text = txt[:random.randint(0,len(txt))]
    # se il testo generato non è alfa numerico rigeneralo
    if (not generated_text.isalnum()):
          generated_text = "I like it"
    return generated_text

for i in range(1, 5):
    df_copy['ProductId'] = df_copy['ProductId'].apply(lambda x : ''.join(random.choice(string.ascii_uppercase  + string.digits) for _ in range(10)))
    df_copy['UserId'] = df_copy['UserId'].apply(lambda x : ''.join(random.choice(string.ascii_uppercase  + string.digits) for _ in range(14)))
    df_copy['HelpfulnessDenominator'] = df_copy['HelpfulnessDenominator'].transform(lambda x : random.randint(0, 100))
    df_copy['HelpfulnessNumerator'] = [random.randint(0, row['HelpfulnessDenominator']) for index, row in df_copy.iterrows()]
    df_copy['Score'] = df_copy['Score'].transform(lambda x : random.randint(1, 5))
    df_copy['Text'] = df_copy['Text'].transform(generate_text)

    # aggiorno il dataframe concatenando lo stesso dataset appena modificato
    df = pd.concat([df, df_copy], ignore_index=True)

    if i+1 in steps:
        df.to_csv(path + '/Reviews_' + str(i+1) + '.csv', index=False)