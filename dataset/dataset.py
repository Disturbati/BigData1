import pandas as pd
import random
import string
import sys

# get the path folder of this file
path = sys.path[0]

# read csv in pandas dataframe
df = pd.read_csv(path + '/Reviews.csv')

random.seed(42)

for i in range(2, 11):
    df['ProductId'] = df['ProductId'].apply(lambda x : ''.join(random.choice(string.ascii_uppercase  + string.digits) for _ in range(10)))
    df['UserId'] = df['UserId'].apply(lambda x : ''.join(random.choice(string.ascii_uppercase  + string.digits) for _ in range(14)))
    df['HelpfulnessDenominator'] = df['HelpfulnessDenominator'].transform(lambda x : random.randint(0, 100))
    df['HelpfulnessNumerator'] = [random.randint(0, row['HelpfulnessDenominator']) for index, row in df.iterrows()]
    df['Score'] = df['Score'].transform(lambda x : random.randint(1, 5))
    df['Text'] = df['Text'].transform(lambda x : x[:random.randint(0, len(x))])

    df.to_csv(path + '/Reviews' + str(i) + '.csv', index=False)