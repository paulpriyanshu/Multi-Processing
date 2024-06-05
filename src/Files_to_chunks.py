import pandas as pd

chunk_size=100000
batch_no=1
filename='annual-enterprise-survey-2021-financial-year-provisional-csv_part1.csv'
print("breaking data into chunks please wait...")
for chunk in pd.read_csv(f'/Users/priyanshupaul/pracs/multithreading/src/{filename}',chunksize=chunk_size):
    
    chunk.to_csv('/Users/priyanshupaul/pracs/multithreading/src/chunks/chunk ' +str(batch_no)+ '.csv' ,index=False)
    batch_no+=1


print("completed now run multiprocessing.py")