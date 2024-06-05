import pandas as pd
import os
import time
from concurrent.futures import ProcessPoolExecutor


def read_csv(file):
    pid = os.getpid()  # This will give the process ID
    print(f"Process {pid} is reading file: {file}")
    
    start_time = time.time()
    chunks=pd.read_csv(file, chunksize=10000)  # Read CSV file in chunks of 10,000 rows
    # for i, chunks in enumerate(chunks):
    #     print(f"Process {pid}, Chunk {i}:")  #remove comments to print the file
    #     print(chunks)  # Print the chunk DataFrame
    
    end_time = time.time()
    elapsed_time = end_time - start_time
    print(f"Process {pid} finished reading file: {file}. Time taken: {elapsed_time:.2f} seconds")
    
    return None #concatenated_df


if __name__ == "__main__":
    folder_path = '/Users/priyanshupaul/pracs/multithreading/src/chunks/'
    

    start_total_time = time.time()
    files = [os.path.join(folder_path, f) for f in os.listdir(folder_path) if f.endswith('.csv')]
        
    with ProcessPoolExecutor() as executor:  # Use ProcessPoolExecutor instead of ThreadPoolExecutor
        futures = [executor.submit(read_csv, file) for file in files]
        # for future in futures:
        #     concatenated_df = future.result()
            #print(concatenated_df)  # Print the concatenated DataFrame for each file

    end_total_time = time.time()
    total_time = end_total_time - start_total_time
    print(f"Total time taken to read and concatenate all files: {total_time:.2f} seconds")
