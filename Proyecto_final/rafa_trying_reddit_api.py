import tensorflow_datasets as tfds
import pandas as pd

# Load the dataset with streaming enabled
ds = tfds.load('reddit', split='test[:1%]')

# Process examples in batches and save periodically to CSV
batch_size = 1000  # Number of examples to process at a time
batch_count = 0
df_list = []

for example in tfds.as_numpy(ds):
    df_list.append(pd.DataFrame([example]))
    if len(df_list) >= batch_size:
        df_batch = pd.concat(df_list, ignore_index=True)
        df_batch.to_csv(f'reddit_dataset_part_{batch_count}.csv', index=False)
        batch_count += 1
        df_list = []  # Reset list for the next batch

# Save any remaining examples
if df_list:
    df_batch = pd.concat(df_list, ignore_index=True)
    df_batch.to_csv(f'reddit_dataset_part_{batch_count}.csv', index=False)
