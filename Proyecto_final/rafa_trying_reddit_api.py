import tensorflow_datasets as tfds
import pandas as pd

# Load the 'reddit' dataset
ds = tfds.load('reddit', split='test')

# Convert the TensorFlow dataset to a pandas DataFrame
df = pd.DataFrame(list(tfds.as_dataframe(ds)))

# Save the DataFrame to a CSV file
df.to_csv('reddit_dataset.csv', index=False)
