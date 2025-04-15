import pandas as pd
import os

input_dir = "data"
output_file = "data/top_100k_books.csv"


book_files = [f for f in os.listdir(input_dir) 
              if f.startswith('book') and f.endswith('.csv')]


chunk_size = 100000
top_books = pd.DataFrame()

for file in book_files:
    file_path = os.path.join(input_dir, file)
    print(f"Processing {file}...")
    

    for chunk in pd.read_csv(file_path, chunksize=chunk_size, on_bad_lines='skip'):

        if 'CountsOfReview' not in chunk.columns:
            continue
    
        filtered = chunk[chunk['CountsOfReview'] >= 0]
    
        top_books = pd.concat([top_books, filtered], ignore_index=True)


if not top_books.empty:
    top_books = top_books.sort_values('CountsOfReview', ascending=False).head(100000)
    top_books.to_csv(output_file, index=False)
    print(f"Successfully created {output_file} with {len(top_books)} top books")
    print(f"Review count range: {top_books['CountsOfReview'].min()} to {top_books['CountsOfReview'].max()}")
else:
    print("No books met the criteria")