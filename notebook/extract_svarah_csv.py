import pyarrow.parquet as pq
import pandas as pd

# Parquet files
parquet_files = [
    #paste your parquet file paths here

  #  r"C:\Users\Garima\Downloads\test-00000-of-00003.parquet",
   
]

max_rows = 100
collected_rows = []
global_index = 0

for file in parquet_files:
    print(f"Reading: {file}")
    table = pq.read_table(file)

    # Convert full table to pandas
    df = table.to_pandas()

    # REMOVE long path column
    if "audio_filepath" in df.columns:
        # parquet stores audio as struct -> expand struct first
        audio_df = pd.DataFrame(df["audio_filepath"].tolist())
        df = df.drop(columns=["audio_filepath"])
        df["audio_bytes"] = audio_df["bytes"]  # keep bytes if needed
        # df = df.drop(columns=["audio_bytes"])  # uncomment to drop bytes too

    # Add global index
    df.insert(0, "global_index", range(global_index, global_index + len(df)))
    global_index += len(df)

    # Append needed rows only
    needed = max_rows - len(collected_rows)
    collected_rows.extend(df.head(needed).to_dict(orient="records"))

    if len(collected_rows) >= max_rows:
        break

# Final DF
final_df = pd.DataFrame(collected_rows)

# Save CSV inside notebook folder
output_path = (
    # paste your output csv path here
   
   # r"C:\projects\English-Improvement-Coach-for-Indian-Accented-Tones-main\notebook\svarah_first_100_no_path.csv"
)
final_df.to_csv(output_path, index=False, encoding="utf-8")

print("\n🎉 CSV CREATED WITHOUT LONG PATH COLUMN!")
print("Saved at:", output_path)
