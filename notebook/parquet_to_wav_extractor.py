import pyarrow.parquet as pq
import soundfile as sf
import io
import os

# 1. Paths to your parquet files
parquet_files = [
    #paste your parquet file paths here

    # r"C:\Users\Garima\Downloads\test-00000-of-00003.parquet",
    # r"C:\Users\Garima\Downloads\test-00001-of-00003.parquet",
]


# 2. Output directory for WAV files

out_dir =  "paste your output directory here"
# out_dir= r"C:\projects\English-Improvement-Coach\notebook\data"
os.makedirs(out_dir, exist_ok=True)

index = 0
# 3. Process files manually using PyArrow ONLY

for file in parquet_files:
    print(f"\nReading {file} ...")
    table = pq.read_table(file)

    audio_col = table["audio_filepath"].to_pylist()
    text_col = table["text"].to_pylist()

    for audio_dict, text in zip(audio_col, text_col):

        audio_bytes = audio_dict["bytes"]

        # Decode bytes using soundfile
        data, sr = sf.read(io.BytesIO(audio_bytes))

        wav_path = os.path.join(out_dir, f"svarah_{index}.wav")
        sf.write(wav_path, data, sr)

        print(f"[{index}] Saved: {wav_path}")

        index += 1

print("\n✔ ALL DONE — WAV files extracted successfully!")
