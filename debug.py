import pandas as pd

# Debug: lihat kolom ke-9
df = pd.read_csv('data/csv/star_schema/trip_fact.csv', low_memory=False)
print("Nama kolom ke-9:", df.columns[9])  # Nama kolom ke-9
print("Tipe data kolom ke-9:", df.iloc[:, 9].dtype)  # Tipe data kolom ke-9
print("Sample nilai kolom ke-9:", df.iloc[:, 9].unique()[:10])  # Sample nilai kolom ke-9
print("Jumlah kolom total:", len(df.columns))
print("Semua nama kolom:", list(df.columns))