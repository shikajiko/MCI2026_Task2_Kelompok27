# Tugas 2 MCI 2026 Kelompok 27

| Name | NRP |
| --- | --- | 
| Riyan Fadli Amazzadin | 5025241068 | 
| Nyoman Surya Hutama Andyartha | 5025241093 |

- **ETL (Extract, Transform, Load)** adalah proses pengambilan data dari sumber, pembersihan, lalu pemuatan ke sistem penyimpanan untuk dianalisis.

- **Apache Airflow** adalah platform orkestrasi workflow yang mengatur urutan dan penjadwalan task secara otomatis. Di sini digunakan untuk menjalankan pipeline ETL secara terjadwal harian.

- **PySpark** adalah API Python dari Apache Spark untuk memproses data secara paralel. Di sini digunakan pada tahap Transform untuk membersihkan dan memvalidasi data.

- **ClickHouse** adalah database analitik kolumnar berkecepatan tinggi yang dioptimalkan untuk query agregasi. Di sini berperan sebagai data warehouse tempat data bersih disimpan dan di-query.

- **Metabase** adalah tool Business Intelligence untuk membuat visualisasi dan dashboard dari database. Di sini digunakan untuk menampilkan insight bisnis dari data di ClickHouse.

Dalam tugas ini, semua proses dan tools di atas akan diterapkan pada dataset [Orders](http://96.9.212.102:8000/orders). Langkah-langkah untuk menjalankan keseluruhan proses di bawah mengikuti sumber berikut: [Wikipedia Realtime Pipeline](https://github.com/yogs14/wikipedia-realtime-pipeline/tree/main) (dengan beberapa penyesuaian).

# Pipeline Data (Apache Airflow DAG)

# Task 1: Fetch Orders 
[`src/utils/orders_source.py`](https://github.com/shikajiko/MCI2026_Task2_Kelompok27/blob/main/src/utils/orders_source.py)
- ```python
  response = requests.get(ORDER_API_URL, timeout=10)
  response.raise_for_status()
  return response.json()
  ```
  Mengambil data dari API. `raise_for_status()` memastikan pipeline langsung berhenti dan error jika API mengembalikan response gagal (misal 404, 500), sehingga data korup tidak ikut diproses.

- ```python
  for order in orders:
      for product in order.get("products", []):
          rows.append({
              "order_id": order.get("order_id"),
              ...
              "product_name": product.get("product_name"),
              ...
          })
  ```
  Data API berbentuk nested (1 order berisi banyak produk), nested loop ini mengubahnya menjadi tabular di mana setiap baris mewakili 1 produk dalam 1 order.

- ```python
  output_dir = Path(DATA_LAKE_DIR) / "raw_json"
  output_path = output_dir / f"orders_raw_{timestamp}.json"
  json.dump(payload, f, indent=2, ensure_ascii=False)
  ```
  Menyimpan response API mentah sebagai JSON untuk keperluan audit.

- ```python
  output_dir = Path(DATA_LAKE_DIR) / "raw_parquet"
  output_path = output_dir / f"orders_{timestamp}.parquet"
  df = pd.DataFrame(rows)
  df.to_parquet(output_path, index=False)
  ```
  Menyimpan hasil flatten sebagai Parquet untuk diproses Spark di task berikutnya. 

[`src/dags/fetch_data.py`](https://github.com/shikajiko/MCI2026_Task2_Kelompok27/blob/main/src/dags/fetch_data.py)

- ```python
  payload = fetch_orders_json()
  orders = extract_orders(payload)
  rows = flatten_orders_to_rows(orders)
  raw_path = save_raw_json(payload)
  parquet_path = save_raw_parquet(rows)
  ```
  `fetch_data.py` hanya memanggil fungsi-fungsi dari orders_source.py secara berurutan. Pemisahan ini membuat debugging lebih mudah karena setiap fungsi punya satu tanggung jawab.

## Task 2: Process Orders
[`src/dags/process_orders_spark.py`](https://github.com/shikajiko/MCI2026_Task2_Kelompok27/blob/main/src/dags/process_orders_spark.py)

- ```python
  df_raw = spark.read.parquet(f"file://{RAW_PARQUET_DIR}")
  ```
  Membaca seluruh file Parquet dari `raw_parquet/` sekaligus.

### Exploratory Data Analysis
Tindakan transformasi data yang kami lakukan ditentukan terlebih dahulu dengan tahap EDA. Kami melihat bagaimana bentuk dan karakteristik dari dataset yang dimiliki sebelum mengambil keputusan. Penjelasan yang lebih detail mengenai tahap EDA bisa dilihat melalui: [`src/eda/eda_summary.md`](https://github.com/shikajiko/MCI2026_Task2_Kelompok27/blob/main/src/eda/eda_summary.md). Tahapan serta hasil dokumentasi tahap EDA bisa dilihat melalui [`src/eda`](https://github.com/shikajiko/MCI2026_Task2_Kelompok27/tree/main/src/eda) dan [images](https://github.com/shikajiko/MCI2026_Task2_Kelompok27/tree/main/images).

- ```python
  df_typed = df_raw \
      .withColumn("order_id", F.col("order_id").cast("int")) \
      .withColumn("days_since_prior_order", F.col("days_since_prior_order").cast("float")) \
      ...
  ```
  Memastikan semua kolom memiliki tipe data yang benar sebelum divalidasi.

- ```python
  range_valid = (
      F.col("order_dow").between(0, 6)
      & F.col("order_hour_of_day").between(0, 23)
      & F.col("reordered").isin(0, 1)
      & (F.col("days_since_prior_order").isNull()
          | F.col("days_since_prior_order").between(0, 30))
  )
  ```
  Validasi range berdasarkan temuan EDA.

- ```python
  df_validated = df_typed \
      .withColumn("is_days_since_prior_order_missing",
          F.col("days_since_prior_order").isNull()) \
      .withColumn("department",
          F.when(F.lower(F.trim(F.col("department"))) == "missing", None)
          .otherwise(F.col("department")))
  ```
  Dua keputusan dari EDA: `null` pada `days_since_prior_order` ditandai dengan `flag` (bukan dihapus, karena bermakna order pertama), dan nilai `"missing"` pada `department` dikonversi ke `null` yang sesungguhnya.

- ```python
  df_clean = df_validated.filter(required_valid & range_valid)
  df_rejected = df_validated.filter(~(required_valid & range_valid))
  
  df_clean.write.mode("overwrite").parquet(f"file://{CLEAN_PARQUET_DIR}")
  df_rejected.write.mode("overwrite").parquet(f"file://{REJECTED_PARQUET_DIR}")
  ```
  Data dipisah menjadi dua, yaitu data bersih ke `clean_parquet/` untuk diproses selanjutnya dan ditolak ke `rejected_parquet/`.

## Task 3: Load Orders
[`src/dags/load_orders_clickhouse.py`](https://github.com/shikajiko/MCI2026_Task2_Kelompok27/blob/main/src/dags/load_orders_clickhouse.py)

- ```python
  df_clean = spark.read.parquet(f"file://{CLEAN_PARQUET_DIR}")
  final_results = df_clean.toPandas()
  ```
  Membaca hasil Task 2 (`clean_parquet/`) dan mengkonversinya ke `Pandas DataFrame` untuk disiapkan sebelum dimasukkan ke `ClickHouse`.

- ```python
  client.execute("DROP TABLE IF EXISTS mci_task2.orders_fact")
  client.execute('''
      CREATE TABLE mci_task2.orders_fact (
          order_id            Int32,
          days_since_prior_order Nullable(Float32),
          department          Nullable(String),
          is_days_since_prior_order_missing UInt8,
          ...
      ) ENGINE = MergeTree()
      ORDER BY (order_id, product_id)
  ''')
  ```
  Tabel didrop dan dibuat ulang setiap run untuk memastikan schema selalu baru. Ini adalah skrip DDL utama yang akan menentukan struktur database, misalnya jumlah kolom dan tipe data. 

- ```python
  final_results = final_results.astype(object).where(
      final_results.notnull(), None
  )
  data_tuples = [
      tuple(row)
      for row in final_results[load_columns].itertuples(index=False, name=None)
  ]
  client.execute("INSERT INTO mci_task2.orders_fact VALUES", data_tuples)
  ```
  Nilai null dikonversi eksplisit ke `None` agar `ClickHouse` menerimanya dengan benar, lalu data dimasukkan sekaligus dalam satu operasi `INSERT`.

# ClickHouse

```python

```

```python

```

```python

```

```python

```

```python

```

```python

```

# Orders Insight Dashboard (Metabase)
