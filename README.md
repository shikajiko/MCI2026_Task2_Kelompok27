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

- Dokumentasi: 
  <img width="1251" height="550" alt="Screenshot 2026-05-18 at 20 38 12" src="https://github.com/user-attachments/assets/43ce1e73-c3b9-4041-ac5e-06da38a9ec59" />
  <img width="831" height="232" alt="Screenshot 2026-05-18 at 21 25 11" src="https://github.com/user-attachments/assets/6f4c816a-605e-4900-a14e-702ddf05362e" />
  <img width="1236" height="391" alt="Screenshot 2026-05-18 at 21 25 27" src="https://github.com/user-attachments/assets/2985a315-b8da-42e1-a405-06da04edd65a" />


# ClickHouse

[`src/sql/ddl/create_orders_fact.sql`](https://github.com/shikajiko/MCI2026_Task2_Kelompok27/blob/main/src/sql/ddl/create_orders_fact.sql)
- ```sql
  CREATE TABLE mci_task2.orders_fact (
              order_id            Int32,
              user_id             Int32,
              order_number        Int32,
              order_dow           Int32,
              order_hour_of_day   Int32,
              days_since_prior_order Nullable(Float32),
              eval_set            String,
              product_id          Int32,
              product_name        String,
              aisle_id            Int32,
              aisle               String,
              department_id       Int32,
              department          Nullable(String),
              add_to_cart_order   Int32,
              reordered           Int32,
              is_days_since_prior_order_missing UInt8
          ) ENGINE = MergeTree()
          ORDER BY (order_id, product_id)
  ```
  Skrip DDL untuk membuat schema. 

- ```sql
  SELECT count(*) FROM mci_task2.orders_fact
  ```
  Query untuk verifikasi apakah data sudah masuk ke dalam `ClickHouse`
  
  <img width="423" height="180" alt="Screenshot 2026-05-18 at 21 22 46" src="https://github.com/user-attachments/assets/0d3812a6-759c-4e80-aac1-4a950d89639d" />

# Orders Insight Dashboard (Metabase)

Dashboard Metabase bisa diakses melalui: [Dashboard Orders](https://drive.google.com/file/d/1OqTR5zzNvwLw0zPdpNHQi-v17EczQeE0/view?usp=sharing)

- Average Days Between Orders
  [`src/sql/analytics/average_days.sql`](https://github.com/shikajiko/MCI2026_Task2_Kelompok27/blob/main/src/sql/analytics/average_days.sql)
  - <img width="982" height="346" alt="Screenshot 2026-05-18 at 22 19 02" src="https://github.com/user-attachments/assets/8f4c005f-d8ee-4da0-856b-f09c16831d6b" />

  
  Rata-rata pelanggan kembali belanja setiap ~11 hari.
  Contoh tindakan yang bisa diambil oleh pihak toko dari informasi ini:
  - Mengirim reminder atau promosi otomatis di hari ke-9 atau ke-10 setelah order sebelumnya untuk mendorong reorder lebih cepat. 

- Average Basket Size
  [`src/sql/analytics/average_basket_size.sql`](https://github.com/shikajiko/MCI2026_Task2_Kelompok27/blob/main/src/sql/analytics/average_basket_size.sql)
  - <img width="988" height="346" alt="Screenshot 2026-05-18 at 22 19 09" src="https://github.com/user-attachments/assets/ee6f5536-54c9-40b4-9f51-8b623e86b67c" />

  Rata-rata pelanggan membeli hampir 10 item produk per order.
  Contoh tindakan yang bisa diambil oleh pihak toko dari informasi ini:
  - Menerapkan sistem threshold hadiah/diskon yang sedikit di atas 10 item untuk mendorong penambahan item. 

- Customer Loyalty Segmentation
  [`src/sql/analytics/cust_loyalty_seg.sql`](https://github.com/shikajiko/MCI2026_Task2_Kelompok27/blob/main/src/sql/analytics/cust_loyalty_seg.sql)
  - <img width="930" height="407" alt="Screenshot 2026-05-18 at 22 19 19" src="https://github.com/user-attachments/assets/ac8b083e-1ab2-4694-a327-34d28a95aa77" />

  Mayoritas pelanggan sudah di segmen Regular/Loyal (walau bisa berubah tergantung definisi per segmen).
  Informasi yang didapatkan pihak toko:
  - Mayoritas pelanggan sudah cukup loyal (>70%), di mana bisa diterapkan upaya mempertahankan loyalitas ini dengan reward.
  - Segmen Returning (24,8%) bisa didorong untuk menjadi lebih loyal dengan penawaran yang bersifat personal.
  - Segmen New (4,8%) sangat kecil, di mana bisa diterapkan upaya untuk menambah pelanggan baru. 

- Busiest Day of the Week
  [`src/sql/analytics/peak_day.sql`](https://github.com/shikajiko/MCI2026_Task2_Kelompok27/blob/main/src/sql/analytics/peak_day.sql)
  - <img width="464" height="285" alt="Screenshot 2026-05-18 at 22 19 26" src="https://github.com/user-attachments/assets/789dc538-86d6-47f4-b89f-f9e1950bd8da" />

  Hari 1 (asumsi hari Senin) adalah yang paling ramai.
  Contoh tindakan yang bisa diambil oleh pihak toko dari informasi ini:
  - Memastikan stok penuh dan sumber daya manusia tersedia pada hari tersebut. 

- Busiest Time of the Day
  [`src/sql/analytics/peak_hour.sql`](https://github.com/shikajiko/MCI2026_Task2_Kelompok27/blob/main/src/sql/analytics/peak_hour.sql)
  - <img width="464" height="283" alt="Screenshot 2026-05-18 at 22 19 32" src="https://github.com/user-attachments/assets/f52ebc45-2d97-4548-a824-7c2c793955cc" />

  Puncak terjadi di jam 14 - 15 (siang menjelang sore).
  Contoh tindakan yang bisa diambil oleh pihak toko dari informasi ini:
  - Memberikan notifikasi, iklan, atau email secara digital menjelang jam sibuk. 
  
- Top Department
  [`src/sql/analytics/top_department.sql`](https://github.com/shikajiko/MCI2026_Task2_Kelompok27/blob/main/src/sql/analytics/top_department.sql)
  - <img width="464" height="403" alt="Screenshot 2026-05-18 at 22 19 38" src="https://github.com/user-attachments/assets/b5fac0d3-0cf5-45a6-8b12-f2c77d04e27e" />

  Produk segar seperti sayur dan buah adalah kebutuhan utama.
  Contoh tindakan yang bisa diambil oleh pihak toko dari informasi ini:
  - Menjaga ketersediaan dan kualitas stok pada departemen yang banyak diminta. 
  
- Aisle Performance
  [`src/sql/analytics/aisle_performance.sql`](https://github.com/shikajiko/MCI2026_Task2_Kelompok27/blob/main/src/sql/analytics/aisle_performance.sql)
  - <img width="465" height="404" alt="Screenshot 2026-05-18 at 22 19 45" src="https://github.com/user-attachments/assets/516a2f67-54cc-4660-b6a9-aa74a4c7987d" />

  Packaged vegetables fruits and yogurt menjadi lorong paling ramai.
  Contoh tindakan yang bisa diambil oleh pihak toko dari informasi ini:
  - Menempatkan produk baru atau produk dengan margin tinggi di dekat lorong ini untuk meningkatkan paparan pelanggan terhadap produknya.
  
- Top Product
  [`src/sql/analytics/top_product.sql`](https://github.com/shikajiko/MCI2026_Task2_Kelompok27/blob/main/src/sql/analytics/top_product.sql)
  - <img width="465" height="409" alt="Screenshot 2026-05-18 at 22 19 53" src="https://github.com/user-attachments/assets/2b583da5-2592-43ed-906e-44bf03b40064" />

  Pisang organik menjadi salah satu produk paling diminati.
  Contoh tindakan yang bisa diambil oleh pihak toko dari informasi ini:
  - Memastikan bahwa produk yang populer tidak kehabisan stok. 

- Reorder Rate
  [`src/sql/analytics/reorder_rate.sql`](https://github.com/shikajiko/MCI2026_Task2_Kelompok27/blob/main/src/sql/analytics/reorder_rate.sql)
  - <img width="465" height="406" alt="Screenshot 2026-05-18 at 22 20 14" src="https://github.com/user-attachments/assets/c2bc5882-adda-44e0-8c44-7b63180b68a1" />

  Produk seperti susu dan ground turkey memiliki banyak pembeli dengan loyalitas tinggi.
  Contoh tindakan yang bisa diambil oleh pihak toko dari informasi ini:
  - Produk ini bisa menerapkan program langganan/subscription.
  
- Market Basket Analysis
  [`src/sql/analytics/basket_analysis.sql`](https://github.com/shikajiko/MCI2026_Task2_Kelompok27/blob/main/src/sql/analytics/basket_analysis.sql)
  - <img width="465" height="410" alt="Screenshot 2026-05-18 at 22 20 00" src="https://github.com/user-attachments/assets/4aa71684-3f2d-4912-927b-1870f902e698" />

  Beberapa produk sering dibeli bersamaan, misalnya pisang organik dengan susu organik.
  Contoh tindakan yang bisa diambil oleh pihak toko dari informasi ini:
  - Menerapkan sistem bundling untuk produk-produk yang sering dibeli bersamaan. 
  
- Order Size Trend by User Segmentation
  [`src/sql/analytics/order_trend_segmentation.sql`](https://github.com/shikajiko/MCI2026_Task2_Kelompok27/blob/main/src/sql/analytics/order_trend_segmentation.sql)
  - <img width="467" height="410" alt="Screenshot 2026-05-18 at 22 20 08" src="https://github.com/user-attachments/assets/bb1bdf89-6102-4003-910d-45a03e34ab64" />

  Pelanggan New membeli lebih sedikit item dari Regular dan Loyal.
  Contoh tindakan yang bisa diambil oleh pihak toko dari informasi ini:
  - Memberikan diskon produk dengan threshold jumlah item pada beberapa pembelian awal untuk mendorong pembeli baru membeli lebih banyak.
 
# Kesimpulan 
Tugas ini berhasil membangun pipeline ETL, mulai dari pengambilan data API, transformasi data, memasukkan data ke ClickHouse, hingga visualisasi dashboard untuk mengambil keputusan. Data mentah yang belum terstruktur dapat diolah menjadi insight bisnis yang bermakna. 

### ~Terima Kasih
`docker-compose down`
