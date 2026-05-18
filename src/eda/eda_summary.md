# Ringkasan EDA Orders Pipeline

## Setup

EDA ini tidak dijalankan sebagai task utama pada DAG Airflow. EDA dijalankan terpisah untuk memahami karakteristik batch data, lalu hasil analisisnya dipakai untuk menentukan langkah cleaning pada pipeline.

Command untuk menjalankan EDA:

```bash
docker compose run --rm airflow-scheduler bash -lc "python /opt/airflow/src/eda/eda_run.py"
```

## Directory Hasil

Hasil data dan EDA disimpan pada directory berikut:

```text
data_lake/orders/raw_json/
data_lake/orders/raw_parquet/
data_lake/orders/clean_parquet/
data_lake/orders/rejected_parquet/
data_lake/orders/eda/reports/
data_lake/orders/eda/charts/
images/
```

Folder `images/` berisi salinan chart dari `data_lake/orders/eda/charts/` agar gambar dapat ditampilkan langsung pada file markdown ini.

## Penjelasan

Pipeline ini mengambil data orders dari API, mengubah struktur nested JSON menjadi baris tabular, menyimpan data mentah ke data lake, membersihkan data menggunakan Spark, lalu memuat hasil akhirnya ke ClickHouse.

Data yang digunakan pada EDA merupakan data yang sudah di-flatten. Artinya, satu baris merepresentasikan satu produk di dalam satu order, bukan satu order unik. Karena itu, jumlah pada chart dibaca sebagai jumlah baris produk/order-product rows.

## Alur Program

`fetch_data.py` menjalankan function dari `orders_source.py`, yaitu:

- `fetch_orders_json()` untuk mengambil payload orders dari API.
- `extract_orders()` untuk mengambil isi `orders` dari payload API.
- `flatten_orders_to_rows()` untuk mengubah order yang berisi banyak produk menjadi baris-baris produk.
- `save_raw_json()` untuk menyimpan payload asli ke `data_lake/orders/raw_json/`.
- `save_raw_parquet()` untuk menyimpan hasil flatten ke `data_lake/orders/raw_parquet/`.

Setelah data raw tersedia, `eda_run.py` menjalankan keseluruhan proses EDA. File ini menulis report menggunakan function pada `eda_profile.py`, yaitu schema report, null report, distinct report, numeric summary, numeric percentile, dan top category report. Setelah itu, file ini membuat grafik menggunakan function pada `eda_charts.py`, yaitu null-rate chart, histogram/boxplot untuk kolom numerik yang relevan, top category chart, dan pola order berdasarkan waktu.

Output EDA disimpan ke:

```text
data_lake/orders/eda/reports/
data_lake/orders/eda/charts/
```

Setelah hasil EDA dianalisis, `process_orders_spark.py` digunakan pada pipeline utama untuk membaca `raw_parquet`, melakukan casting tipe data, validasi nilai, pemisahan data valid dan invalid, lalu menyimpan hasilnya ke:

```text
data_lake/orders/clean_parquet/
data_lake/orders/rejected_parquet/
```

Terakhir, `load_orders_clickhouse.py` membaca data dari `clean_parquet` dan memuat data bersih tersebut ke table `mci_task2.orders_fact` pada ClickHouse.

## Analisis

Dari hasil `fetch_data.py`, pada batch ini kami mendapatkan data dengan bentuk:

```text
Jumlah baris   : 968
Jumlah kolom   : 15
Unit per baris : satu produk dalam satu order
```

Schema batch ini adalah:

| Column | Spark Type | Nullable |
| --- | --- | --- |
| `order_id` | `LongType()` | Yes |
| `user_id` | `LongType()` | Yes |
| `order_number` | `LongType()` | Yes |
| `order_dow` | `LongType()` | Yes |
| `order_hour_of_day` | `LongType()` | Yes |
| `days_since_prior_order` | `DoubleType()` | Yes |
| `eval_set` | `StringType()` | Yes |
| `product_id` | `LongType()` | Yes |
| `product_name` | `StringType()` | Yes |
| `aisle_id` | `LongType()` | Yes |
| `aisle` | `StringType()` | Yes |
| `department_id` | `LongType()` | Yes |
| `department` | `StringType()` | Yes |
| `add_to_cart_order` | `LongType()` | Yes |
| `reordered` | `LongType()` | Yes |

Kolom `order_dow`, `order_hour_of_day`, `reordered`, dan `department` lebih cocok menjadi bar chart karena berisi kategori atau kelompok. Kolom `order_number`, `days_since_prior_order`, dan `add_to_cart_order` dibuat menjadi histogram dan boxplot karena nilainya numerik dan dapat dianalisis distribusinya.

Chart yang digunakan pada EDA ini adalah:

- `null_rate_by_column.png`
- `hist_order_number.png`
- `boxplot_order_number.png`
- `hist_days_since_prior_order.png`
- `boxplot_days_since_prior_order.png`
- `hist_add_to_cart_order.png`
- `boxplot_add_to_cart_order.png`
- `hist_reordered.png`
- `orders_by_day_of_week.png`
- `orders_by_hour_of_day.png`
- `top_department.png`

### 1. Null Rate by Column

![Null Rate by Column](/images/null_rate_by_column.png)

Alasan kami membuat chart null rate adalah untuk mengetahui kolom mana yang memiliki nilai kosong dan seberapa besar persentasenya. Hal ini penting karena nilai null dapat mempengaruhi proses transformasi, agregasi, dan load ke warehouse.

Dari chart yang terbentuk untuk batch ini, hanya kolom `days_since_prior_order` yang memiliki null, yaitu 54 baris dari 968 baris atau sekitar 5.58%. Kolom lain tidak memiliki null pada batch ini.

Dapat disimpulkan bahwa `days_since_prior_order` tidak boleh langsung dianggap data rusak. Null pada kolom ini kemungkinan menunjukkan order pertama atau kondisi ketika belum ada order sebelumnya. Karena itu, pada pipeline cleaning kolom ini tetap diperbolehkan null dan ditambahkan flag `is_days_since_prior_order_missing`.

### 2. Histogram Order Number

![Histogram Order Number](/images/hist_order_number.png)

Alasan kami mengubah kolom `order_number` menjadi histogram adalah untuk melihat persebaran urutan order user. Histogram membantu melihat apakah data didominasi oleh user baru atau user yang sudah sering melakukan order.

Dari chart yang terbentuk untuk batch ini, mayoritas baris berada pada order number rendah sampai menengah. Median `order_number` adalah 13, kuartil 75 berada di 25, dan nilai maksimum mencapai 96.

Dapat disimpulkan bahwa nilai `order_number` yang tinggi tidak otomatis dianggap outlier atau data salah. Pada pipeline, cleaning cukup memastikan `order_number >= 1`.

### 3. Boxplot Order Number

![Boxplot Order Number](/images/boxplot_order_number.png)

Alasan kami membuat boxplot untuk `order_number` adalah untuk melihat median, rentang nilai umum, dan nilai tinggi yang muncul pada batch.

Dari chart yang terbentuk, distribusi `order_number` condong ke kanan. Sebagian besar data berada pada nilai rendah sampai menengah, tetapi tetap ada user dengan order number tinggi sampai 96.

Dapat disimpulkan bahwa nilai tinggi pada `order_number` masih masuk akal secara bisnis. Nilai tersebut tidak perlu dihapus hanya karena jarang muncul pada batch ini.

### 4. Histogram Days Since Prior Order

![Histogram Days Since Prior Order](/images/hist_days_since_prior_order.png)

Alasan kami membuat histogram untuk `days_since_prior_order` adalah untuk melihat pola jeda hari antar order. Kolom ini penting karena menunjukkan perilaku repeat order.

Dari chart yang terbentuk, median `days_since_prior_order` adalah 7 hari, kuartil 75 adalah 14 hari, dan nilai maksimum adalah 30 hari. Nilai 30 cukup sering muncul sebagai batas atas.

Dapat disimpulkan bahwa nilai 30 tidak langsung dianggap outlier. Nilai tersebut kemungkinan merupakan batas maksimum atau capped value dari sumber data. Null juga tidak diisi dengan 0 karena "tidak ada order sebelumnya" berbeda dengan "order lagi setelah 0 hari".

### 5. Boxplot Days Since Prior Order

![Boxplot Days Since Prior Order](/images/boxplot_days_since_prior_order.png)

Alasan kami membuat boxplot untuk `days_since_prior_order` adalah untuk melihat rentang umum jeda order dan apakah ada nilai yang terlalu ekstrem.

Dari chart yang terbentuk, sebagian besar nilai non-null berada pada rentang 5 sampai 14 hari, dengan nilai maksimum 30.

Dapat disimpulkan bahwa range tersebut masih wajar. Pada pipeline, nilai `days_since_prior_order` boleh null atau berada pada range 0 sampai 30.

### 6. Histogram Add to Cart Order

![Histogram Add To Cart Order](/images/hist_add_to_cart_order.png)

Alasan kami membuat histogram untuk `add_to_cart_order` adalah untuk melihat posisi produk ketika dimasukkan ke keranjang. Nilai kecil berarti produk ditambahkan lebih awal.

Dari chart yang terbentuk, sebagian besar produk ditambahkan pada posisi awal. Median `add_to_cart_order` adalah 6, kuartil 75 adalah 11, dan nilai maksimum adalah 34.

Dapat disimpulkan bahwa posisi yang lebih tinggi masih dapat terjadi pada order dengan jumlah item banyak. Pada pipeline, cleaning cukup memastikan `add_to_cart_order >= 1`.

### 7. Boxplot Add to Cart Order

![Boxplot Add To Cart Order](/images/boxplot_add_to_cart_order.png)

Alasan kami membuat boxplot untuk `add_to_cart_order` adalah untuk melihat apakah posisi item di keranjang memiliki nilai yang tidak masuk akal.

Dari chart yang terbentuk, distribusi condong ke kanan. Banyak produk masuk pada posisi awal, sementara sebagian kecil berada pada posisi lebih tinggi.

Dapat disimpulkan bahwa nilai tinggi pada `add_to_cart_order` tidak perlu dibuang selama nilainya masih positif. Nilai 0 atau negatif baru dianggap invalid.

### 8. Histogram Reordered

![Histogram Reordered](/images/hist_reordered.png)

Alasan kami membuat chart untuk `reordered` adalah karena kolom ini merupakan flag biner yang menunjukkan apakah produk pernah dipesan sebelumnya.

Dari chart yang terbentuk, terdapat 640 baris dengan `reordered = 1` dan 328 baris dengan `reordered = 0`. Artinya, sekitar 66% baris pada batch ini adalah produk yang sudah pernah dipesan sebelumnya.

Dapat disimpulkan bahwa `reordered` harus diperlakukan sebagai kolom biner. Pada pipeline, nilai yang valid hanya 0 dan 1.

### 9. Orders by Day of Week

![Orders by Day of Week](/images/orders_by_day_of_week.png)

Alasan kami membuat chart untuk `order_dow` adalah untuk melihat pola order berdasarkan hari. Kolom ini berbentuk kode kategori 0 sampai 6, sehingga lebih cocok menggunakan bar chart dibanding boxplot.

Dari chart yang terbentuk, day 0 memiliki jumlah baris tertinggi pada batch ini, disusul day 6. Day 1 dan day 5 lebih rendah.

Dapat disimpulkan bahwa variasi antar hari merupakan hal normal. Cleaning tidak boleh menghapus data hanya karena suatu hari lebih tinggi atau lebih rendah. Pipeline cukup memastikan `order_dow` berada pada range 0 sampai 6.

### 10. Orders by Hour of Day

![Orders by Hour of Day](/images/orders_by_hour_of_day.png)

Alasan kami membuat chart untuk `order_hour_of_day` adalah untuk melihat pola waktu order dalam satu hari. Kolom ini berupa kategori jam 0 sampai 23, sehingga bar chart lebih tepat digunakan.

Dari chart yang terbentuk, batch ini banyak muncul pada pagi, siang, dan sore hari. Nilai terbesar terlihat sekitar jam 9 dan jam 17. Pada batch ini, nilai yang muncul berada pada jam 6 sampai 22.

Dapat disimpulkan bahwa range 6 sampai 22 hanya berlaku untuk batch ini. Pada pipeline, aturan validasi yang lebih tepat adalah `order_hour_of_day` berada pada range 0 sampai 23.

### 11. Top Department

![Top Department](/images/top_department.png)

Alasan kami membuat chart untuk `department` adalah untuk melihat kategori produk yang paling dominan pada batch. Kolom ini kategorikal, sehingga top category bar chart lebih mudah dibaca.

Dari chart yang terbentuk, department `produce` menjadi department terbesar, disusul `dairy eggs`, `snacks`, `beverages`, dan `frozen`. Pada report kategori juga ditemukan nilai `missing` dalam jumlah kecil.

Dapat disimpulkan bahwa dominasi department tertentu masih wajar pada data grocery. Namun nilai `missing` perlu ditangani secara eksplisit. Pada pipeline, nilai department `"missing"` diubah menjadi null agar lebih jelas bahwa nilai tersebut bukan nama department valid.

## Pre-processing

Dengan hasil analisis yang telah didapat, kami memutuskan bahwa pada pipeline data, data akan diproses melalui langkah berikut:

1. Membaca data raw dari `data_lake/orders/raw_parquet/`.
2. Melakukan casting tipe data untuk kolom numerik seperti `order_id`, `user_id`, `order_number`, `order_dow`, `order_hour_of_day`, `product_id`, `aisle_id`, `department_id`, `add_to_cart_order`, dan `reordered`.
3. Memastikan kolom penting tidak null, yaitu `order_id`, `user_id`, `order_number`, `order_dow`, `order_hour_of_day`, `product_id`, `product_name`, `aisle_id`, `aisle`, `department_id`, `department`, `add_to_cart_order`, dan `reordered`.
4. Membiarkan `days_since_prior_order` bernilai null karena null pada kolom ini masih memiliki makna bisnis.
5. Menambahkan kolom `is_days_since_prior_order_missing` untuk menandai baris yang memiliki null pada `days_since_prior_order`.
6. Melakukan validasi range:
   - `order_number >= 1`
   - `order_dow` berada pada 0 sampai 6
   - `order_hour_of_day` berada pada 0 sampai 23
   - `add_to_cart_order >= 1`
   - `reordered` hanya boleh bernilai 0 atau 1
   - `days_since_prior_order` boleh null atau berada pada 0 sampai 30
7. Mengubah `department = "missing"` menjadi null.
8. Menyimpan data valid ke `data_lake/orders/clean_parquet/`.
9. Menyimpan data invalid ke `data_lake/orders/rejected_parquet/` untuk kebutuhan audit.
10. Memuat data valid dari `clean_parquet` ke ClickHouse.

Hal ini bertujuan agar data yang masuk ke warehouse sudah memiliki tipe data yang konsisten, nilai wajib yang lengkap, dan range nilai yang masuk akal. Data invalid tidak langsung dibuang tanpa jejak, tetapi dipisahkan ke `rejected_parquet` agar dapat diperiksa kembali.

Namun perlu diingat, pada data pipeline dengan batch data yang selalu berubah, hasil EDA satu batch ini mungkin tidak merepresentasikan keseluruhan data. Oleh karena itu, aturan cleaning tidak dibuat berdasarkan bentuk distribusi batch ini saja. Contohnya, walaupun pada batch ini `order_hour_of_day` hanya muncul dari 6 sampai 22, pipeline tetap menggunakan range valid 0 sampai 23 karena itu adalah aturan yang lebih stabil untuk kolom jam.

Dengan pendekatan ini, EDA digunakan sebagai dasar untuk memahami data dan menentukan aturan awal, sedangkan pipeline tetap menggunakan validasi yang lebih umum, stabil, dan aman untuk batch berikutnya.
