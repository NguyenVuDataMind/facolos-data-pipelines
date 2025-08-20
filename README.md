# Hệ Thống ETL Production TikTok Shop│   └── utils/                    # Modules tiện ích
│       ├── auth.py              # Xác thực TikTok API
│       ├── database.py          # Thao tác database
│       └── logging.py           # Thiết lập logging
├── sql/                          # Scripts database
│   ├── 01_create_database.sql   # Khởi tạo database
│   └── staging/
│       └── create_tiktok_shop_orders_table.sql
├── config/                       # Cấu hình ứng dụng
│   ├── __init__.py              # Config module init
│   └── settings.py              # Quản lý settings
├── logs/                         # Files log ETL production sẵn sàng triển khai để trích xuất dữ liệu đơn hàng TikTok Shop và tải vào database staging SQL Server với sự điều phối của Apache Airflow.

## Kiến trúc

```
TikTok Shop API → Extractor → Transformer → Loader → SQL Server Staging
                      ↓
              Điều phối Apache Airflow
                      ↓
                 Docker Containers
```

## Tính năng

- **Trích xuất dữ liệu tự động**: Lấy dữ liệu đơn hàng từ TikTok Shop API
- **Chuyển đổi dữ liệu**: Làm phẳng cấu trúc đơn hàng cho database staging
- **Database Staging**: SQL Server với schema được tối ưu cho phân tích
- **Điều phối**: Apache Airflow cho lập lịch và giám sát
- **Container hóa**: Thiết lập Docker cho triển khai nhất quán
- **Xử lý lỗi**: Logging toàn diện và cơ chế thử lại
- **Khả năng mở rộng**: Thiết kế modular để thêm nhiều platform thương mại điện tử

## Cấu trúc dự án

```
tiktok-etl-production/
├── dags/                           # Airflow DAGs
│   └── tiktok_shop_orders_etl_dag.py # Quy trình ETL chính
├── src/                           # Mã nguồn
│   ├── extractors/               # Modules trích xuất dữ liệu
│   │   └── tiktok_shop_extractor.py
│   ├── transformers/             # Modules chuyển đổi dữ liệu
│   │   └── tiktok_shop_transformer.py
│   ├── loaders/                  # Modules tải dữ liệu
│   │   └── tiktok_shop_staging_loader.py
│   ├── utils/                    # Modules tiện ích
│   │   ├── auth.py              # Xác thực TikTok API
│   │   ├── database.py          # Thao tác database
│   │   └── logging.py           # Thiết lập logging
│   └── config/                   # Cấu hình ứng dụng
│       ├── __init__.py          # Config module init
│       └── settings.py          # Quản lý settings
├── sql/                          # Scripts database
│   ├── 01_create_database.sql   # Khởi tạo database
│   └── staging/
│       └── create_tiktok_shop_orders_table.sql
├── logs/                         # Files log
├── tests/                        # Files test
├── .env                          # Environment variables
├── Dockerfile                    # Thiết lập Docker container
├── docker-compose.yml           # Thiết lập multi-container
├── requirements.txt             # Dependencies Python
├── run_etl.py                   # Chạy ETL thủ công
└── README.md                    # File này
```

## Yêu cầu tiên quyết

- Docker và Docker Compose
- Thông tin xác thực TikTok Shop API
- Tối thiểu 4GB RAM cho containers

## Hướng dẫn cài đặt

### 1. Clone và cấu hình

```bash
cd "tiktok-etl-production"

# Chỉnh sửa .env với thông tin xác thực của bạn
notepad .env  # hoặc editor bạn ưa thích
```

### 2. Cấu hình biến môi trường

Chỉnh sửa file `.env` với thông tin TikTok của bạn:

```env
# Cấu hình TikTok Shop API
TIKTOK_APP_KEY=6h2cosrovhjab
TIKTOK_APP_SECRET=your_app_secret_here
TIKTOK_ACCESS_TOKEN=your_access_token_here
TIKTOK_REFRESH_TOKEN=your_refresh_token_here
TIKTOK_SHOP_CIPHER=your_shop_cipher_here

# Cấu hình Database
DB_PASSWORD=FacolosDB2024!
```

### 3. Khởi động hệ thống

```bash
# Build và khởi động tất cả containers
docker-compose up -d

# Kiểm tra trạng thái container
docker-compose ps

# Xem logs
docker-compose logs -f airflow-webserver
```

### 4. Truy cập Airflow

- **Web UI**: http://localhost:8080
- **Username**: admin
- **Password**: admin

### 5. Khởi tạo Database

Database và các bảng staging sẽ được tạo tự động khi containers khởi động. Bạn có thể xác minh bằng cách kiểm tra SQL Server container:

```bash
# Kết nối đến SQL Server (tùy chọn)
docker exec -it tiktok_sqlserver /opt/mssql-tools/bin/sqlcmd -S localhost -U sa -P "FacolosDB2024!"
```

## Sử dụng

### Giao diện Web Airflow

1. Truy cập http://localhost:8080
2. Tìm DAG `tiktok_shop_orders_etl`
3. Bật ON để kích hoạt lập lịch tự động
4. Click "Trigger DAG" để thực thi thủ công

### Thực thi ETL thủ công

Cho testing hoặc chạy một lần:

```bash
# Test kết nối
python run_etl.py test

# Chạy ETL đầy đủ cho 7 ngày gần đây
python run_etl.py etl --days-back 7 --load-mode append

# Chỉ trích xuất đơn hàng
python run_etl.py extract --days-back 1
```

### Giám sát

- **Airflow UI**: http://localhost:8080 - Trạng thái thực thi DAG
- **Logs**: Kiểm tra thư mục `logs/` hoặc container logs
- **Database**: Query bảng staging để xác minh dữ liệu

## Cấu hình

### Lập lịch

DAG được cấu hình chạy mỗi 6 giờ. Chỉnh sửa trong `dags/tiktok_shop_orders_etl_dag.py`:

```python
schedule_interval='0 */6 * * *'  # Mỗi 6 giờ
```

### Batch Size

Điều chỉnh batch size trích xuất trong `config/settings.py`:

```python
etl_batch_size: int = 1000
```

### Logic thử lại

Cấu hình số lần thử và delay:

```python
etl_retry_attempts: int = 3
etl_retry_delay: int = 60  # giây
```

## Schema Database

### Bảng Staging: `Facolos_Staging.tiktok_shop_orders`

Bảng staging sử dụng cấu trúc phẳng với mỗi dòng đại diện cho một line item từ đơn hàng:

- **ETL Metadata**: `etl_batch_id`, `etl_created_at`, `etl_updated_at`
- **Thông tin đơn hàng**: `order_id`, `order_status`, `create_time`, etc.
- **Số tiền đơn hàng**: `total_amount`, `shipping_fee`, `tax_amount`, etc.
- **Thông tin người nhận**: `recipient_name`, `recipient_address_*`, etc.
- **Chi tiết sản phẩm**: `item_id`, `item_name`, `item_quantity`, etc.

## Khắc phục sự cố

### Vấn đề thường gặp

1. **Lỗi xác thực API**
   - Kiểm tra thông tin TikTok trong `.env`
   - Xác minh tính hợp lệ của token và refresh tokens

2. **Vấn đề kết nối Database**
   - Đảm bảo SQL Server container đang chạy
   - Kiểm tra thông tin xác thực database và connection string

3. **DAG Airflow không xuất hiện**
   - Kiểm tra syntax DAG để tìm lỗi
   - Xác minh file trong thư mục `dags/`
   - Kiểm tra logs Airflow scheduler

### Lệnh debug

```bash
# Kiểm tra container logs
docker-compose logs airflow-scheduler
docker-compose logs sqlserver

# Test kết nối API
python -c "from src.extractors.tiktok_shop_extractor import TikTokShopOrderExtractor; TikTokShopOrderExtractor().test_api_connection()"

# Test kết nối database
python -c "from src.loaders.tiktok_shop_staging_loader import TikTokShopOrderLoader; TikTokShopOrderLoader().test_connection()"
```

### Files Log

- **ETL Logs**: `logs/tiktok_etl_*.log`
- **Airflow Logs**: `logs/dag_id/task_id/execution_date/`
- **Container Logs**: `docker-compose logs [service_name]`

## Phát triển

### Thêm Extractors mới

1. Tạo extractor mới trong `src/extractors/`
2. Theo pattern từ `tiktok_shop_extractor.py`
3. Thêm transformer và loader tương ứng
4. Tạo DAG mới hoặc mở rộng DAG hiện tại

### Testing

```bash
# Chạy tests thủ công
python run_etl.py test

# Test các component riêng lẻ
python -m pytest tests/  # Khi tests được thêm vào
```

### Cấu trúc Code

- **Extractors**: Xử lý kết nối API và trích xuất dữ liệu
- **Transformers**: Chuyển đổi dữ liệu thô sang định dạng staging
- **Loaders**: Chèn dữ liệu vào database
- **Utils**: Chức năng chia sẻ (auth, database, logging)

## Cân nhắc về hiệu suất

- **Rate Limiting**: Delay tích hợp cho API calls
- **Batch Processing**: Xử lý datasets lớn theo chunks
- **Incremental Loading**: Tránh dữ liệu trùng lặp
- **Connection Pooling**: Kết nối database hiệu quả

## Bảo mật

- Biến môi trường cho dữ liệu nhạy cảm
- Không hardcode thông tin xác thực trong code
- Bảo vệ SQL injection qua parameterized queries
- Cô lập container cho các services

## Giám sát và Cảnh báo

Cải tiến trong tương lai có thể bao gồm:
- Thông báo email khi DAG thất bại
- Giám sát chất lượng dữ liệu
- Thu thập metrics hiệu suất
- Tích hợp với công cụ giám sát (Grafana, Prometheus)

## Giấy phép

Chỉ sử dụng nội bộ - Hệ thống ETL Công ty Facolos
