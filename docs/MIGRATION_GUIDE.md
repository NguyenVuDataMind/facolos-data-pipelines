# 🚀 Hướng Dẫn Migration Database Staging

## 📋 Tổng Quan

Migration này thực hiện các thay đổi sau:

1. **Database**: `Facolos_Database` → `Facolos_Staging`
2. **Bảng TikTok Shop**: `Facolos_Staging.data` → `staging.tiktok_shop_order_detail`
3. **Cấu trúc mới**: Chuẩn bị cho việc tích hợp MISA CRM và các platform khác

## ✅ Các Thay Đổi Đã Hoàn Thành

### 1. File Cấu Hình
- ✅ `config/settings.py` - Cập nhật database name và schema mappings
- ✅ `.env` - Cập nhật `SQL_SERVER_DATABASE=Facolos_Staging`

### 2. File SQL Scripts
- ✅ `sql/01_create_database.sql` - Tạo database `Facolos_Staging`
- ✅ `sql/staging/create_tiktok_shop_orders_table.sql` - Bảng `staging.tiktok_shop_order_detail`
- ✅ `sql/staging/create_facolos_enterprise_schemas.sql` - Cập nhật tất cả schemas
- ✅ `sql/staging/create_misa_crm_tables.sql` - **MỚI**: Schema và bảng cho MISA CRM
- ✅ `sql/migration_rename_database_and_table.sql` - **MỚI**: Script migration hoàn chỉnh
- ✅ `fix_missing_columns.sql` - Cập nhật tên database và bảng

### 3. File Python
- ✅ `check_staging_data.py` - Cập nhật tên database và bảng
- ✅ `test_migration.py` - **MỚI**: Script test migration

### 4. Schema Mappings Mới
```python
schema_mappings = {
    'tiktok_shop': 'staging',           # TikTok Shop orders
    'shopee': 'shopee',                 # Shopee (future)
    'lazada': 'lazada',                 # Lazada (future)
    'tiktok_ads': 'tiktok_ads',         # TikTok Ads (future)
    'misa_crm': 'misa_crm',             # MISA CRM
    'other_platforms': 'other_platforms' # Các platform khác
}
```

## 🔧 Cách Thực Hiện Migration

### Bước 1: Kiểm Tra Cấu Hình
```bash
python test_migration.py
```

**Kết quả mong đợi**: 4/5 tests pass (Database Connection sẽ fail vì database chưa tồn tại)

### Bước 2: Chạy Migration Script
```sql
-- Kết nối SQL Server Management Studio hoặc sqlcmd
-- Chạy file:
sql/migration_rename_database_and_table.sql
```

**Script này sẽ**:
- Tạo database `Facolos_Staging`
- Sao chép dữ liệu từ `Facolos_Database.Facolos_Staging.data` sang `Facolos_Staging.staging.tiktok_shop_order_detail`
- Tạo các schema mới: `misa_crm`, `shopee`, `lazada`, `etl_control`
- Tạo ETL control tables

### Bước 3: Tạo MISA CRM Tables
```sql
-- Chạy file:
sql/staging/create_misa_crm_tables.sql
```

**Script này sẽ tạo**:
- `misa_crm.customers` - Bảng khách hàng MISA CRM (đầy đủ structure)
- `misa_crm.orders` - Bảng đơn hàng (placeholder)
- `misa_crm.products` - Bảng sản phẩm (placeholder)

### Bước 4: Kiểm Tra Lại
```bash
python test_migration.py
```

**Kết quả mong đợi**: 5/5 tests pass

### Bước 5: Test ETL Pipeline
```bash
python check_staging_data.py
```

## 📊 Cấu Trúc Database Mới

```
Facolos_Staging/
├── staging/
│   └── tiktok_shop_order_detail    # TikTok Shop orders (116+ columns)
├── misa_crm/
│   ├── customers                   # MISA CRM customers (77+ columns)
│   ├── orders                      # MISA CRM orders (placeholder)
│   └── products                    # MISA CRM products (placeholder)
├── shopee/                         # Shopee data (future)
├── lazada/                         # Lazada data (future)
├── tiktok_ads/                     # TikTok Ads data (future)
├── other_platforms/                # Các platform khác (future)
└── etl_control/
    ├── batch_runs                  # ETL batch tracking
    └── data_sources                # Data source configuration
```

## 🔍 MISA CRM Integration

### API Structure (từ RequestAPI_MISA_CRM.ipynb)
- **Endpoint**: `https://crmconnect.misa.vn/api/v2/Customers`
- **Authentication**: Bearer token
- **Client ID**: `FACOLOS`

### Bảng `misa_crm.customers` Columns
- **Basic Info**: id, account_name, account_number, tax_code
- **Contact**: office_tel, office_email, owner_name
- **Address**: billing_address, shipping_address (với đầy đủ chi tiết)
- **Business**: debt, order_sales, number_orders, average_order_value
- **Products**: list_product_category, list_product
- **Tracking**: last_interaction_date, number_days_not_interacted

## 🚨 Lưu Ý Quan Trọng

### 1. Backup Dữ Liệu
```sql
-- Backup database cũ trước khi migration
BACKUP DATABASE Facolos_Database 
TO DISK = 'C:\Backup\Facolos_Database_backup.bak'
```

### 2. Không Xóa File Docs
- ✅ Tất cả file trong folder `docs/` được giữ nguyên
- ✅ `RequestAPI_MISA_CRM.ipynb` được sử dụng làm tài liệu tham khảo
- ✅ Các file tài liệu TikTok được bảo toàn

### 3. Compatibility
- ✅ Backward compatibility với existing code thông qua `settings.staging_schema` và `settings.staging_table_orders`
- ✅ Các file Python loader/extractor tự động sử dụng tên mới

## 📝 Bước Tiếp Theo

### 1. Phát Triển MISA CRM ETL
- Tạo `src/extractors/misa_crm_extractor.py`
- Tạo `src/transformers/misa_crm_transformer.py`
- Tạo `src/loaders/misa_crm_loader.py`

### 2. Tích Hợp Các Platform Khác
- Shopee API integration
- Lazada API integration
- TikTok Ads integration

### 3. Data Mart Development
- Tạo aggregated tables trong `data_mart` schema
- Business intelligence reports
- Cross-platform analytics

## 🎯 Kết Quả Mong Đợi

✅ **Database staging rõ ràng, dễ mở rộng**
✅ **Chuẩn bị sẵn sàng cho MISA CRM integration**
✅ **Cấu trúc scalable cho nhiều data sources**
✅ **ETL control và monitoring**
✅ **Backward compatibility với code hiện tại**

---

**Tác giả**: Facolos Data Team  
**Ngày tạo**: 2024-08-24  
**Version**: 1.0
