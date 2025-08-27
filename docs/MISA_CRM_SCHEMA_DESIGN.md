# 🏢 MISA CRM Schema Design

## 📋 Tổng Quan

Dựa trên phân tích file `docs/RequestsAPI_MISA_CRM.ipynb`, hệ thống MISA CRM có 5 API endpoints chính:

1. **Customers** - `/api/v2/Customers` - Dữ liệu khách hàng
2. **Sales Orders** - `/api/v2/SaleOrders` - Dữ liệu đơn hàng bán
3. **Contacts** - `/api/v2/Contacts` - Dữ liệu liên hệ
4. **Products** - `/api/v2/Products` - Dữ liệu sản phẩm/dịch vụ
5. **Stocks** - `/api/v2/Stocks` - Dữ liệu kho

## 🗄️ Schema Design

### 1. `misa_crm.customers` - Khách Hàng

**Mục đích**: Lưu trữ thông tin khách hàng từ MISA CRM

**Columns chính** (77+ columns):
- **Primary Key**: `id` (INT)
- **Business Info**: `account_name`, `account_number`, `tax_code`, `account_type`
- **Contact Info**: `office_tel`, `office_email`, `owner_name`
- **Address**: `billing_address`, `shipping_address` (với chi tiết đầy đủ)
- **Financial**: `debt`, `debt_limit`, `order_sales`, `average_order_value`
- **Sales Metrics**: `number_orders`, `purchase_date_recent`, `purchase_date_first`
- **Products**: `list_product_category`, `list_product`
- **Interaction**: `last_interaction_date`, `last_call_date`, `number_days_not_interacted`

**Relationships**: 
- One-to-Many với `sales_orders` (customer_id)
- One-to-Many với `contacts` (customer_id)

### 2. `misa_crm.sales_orders` - Đơn Hàng Bán

**Mục đích**: Lưu trữ thông tin đơn hàng bán từ MISA CRM

**Columns chính** (30+ columns):
- **Primary Key**: `id` (INT)
- **Foreign Key**: `customer_id` (INT) → `customers.id`
- **Order Info**: `sale_order_no`, `sale_order_name`, `sale_order_date`
- **Financial**: `sale_order_amount`, `exchange_rate`
- **Status**: `status`, `delivery_status`
- **Dates**: `book_date`, `deadline_date`, `due_date`
- **Campaign**: `campaign_name`, `quote_name`
- **Contact**: `contact_name`, `account_name`

**Relationships**:
- Many-to-One với `customers` (customer_id)
- Potential Many-to-Many với `products` (order items)

### 3. `misa_crm.contacts` - Liên Hệ

**Mục đích**: Lưu trữ thông tin liên hệ và người đại diện khách hàng

**Columns chính** (25+ columns):
- **Primary Key**: `id` (INT)
- **Foreign Key**: `customer_id` (INT) → `customers.id`
- **Personal Info**: `contact_name`, `first_name`, `last_name`, `salutation`
- **Position**: `title`, `department`
- **Contact**: `mobile`, `other_phone`, `email`, `fax`
- **Address**: `mailing_address`, `other_address`
- **Business**: `account_name`, `contact_code`

**Relationships**:
- Many-to-One với `customers` (customer_id)

### 4. `misa_crm.products_services` - Sản Phẩm/Dịch Vụ

**Mục đích**: Lưu trữ catalog sản phẩm và dịch vụ

**Columns chính** (15+ columns):
- **Primary Key**: `id` (INT)
- **Product Info**: `product_name`, `product_code`, `product_category`
- **Pricing**: `unit_price`, `tax`
- **Details**: `usage_unit`, `description`
- **Management**: `owner_name`, `inactive`

**Relationships**:
- Many-to-Many với `sales_orders` (through order items)
- Referenced in `customers.list_product`

### 5. `misa_crm.stocks` - Kho

**Mục đích**: Lưu trữ thông tin kho hàng

**Columns chính** (10+ columns):
- **Primary Key**: `act_database_id` (INT)
- **Stock Info**: `stock_code`, `stock_name`, `description`
- **Management**: `created_by`, `modified_by`, `inactive`
- **Dates**: `created_date`, `modified_date`
- **System**: `async_id`

**Relationships**:
- Potential relationship với `products` (stock locations)

## 🔗 Relationship Diagram

```
customers (1) ←→ (M) sales_orders
customers (1) ←→ (M) contacts
products_services (M) ←→ (M) sales_orders [through order_items]
stocks (1) ←→ (M) products_services [potential]
```

## 📊 ETL Considerations

### Data Volume Estimates
- **Customers**: ~2,000+ records
- **Sales Orders**: ~10,000+ records
- **Contacts**: ~5,000+ records  
- **Products**: ~500+ records
- **Stocks**: ~50+ records

### Update Frequency
- **Customers**: Daily (business metrics update)
- **Sales Orders**: Real-time/Hourly
- **Contacts**: Weekly
- **Products**: Weekly
- **Stocks**: Daily

### API Pagination
- All endpoints support pagination
- Recommended page size: 100-500 records
- Use `offset` and `limit` parameters

## 🔧 Technical Specifications

### Authentication
- **Method**: Bearer Token
- **Client ID**: `FACOLOS`
- **Token Endpoint**: `/api/v2/Account`
- **Token Expiry**: ~24 hours

### Data Types Mapping
- **MISA API** → **SQL Server**
- `string` → `NVARCHAR(MAX)` or specific length
- `number` → `DECIMAL(18,2)` for currency, `INT` for counts
- `boolean` → `BIT`
- `datetime` → `DATETIME2`

### ETL Metadata
All tables include:
- `etl_batch_id` (UNIQUEIDENTIFIER)
- `etl_created_at` (DATETIME2)
- `etl_updated_at` (DATETIME2)

## 🎯 Implementation Priority

1. **Phase 1**: `customers` (foundation table)
2. **Phase 2**: `sales_orders` (business critical)
3. **Phase 3**: `contacts` (customer relationships)
4. **Phase 4**: `products_services` (catalog)
5. **Phase 5**: `stocks` (inventory management)

## 📝 Notes

- All tables designed for incremental updates
- Support for soft deletes (inactive flag)
- Comprehensive indexing for performance
- Foreign key constraints for data integrity
- Extended properties for documentation

---

**Tác giả**: Facolos Data Team  
**Ngày tạo**: 2024-08-25  
**Dựa trên**: `docs/RequestsAPI_MISA_CRM.ipynb`
