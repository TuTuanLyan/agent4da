# Project Structure & Environment Guide

## 1. Cấu trúc thư mục
Dưới đây là sơ đồ tổ chức cây thư mục của dự án:

```text
.
├── app/                    # Core application (UI, API, service chính)
├── code/                   # Code xử lý chính (business logic, model, pipeline,...)
├── data/                   # Dữ liệu (input/output, sample data)
├── imgs/                   # Hình ảnh (phục vụ docs, demo)
├── log/                    # Log runtime của hệ thống
├── monitoring/             # Cấu hình monitoring (Prometheus, Grafana,...)
├── notebook/               # Jupyter notebook (EDA, thử nghiệm nhanh)
├── orchestration/          # Điều phối quy trình (Airflow, scheduler,...)
├── script/                 # Các script tiện ích chạy nhanh (bash, util)
├── utils/                  # Các hàm tiện ích dùng chung cho toàn dự án
├── dockerfile              # File cấu hình build Docker image
├── docker-compose.*.yml    # Cấu hình các service (Kafka, MinIO, Postgres, ...)
├── agent4da.env.yml        # File export môi trường (Conda)
├── README.md
└── .gitignore
```

### Nguyên tắc quản lý:
* **`app/`**: UI FE, Core Endpoint Backend FastAPI
* **`code/`**: Nơi tập trung xử lý logic nghiệp vụ; tuyệt đối không để lẫn lộn vào thư mục `app`.
* **`utils/`**: Chỉ chứa các module được tái sử dụng ở nhiều nơi khác nhau.
* **`script/`**: Dùng cho các tác vụ phụ trợ, không chứa logic vận hành.
* **`notebook/`**: Chỉ dùng để nghiên cứu; không được gọi trực tiếp trong môi trường production.

---

## 2. Thiết lập môi trường (Environment)
Để tạo lại môi trường làm việc từ file cấu hình có sẵn:

**Tạo môi trường mới:**
```bash
conda env create -f agent4da.env.yml
```

**Kích hoạt môi trường:**
```bash
conda activate agent4daenv
```

---

## 3. Cập nhật thư viện (QUAN TRỌNG)
Để đảm bảo tính đồng nhất giữa các thành viên trong đội ngũ, khi cài đặt thêm bất kỳ package nào, bạn cần thực hiện theo các bước sau:

1. **Cài đặt package:**
```bash
conda install <package_name>
# hoặc
pip install <package_name>
```

2. **BẮT BUỘC export lại cấu hình môi trường:**
```bash
conda env export > agent4da.env.yml
```

**Lưu ý:** Luôn luôn kiểm tra và commit file `agent4da.env.yml` sau khi cập nhật để đồng bộ hóa môi trường trên Git.

## Shutdown
```bash
docker compose -f docker-compose.spark.yml -f docker-compose.minio.yml -f docker-compose.kafka.yml down
```

## View docker ps with norm format
```bash
docker ps --format "table {{.ID}}\t{{.Names}}\t{{.Status}}\t{{.Image}}"
```