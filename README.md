# Airflow Lab: Apache Airflow 3.1 + PostgreSQL + MinIO

Data Engineering kursu çərçivəsində Docker Compose ilə qurulmuş lokal Airflow təcrübə mühiti.
Repoda mühitin özü (Airflow, PostgreSQL, MinIO) və kurs boyu yazılmış DAG-lar var: CSV-dən Postgres-ə yükləmə, TaskFlow API, MinIO ilə işləmək, Dynamic Task Mapping, Branching, Asset-lər və Sensor-lar.

## Nələr var

- **Apache Airflow 3.1.0**: `api-server`, `scheduler`, `dag-processor`, `triggerer` (LocalExecutor)
- **PostgreSQL 16**: Airflow metadata bazası və ETL hədəf bazası (`etl_db`)
- **MinIO**: S3 uyğun object storage
- **10 DAG**: əsas dərs nümunələrindən irəli səviyyəli tapşırıqlara qədər

## Texnologiyalar

| Texnologiya | Məqsəd |
|---|---|
| Apache Airflow 3.1.0 | İş axınlarının (DAG) idarəsi və planlaşdırılması |
| Docker, Docker Compose | Mühitin konteynerlərdə qurulması |
| PostgreSQL 16 | Metadata və ETL hədəf bazası |
| MinIO | S3 uyğun fayl saxlanması |
| Python provider paketləri | `standard`, `postgres`, `common-sql`, `fab`, `smtp`, `amazon` |

## Layihə strukturu

```
airflow-lab/
├── airflow/
│   ├── dags/          # bütün DAG faylları
│   ├── files/         # nümunə CSV / JSON datalar
│   ├── config/
│   ├── plugins/
│   └── logs/          # Git-ə getmir
├── docs/
│   ├── airflow-advanced-tasks.md   # irəli səviyyəli tapşırıqların izahı
│   └── images/                     # ekran görüntüləri
├── Dockerfile
├── docker-compose.yaml
├── requirements.txt
├── .env.example       # mühit dəyişənlərinin şablonu
└── README.md
```

## Servislər və portlar

| Servis | Ünvan (host) | Qeyd |
|---|---|---|
| Airflow UI | http://localhost:8081 | Giriş: `admin` / `admin` (yalnız lokal lab üçün) |
| PostgreSQL | `localhost:5436` | Konteynerlər arası ünvan: `postgres:5432` |
| MinIO API | `localhost:9002` | Konteynerlər arası ünvan: `http://minio:9000` |
| MinIO Console | http://localhost:9003 | Giriş `.env`-dəki istifadəçi adı və şifrə ilə |

## Quraşdırma

**Tələb olunanlar:** Docker Desktop və Git.

1. Repozitoriyanı klonla:
   ```bash
   git clone https://github.com/ceyhun2306/airflow-lab.git
   cd airflow-lab
   ```
2. Mühit faylını yarat və dəyərləri doldur:
   ```bash
   cp .env.example .env
   ```
   `CHANGE_ME` yazılmış yerləri doldur. `AIRFLOW__DATABASE__SQL_ALCHEMY_CONN` içindəki şifrə `POSTGRES_PASSWORD` ilə eyni olmalıdır.
3. Fernet açarı və gizli açarlar üçün təsadüfi dəyərlər yarat:
   ```bash
   docker run --rm apache/airflow:3.1.0 python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
   docker run --rm apache/airflow:3.1.0 python -c "import secrets; print(secrets.token_hex(32))"
   ```
   Birinci çıxışı `AIRFLOW__CORE__FERNET_KEY`-ə, ikinci əmri iki dəfə işlədib `AIRFLOW__API__SECRET_KEY` və `AIRFLOW__API_AUTH__JWT_SECRET`-ə yaz.
4. Image-i qur və servisləri qaldır:
   ```bash
   docker compose build
   docker compose up -d
   docker ps
   ```
5. http://localhost:8081 ünvanını aç və daxil ol.

## Əlavə hazırlıq

### ETL bazası və cədvəl (CSV → Postgres DAG-ları üçün)

```bash
docker exec -it postgres psql -U airflow -d airflow -c "CREATE DATABASE etl_db;"
docker exec -it postgres psql -U airflow -d etl_db -c "CREATE TABLE IF NOT EXISTS customers (customer_id VARCHAR(20), full_name VARCHAR(200), city VARCHAR(100), registration_date DATE, membership_level VARCHAR(50));"
```

### Airflow connection-ları (Admin → Connections)

| Connection Id | Tip | Məqsəd | Əsas parametrlər |
|---|---|---|---|
| `postgres_etl` | Postgres | CSV-ni `etl_db`-yə yazmaq | host `postgres`, port `5432`, database `etl_db` |
| `fs_default` | File (path) | `FileSensor` üçün | Extra: `{"path": "/"}` |
| `smtp_default` | SMTP | E-poçt bildirişləri | host `smtp.gmail.com`, port `587`, Extra: `"disable_ssl": true` və `"from_email"` |
| `minio_conn` | Amazon Web Services | MinIO-ya qoşulmaq | Extra: `{"endpoint_url": "http://minio:9000", "region_name": "us-east-1"}` |

Gmail üçün adi şifrə yox, Google hesabında yaradılmış **App Password** istifadə olunur.

### Variable (Admin → Variables)

`company_name`: `taskflow_demo_dag`-ın Jinja nümunəsi üçün lazımdır.

### MinIO bucket

MinIO Console-da `incoming` bucket-i və içində `raw/` qovluğu yaradılır. `airflow/files/` qovluğundakı CSV və JSON faylları ora yüklənir (`minio_files_pipeline_dag` üçün).

## DAG-lar

| Fayl | Mövzu |
|---|---|
| `csv_to_postgres_dag.py` | FileSensor ilə faylı yoxlayıb CSV-ni Postgres-ə yükləmə, sonda e-poçt |
| `customers_csv_to_postgres_dag.py` | Eyni axın + 3 təkrar cəhd (5 dəqiqə aralıqla), 4 saatdan bir icra |
| `taskflow_demo_dag.py` | TaskFlow API (`@dag`, `@task`), Param, XCom, Jinja və Variable |
| `postgres_hook_select_check.py` | `PostgresHook` ilə bazaya qoşulma və SELECT yoxlaması |
| `minio_bash_operator_pipeline.py` | MinIO ilə `BashOperator` əsaslı pipeline |
| `minio_files_pipeline_dag.py` | MinIO fayl siyahısı, Dynamic Task Mapping, CSV/JSON branching, trigger rule-lar |
| `sales_assets_dag.py` | `sales_producer_dag` və `sales_consumer_dag`: Asset ilə data-aware scheduling |
| `external_sensor_dag.py` | `source_dag` və `report_waiter_dag`: deferrable `ExternalTaskSensor` |

İrəli səviyyəli DAG-ların (mapping, branching, asset, sensor, catchup/backfill, triggerer) ətraflı izahı və ekran görüntüləri:
[docs/airflow-advanced-tasks.md](docs/airflow-advanced-tasks.md)

## Faydalı əmrlər

```bash
docker compose up -d                     # servisləri qaldır
docker compose stop                      # dayandır (data qalır)
docker compose down                      # konteynerləri sil (volume-lar qalır)
docker compose down -v                   # konteynerlər və volume-lar (data silinir)
docker logs airflow-scheduler --tail 50  # log-lara bax
docker logs airflow-dag-processor --tail 50   # DAG import xətaları üçün
docker exec airflow-scheduler airflow backfill create --help
```

## Təhlükəsizlik qeydləri

- `.env` faylı `.gitignore`-dadır və repoya düşmür. Real şifrələri yalnız orada saxla.
- `.env.example` yalnız şablondur, real dəyər yazma.
- `admin` / `admin` giriş məlumatı yalnız lokal təcrübə mühiti üçündür.
- E-poçt üçün Gmail App Password-u koda və ya Git-ə yazma. Airflow UI-dəki connection-da saxla.

## Müəllif

**Ceyhun**, Matrix Academy, Data Engineering kursu.
