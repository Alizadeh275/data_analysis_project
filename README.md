# ⚙️ FastAPI Data Analysis Backend

A production-ready backend API built with **FastAPI**, **PostgreSQL**, and **Docker Compose**, designed for data aggregation, transformation, and ETL (Extract–Transform–Load) operations.

---

## 🚀 Features

- 🧩 Modular architecture (routers, services, and core layers)
- 🗄️ PostgreSQL database integration
- 📊 ETL pipeline for sample Excel data transformation
- ⚙️ Dynamic aggregation and dimension endpoints
- 🐳 Full Docker Compose setup (API, DB, Adminer)
- 🔁 Auto-reload during development

---

## 📁 Project Structure

```
.
├── app
│   ├── core
│   │   ├── constants.py            # Global constants and settings
│   │   └── database.py             # Database session and engine setup
│   ├── main.py                     # FastAPI entry point
│   ├── models.py                   # SQLAlchemy models
│   ├── routers                     # API routers for different modules
│   │   ├── aggregations_router.py  # Aggregation endpoints
│   │   ├── dimensions_router.py    # Dimensions endpoints
│   │   ├── etl_router.py           # ETL API routes
│   │   └── __init__.py
│   ├── services                    # Core business logic and ETL services
│   │   ├── aggregation_service.py  # Aggregation logic
│   │   ├── db_cleaner.py           # Database maintenance utilities
│   │   └── etl_service
│   │       ├── excel_transformer.py # Excel data transformation logic
│   │       ├── loader.py           # Data loading logic
│   │       ├── runner.py           # ETL orchestration
│   │       └── __init__.py
│   └── static                      # Example or demo data files
│       ├── 2016.xlsx
│       ├── data_long.csv
│       └── data_long.xlsx
├── docker-compose.yml              # Multi-service orchestration file
├── Dockerfile                      # Image definition for FastAPI app
└── requirements.txt                # Python dependencies
```

---

## ⚙️ Setup & Installation

### 1. Clone the repository
```bash
git clone https://github.com/Alizadeh275/data_analysis_project.git
cd data_analysis_project
```

### 2. Create a `.env` file
```bash
DATABASE_URL=postgresql+asyncpg://postgres:password@db:5432/postgres
POSTGRES_USER=postgres
POSTGRES_PASSWORD=password
POSTGRES_DB=postgres
```

### 3. Build and start containers
```bash
docker-compose up --build -d
```

### 4. Access the services

| Service       | External Port |
| ------------- | ------------- |
| FastAPI       | 8005          |
| Adminer UI    | 8088          |
| db (postgres) | 5432          |
---

## 🧠 Development Notes

- API auto-reloads on code changes (`--reload` flag).  
- Database connection handled in `app/core/database.py`.  
- Routers define REST endpoints and depend on services for logic.  
- Add your models in `models.py` and register new routers in `main.py`.

---

## 🧩 Useful Commands

Rebuild containers:
```bash
docker-compose up --build -d
```

Check logs:
```bash
docker-compose logs -f api
```

Stop all services:
```bash
docker-compose down
```

Access PostgreSQL shell:
```bash
docker exec -it postgres_db psql -U postgres
```

---

## 🧩 Future Enhancements

- Add async Celery tasks for heavy ETL jobs  
- Integrate Redis or RabbitMQ for background processing  
- Add unit tests and CI/CD pipeline  
- Extend ETL logic to handle multiple Excel sheets

---

## 📄 License

This project is licensed under the [MIT License](LICENSE).

---

### 🧑‍💻 Author
Developed by **Sajjad Alizadeh Fard**  
📧 Contact: (https://github.com/Alizadeh275/)
