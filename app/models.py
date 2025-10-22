from sqlalchemy import Column, Integer, String, ForeignKey
from sqlalchemy.orm import relationship
from app.core.database import Base

class DimLocation(Base):
    __tablename__ = "dim_location"
    id = Column(Integer, primary_key=True)
    city_name = Column(String)
    department_code = Column(String)
    work_orders = relationship("FactWorkOrder", back_populates="location")

class DimDate(Base):
    __tablename__ = "dim_date"
    id = Column(Integer, primary_key=True)
    year = Column(Integer)
    month = Column(Integer)
    work_orders = relationship("FactWorkOrder", back_populates="date")

class DimProjectType(Base):
    __tablename__ = "dim_project_type"
    id = Column(Integer, primary_key=True)
    name = Column(String)
    work_orders = relationship("FactWorkOrder", back_populates="project_type")

class DimStatus(Base):
    __tablename__ = "dim_status"
    id = Column(Integer, primary_key=True)
    name = Column(String, unique=True)  # "در دست اجرا", "نزد مالی", etc.
    work_orders = relationship("FactWorkOrder", back_populates="status_obj")

class FactWorkOrder(Base):
    __tablename__ = "fact_work_order"
    id = Column(Integer, primary_key=True)
    location_id = Column(Integer, ForeignKey("dim_location.id"))
    date_id = Column(Integer, ForeignKey("dim_date.id"))
    project_type_id = Column(Integer, ForeignKey("dim_project_type.id"))
    status_id = Column(Integer, ForeignKey("dim_status.id")) 
    count = Column(Integer)

    location = relationship("DimLocation", back_populates="work_orders")
    date = relationship("DimDate", back_populates="work_orders")
    project_type = relationship("DimProjectType", back_populates="work_orders")
    status_obj = relationship("DimStatus", back_populates="work_orders") 
