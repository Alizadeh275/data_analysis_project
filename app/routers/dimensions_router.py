from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from typing import List, Dict
from app.core.database import get_async_session
from app.models import DimLocation, DimDate, DimProjectType, DimStatus

router = APIRouter( tags=["Dimensions"])

# ------------------ DimLocation ------------------
@router.get("/locations", response_model=List[Dict])
async def get_locations(db: AsyncSession = Depends(get_async_session)):
    """
    Returns all locations for filter dropdown.
    """
    result = await db.execute(select(DimLocation.id, DimLocation.city_name))
    rows = result.all()
    return [{"id": r.id, "name": r.city_name} for r in rows]


# ------------------ DimProjectType ------------------
@router.get("/project-types", response_model=List[Dict])
async def get_project_types(db: AsyncSession = Depends(get_async_session)):
    """
    Returns all project types for filter dropdown.
    """
    result = await db.execute(select(DimProjectType.id, DimProjectType.name))
    rows = result.all()
    return [{"id": r.id, "name": r.name} for r in rows]


# ------------------ DimStatus ------------------
@router.get("/statuses", response_model=List[Dict])
async def get_statuses(db: AsyncSession = Depends(get_async_session)):
    """
    Returns all statuses for filter dropdown.
    """
    result = await db.execute(select(DimStatus.id, DimStatus.name))
    rows = result.all()
    return [{"id": r.id, "name": r.name} for r in rows]


# ------------------ DimDate ------------------
@router.get("/years", response_model=List[int])
async def get_years(db: AsyncSession = Depends(get_async_session)):
    """
    Returns distinct years for filter dropdown.
    """
    result = await db.execute(select(DimDate.year).distinct().order_by(DimDate.year))
    rows = result.scalars().all()
    return rows

@router.get("/months", response_model=List[int])
async def get_months(db: AsyncSession = Depends(get_async_session)):
    """
    Returns distinct months for filter dropdown.
    """
    result = await db.execute(select(DimDate.month).distinct().order_by(DimDate.month))
    rows = result.scalars().all()
    return rows
