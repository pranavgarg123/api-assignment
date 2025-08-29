from sqlalchemy import Column, Integer, String, Float, ForeignKey, Text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship

Base = declarative_base()


class Provider(Base):
    """Provider entity representing healthcare providers"""
    __tablename__ = "providers"
    
    provider_id = Column(String, primary_key=True, index=True)
    provider_name = Column(String, nullable=False)
    provider_city = Column(String, nullable=False)
    provider_state = Column(String, nullable=False)
    provider_zip_code = Column(String, nullable=False)
    
    # Relationships
    provider_procedures = relationship("ProviderProcedure", back_populates="provider")
    ratings = relationship("Rating", back_populates="provider")


class Procedure(Base):
    """Procedure entity representing medical procedures"""
    __tablename__ = "procedures"
    
    id = Column(Integer, primary_key=True, autoincrement=True, index=True)
    ms_drg_code = Column(String, nullable=False, index=True)
    ms_drg_description = Column(Text, nullable=False)
    
    # Relationships
    provider_procedures = relationship("ProviderProcedure", back_populates="procedure")


class ProviderProcedure(Base):
    """Junction table linking providers and procedures with discharge and payment data"""
    __tablename__ = "provider_procedures"
    
    id = Column(Integer, primary_key=True, autoincrement=True, index=True)
    provider_id = Column(String, ForeignKey("providers.provider_id"), nullable=False, index=True)
    procedure_id = Column(Integer, ForeignKey("procedures.id"), nullable=False, index=True)
    total_discharges = Column(Integer, nullable=False)
    average_covered_charges = Column(Float, nullable=False)
    average_total_payments = Column(Float, nullable=False)
    average_medicare_payments = Column(Float, nullable=False)
    
    # Relationships
    provider = relationship("Provider", back_populates="provider_procedures")
    procedure = relationship("Procedure", back_populates="provider_procedures")


class Rating(Base):
    """Rating entity for provider ratings"""
    __tablename__ = "ratings"
    
    id = Column(Integer, primary_key=True, autoincrement=True, index=True)
    provider_id = Column(String, ForeignKey("providers.provider_id"), nullable=False, index=True)
    rating = Column(Integer, nullable=False)  # 1-10 scale
    
    # Relationships
    provider = relationship("Provider", back_populates="ratings")
