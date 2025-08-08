"""
Database models for Lead Analysis with transcription caching
"""

from sqlalchemy import create_engine, Column, Integer, String, DateTime, Float, Text, Boolean, JSON, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship
from datetime import datetime
from typing import Optional, Dict, Any
import os

Base = declarative_base()


class Lead(Base):
    """Lead model with analysis history"""
    __tablename__ = 'leads'

    id = Column(String(50), primary_key=True)
    title = Column(String(500))
    status_id = Column(String(50))
    junk_status = Column(Integer)
    junk_status_name = Column(String(200))
    date_create = Column(DateTime)
    last_analyzed = Column(DateTime)
    analysis_count = Column(Integer, default=0)

    # Contact information
    phone = Column(String(50))
    email = Column(String(255))
    name = Column(String(255))

    # Cached analysis results
    last_analysis_result = Column(String(50))  # KEEP, CHANGE, SKIP
    last_analysis_reason = Column(String(100))
    unsuccessful_calls_count = Column(Integer, default=0)

    # Raw data from Bitrix
    raw_data = Column(JSON)

    # Timestamps
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    # Relationships
    transcriptions = relationship("Transcription", back_populates="lead", cascade="all, delete-orphan")
    analysis_history = relationship("AnalysisHistory", back_populates="lead", cascade="all, delete-orphan")


class Transcription(Base):
    """Transcription model for caching audio analysis"""
    __tablename__ = 'transcriptions'

    id = Column(Integer, primary_key=True)
    lead_id = Column(String(50), ForeignKey('leads.id'), nullable=False)
    audio_url = Column(Text, nullable=False)
    audio_hash = Column(String(64), unique=True, nullable=False)  # SHA256 hash of audio URL

    # Transcription results
    transcription_text = Column(Text)
    confidence = Column(Float)
    duration = Column(Float)
    language = Column(String(10), default='uz')

    # Processing status
    is_successful = Column(Boolean, default=False)
    error_message = Column(Text)
    processing_time = Column(Float)

    # Service information
    transcription_service = Column(String(100), default='docker_service')
    service_version = Column(String(50))

    # Timestamps
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    # Relationships
    lead = relationship("Lead", back_populates="transcriptions")


class AnalysisHistory(Base):
    """Analysis history for leads"""
    __tablename__ = 'analysis_history'

    id = Column(Integer, primary_key=True)
    lead_id = Column(String(50), ForeignKey('leads.id'), nullable=False)

    # Analysis input
    original_status = Column(String(50))
    original_junk_status = Column(Integer)

    # Analysis results
    action = Column(String(50))  # KEEP_STATUS, CHANGE_STATUS, SKIP
    reason = Column(String(100))
    new_status = Column(String(50))
    new_junk_status = Column(Integer)

    # AI Analysis results
    ai_suitable = Column(Boolean)
    ai_confidence = Column(Float)
    ai_reasoning = Column(Text)
    ai_alternative_status = Column(Integer)
    ai_processing_time = Column(Float)
    ai_model_used = Column(String(100))

    # Processing details
    unsuccessful_calls_count = Column(Integer, default=0)
    transcription_success_rate = Column(Float)
    total_processing_time = Column(Float)

    # Status
    is_successful = Column(Boolean, default=False)
    requires_update = Column(Boolean, default=False)
    error_message = Column(Text)
    dry_run = Column(Boolean, default=False)

    # Timestamps
    analysis_date = Column(DateTime, default=datetime.utcnow)
    created_at = Column(DateTime, default=datetime.utcnow)

    # Relationships
    lead = relationship("Lead", back_populates="analysis_history")


class SystemConfig(Base):
    """System configuration and state"""
    __tablename__ = 'system_config'

    id = Column(Integer, primary_key=True)
    key = Column(String(100), unique=True, nullable=False)
    value = Column(Text)
    description = Column(Text)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class SchedulerState(Base):
    """Scheduler state tracking"""
    __tablename__ = 'scheduler_state'

    id = Column(Integer, primary_key=True)
    last_analysis_time = Column(DateTime, nullable=False)
    leads_processed = Column(Integer, default=0)
    leads_updated = Column(Integer, default=0)
    success_rate = Column(Float, default=0.0)
    processing_time = Column(Float, default=0.0)

    # Status
    status = Column(String(50), default='idle')  # idle, running, completed, failed
    error_message = Column(Text)

    # Timestamps
    started_at = Column(DateTime)
    completed_at = Column(DateTime)
    created_at = Column(DateTime, default=datetime.utcnow)


class DatabaseManager:
    """Database manager for lead analysis system"""

    def __init__(self, database_url: Optional[str] = None):
        if database_url is None:
            # Default to SQLite in data directory
            os.makedirs('data', exist_ok=True)
            database_url = 'sqlite:///data/lead_analysis.db'

        self.engine = create_engine(database_url, echo=False)
        self.SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=self.engine)

        # Create tables
        Base.metadata.create_all(bind=self.engine)

    def get_session(self):
        """Get database session"""
        return self.SessionLocal()

    def init_system_config(self):
        """Initialize system configuration"""
        with self.get_session() as session:
            # Check if config exists
            config_keys = [
                ('last_analysis_time', str(datetime.utcnow().isoformat()), 'Last time leads were analyzed'),
                ('total_leads_processed', '0', 'Total number of leads processed'),
                ('total_transcriptions_cached', '0', 'Total number of transcriptions cached'),
                ('system_version', '1.0.0', 'System version'),
            ]

            for key, default_value, description in config_keys:
                existing = session.query(SystemConfig).filter(SystemConfig.key == key).first()
                if not existing:
                    config = SystemConfig(key=key, value=default_value, description=description)
                    session.add(config)

            session.commit()

    def get_config_value(self, key: str, default: str = None) -> Optional[str]:
        """Get configuration value"""
        with self.get_session() as session:
            config = session.query(SystemConfig).filter(SystemConfig.key == key).first()
            return config.value if config else default

    def set_config_value(self, key: str, value: str, description: str = None):
        """Set configuration value"""
        with self.get_session() as session:
            config = session.query(SystemConfig).filter(SystemConfig.key == key).first()
            if config:
                config.value = value
                config.updated_at = datetime.utcnow()
                if description:
                    config.description = description
            else:
                config = SystemConfig(key=key, value=value, description=description)
                session.add(config)
            session.commit()

    def close(self):
        """Close database connections"""
        self.engine.dispose()


# Initialize database manager
db_manager = DatabaseManager()


def get_db():
    """Dependency to get database session"""
    db = db_manager.get_session()
    try:
        yield db
    finally:
        db.close()