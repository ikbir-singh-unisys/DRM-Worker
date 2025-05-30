# worker/core/models.py
from sqlalchemy import Column, Integer, String
from core.database import Base

class S3Credential(Base):
    __tablename__ = "s3_credentials"

    id = Column(Integer, primary_key=True, index=True)
    access_key = Column(String, nullable=False)
    secret_key = Column(String, nullable=False)
    region = Column(String, default="ap-south-1")


class AudioTrack(Base):
    __tablename__ = "job_audio_tracks"
    id = Column(Integer, primary_key=True, index=True)
    job_id = Column(Integer, nullable=False)
    language = Column(String, nullable=False)
    file_path = Column(String, nullable=False)


class SubtitleTrack(Base):
    __tablename__ = "job_subtitle_tracks"
    id = Column(Integer, primary_key=True, index=True)
    job_id = Column(Integer, nullable=False)
    language = Column(String, nullable=False)
    file_path = Column(String, nullable=False)
