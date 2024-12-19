from sqlalchemy import Column, Integer, String, Float, Date, ForeignKey
from sqlalchemy.orm import relationship

from base.base import Base


class Event(Base):
    __tablename__ = 'events'
    'iyear', 'imonth', 'iday', 'latitude', 'longitude', 'summary', 'nperps'

    event_id = Column(Integer, primary_key=True)
    year = Column(Integer)
    month = Column(Integer)
    day = Column(Integer)
    latitude = Column(Float)
    longitude = Column(Float)
    summary = Column(String)
    team = Column(Integer)


class TerroristGroup(Base):
    __tablename__ = 'terrorist_groups'
    group_id = Column(Integer, primary_key=True)
    group_name = Column(String, nullable=False)
    country = Column(String)
    region = Column(String)
    attack_type = Column(String)

class GroupTargetRelation(Base):
    __tablename__ = 'group_target_relations'
    id = Column(Integer, primary_key=True)
    group_id = Column(Integer, ForeignKey('terrorist_groups.group_id'))
    target_id = Column(Integer)
    target_type = Column(String)
    year = Column(Integer)
    attack_type = Column(String)
    count = Column(Integer)
    group = relationship("TerroristGroup")

class AttackFrequencyByRegion(Base):
    __tablename__ = 'attack_frequency_by_region'
    id = Column(Integer, primary_key=True)
    region = Column(String)
    year = Column(Integer)
    attack_count = Column(Integer)
    month = Column(Integer)
    attack_type = Column(String)

class GeospatialData(Base):
    __tablename__ = 'geospatial_data'
    eventid = Column(Integer, ForeignKey('events.eventid'), primary_key=True)
    latitude = Column(Float)
    longitude = Column(Float)
    date = Column(Date)
    attack_type = Column(String)
    group_name = Column(String)
    region = Column(String)
    city = Column(String)
    event = relationship("Event")

class TerroristGroupExpansion(Base):
    __tablename__ = 'terrorist_group_expansion'
    id = Column(Integer, primary_key=True)
    group_id = Column(Integer, ForeignKey('terrorist_groups.group_id'))
    group_name = Column(String)
    year = Column(Integer)
    new_region = Column(String)
    region_entered = Column(String)
    group = relationship("TerroristGroup")

class Correlation(Base):
    __tablename__ = 'correlations'
    correlation_id = Column(Integer, primary_key=True)
    variable_1 = Column(String)
    variable_2 = Column(String)
    correlation_value = Column(Float)

class TerroristGroupLocation(Base):
    __tablename__ = 'terrorist_group_locations'
    id = Column(Integer, primary_key=True)
    group_id = Column(Integer, ForeignKey('terrorist_groups.group_id'))
    latitude = Column(Float)
    longitude = Column(Float)
    year = Column(Integer)
    month = Column(Integer)
    attack_type = Column(String)
    group = relationship("TerroristGroup")



print("Tables created successfully!")
