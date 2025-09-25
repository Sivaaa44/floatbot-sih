from dataclasses import dataclass
from typing import List, Dict, Optional, Tuple, Any

@dataclass
class QueryResult:
    sql: str
    data: List[Tuple]
    headers: List[str]
    success: bool
    error: Optional[str] = None

@dataclass
class AnalysisResult:
    record_count: int
    float_count: int
    profile_count: int
    depth_range: Optional[Tuple[float, float]] = None
    temp_range: Optional[Tuple[float, float]] = None
    quality_stats: Optional[Dict[str, float]] = None
    geographic_bounds: Optional[Dict[str, float]] = None
    key_insights: List[str] = None

@dataclass
class ChartConfig:
    chart_type: str
    x_axis: Optional[str] = None
    y_axis: Optional[str] = None
    title: str = "Chart"
    x_label: Optional[str] = None
    y_label: Optional[str] = None
    y_reversed: bool = False
    lat_column: Optional[str] = None
    lon_column: Optional[str] = None

@dataclass
class OceanographicResponse:
    question: str
    sql: str
    results: List[Tuple]
    headers: List[str]
    analysis: str
    chart_config: Optional[ChartConfig]
    success: bool
    error: Optional[str] = None