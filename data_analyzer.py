from typing import List, Dict, Optional, Tuple
from models import QueryResult, AnalysisResult

class DataAnalyzer:
    def analyze_data(self, question: str, query_result: QueryResult) -> AnalysisResult:
        if not query_result.success or not query_result.data:
            return AnalysisResult(0, 0, 0, key_insights=["No data available for analysis"])
        
        data = query_result.data
        headers = query_result.headers
        col_idx = {col: i for i, col in enumerate(headers)}
        
        record_count = len(data)
        float_count = self._count_unique_values(data, col_idx, 'FLOAT_ID')
        profile_count = self._count_unique_profile_pairs(data, col_idx)
        
        depth_range = self._extract_range(data, col_idx, 'PRES')
        temp_range = self._extract_range(data, col_idx, 'TEMP')
        quality_stats = self._analyze_quality_flags(data, col_idx)
        geographic_bounds = self._extract_geographic_bounds(data, col_idx)
        
        insights = self._generate_insights(question, data, col_idx)
        
        return AnalysisResult(
            record_count=record_count,
            float_count=float_count,
            profile_count=profile_count,
            depth_range=depth_range,
            temp_range=temp_range,
            quality_stats=quality_stats,
            geographic_bounds=geographic_bounds,
            key_insights=insights
        )
    
    def _count_unique_values(self, data: List[Tuple], col_idx: Dict, column: str) -> int:
        if column not in col_idx:
            return 0
        return len({row[col_idx[column]] for row in data if row[col_idx[column]] is not None})
    
    def _count_unique_profile_pairs(self, data: List[Tuple], col_idx: Dict) -> int:
        if 'FLOAT_ID' not in col_idx or 'PROFILE_NUMBER' not in col_idx:
            return 0
        return len({(row[col_idx['FLOAT_ID']], row[col_idx['PROFILE_NUMBER']]) 
                   for row in data 
                   if row[col_idx['FLOAT_ID']] is not None and row[col_idx['PROFILE_NUMBER']] is not None})
    
    def _extract_range(self, data: List[Tuple], col_idx: Dict, column: str) -> Optional[Tuple[float, float]]:
        if column not in col_idx:
            return None
        
        values = []
        for row in data:
            val = row[col_idx[column]]
            if val is not None:
                try:
                    values.append(float(val))
                except (ValueError, TypeError):
                    continue
        return (min(values), max(values)) if values else None
    
    def _analyze_quality_flags(self, data: List[Tuple], col_idx: Dict) -> Dict[str, float]:
        qc_columns = [col for col in col_idx.keys() if col.endswith('_QC')]
        if not qc_columns:
            return {}
        
        stats = {}
        for qc_col in qc_columns:
            param = qc_col.replace('_QC', '')
            good_count = sum(1 for row in data if row[col_idx[qc_col]] in ['1', '2'])
            total_count = len([row for row in data if row[col_idx[qc_col]] is not None])
            if total_count > 0:
                stats[param] = (good_count / total_count) * 100
        return stats
    
    def _extract_geographic_bounds(self, data: List[Tuple], col_idx: Dict) -> Optional[Dict[str, float]]:
        lat_cols = [col for col in col_idx.keys() if 'LATITUDE' in col]
        lon_cols = [col for col in col_idx.keys() if 'LONGITUDE' in col]
        
        if not lat_cols or not lon_cols:
            return None
        
        lat_col = lat_cols[0]
        lon_col = lon_cols[0]
        
        lats = []
        lons = []
        for row in data:
            lat_val = row[col_idx[lat_col]]
            lon_val = row[col_idx[lon_col]]
            if lat_val is not None and lon_val is not None:
                try:
                    lats.append(float(lat_val))
                    lons.append(float(lon_val))
                except (ValueError, TypeError):
                    continue
        
        if lats and lons:
            return {
                'lat_min': min(lats), 'lat_max': max(lats),
                'lon_min': min(lons), 'lon_max': max(lons)
            }
        return None
    
    def _generate_insights(self, question: str, data: List[Tuple], col_idx: Dict) -> List[str]:
        insights = []
        question_lower = question.lower()
        
        if 'TEMP' in col_idx and 'PRES' in col_idx and ('profile' in question_lower or 'temperature' in question_lower):
            temp_data = []
            for row in data:
                temp_val = row[col_idx['TEMP']]
                pres_val = row[col_idx['PRES']]
                if temp_val is not None and pres_val is not None:
                    try:
                        temp_data.append((float(pres_val), float(temp_val)))
                    except (ValueError, TypeError):
                        continue
            
            if temp_data:
                temp_data.sort()
                surface_temp = temp_data[0][1]
                deep_temp = temp_data[-1][1]
                temp_drop = surface_temp - deep_temp
                insights.append(f"Temperature drops {temp_drop:.1f}°C from surface ({surface_temp:.1f}°C) to depth ({deep_temp:.1f}°C)")
        
        if any(col.endswith('_QC') for col in col_idx.keys()):
            qc_stats = self._analyze_quality_flags(data, col_idx)
            good_quality_params = [param for param, pct in qc_stats.items() if pct > 95]
            if good_quality_params:
                insights.append(f"High data quality (>95%) for: {', '.join(good_quality_params)}")
        
        return insights