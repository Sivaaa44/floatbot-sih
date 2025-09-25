from typing import Optional, List
from models import QueryResult, ChartConfig

class ChartAnalyzer:
    def suggest_chart(self, question: str, query_result: QueryResult) -> Optional[ChartConfig]:
        if not query_result.success or not query_result.data:
            return None
        
        headers = query_result.headers
        question_lower = question.lower()
        
        if 'PRES' in headers and 'TEMP' in headers:
            if 'profile' in question_lower or 'depth' in question_lower:
                return ChartConfig(
                    chart_type='line',
                    x_axis='TEMP',
                    y_axis='PRES',
                    y_reversed=True,
                    title='Temperature Profile',
                    x_label='Temperature (°C)',
                    y_label='Pressure (dbar)'
                )
        
        if 'TEMP' in headers and 'PSAL' in headers:
            if 't-s' in question_lower or 'water mass' in question_lower:
                return ChartConfig(
                    chart_type='scatter',
                    x_axis='PSAL',
                    y_axis='TEMP',
                    title='Temperature-Salinity Diagram',
                    x_label='Practical Salinity',
                    y_label='Temperature (°C)'
                )
        
        lat_col = self._find_column_containing(headers, 'LATITUDE')
        lon_col = self._find_column_containing(headers, 'LONGITUDE')
        
        if lat_col and lon_col:
            if 'location' in question_lower or 'map' in question_lower or 'geographic' in question_lower:
                return ChartConfig(
                    chart_type='scatter_geo',
                    lat_column=lat_col,
                    lon_column=lon_col,
                    title='Float Locations'
                )
        
        numeric_cols = self._get_numeric_columns(query_result)
        if len(numeric_cols) >= 2:
            return ChartConfig(
                chart_type='scatter',
                x_axis=numeric_cols[0],
                y_axis=numeric_cols[1],
                title=f'{numeric_cols[1]} vs {numeric_cols[0]}'
            )
        
        return None
    
    def _find_column_containing(self, headers: List[str], text: str) -> Optional[str]:
        for header in headers:
            if text in header:
                return header
        return None
    
    def _get_numeric_columns(self, query_result: QueryResult) -> List[str]:
        numeric_cols = []
        for i, header in enumerate(query_result.headers):
            sample_values = [row[i] for row in query_result.data[:10] if row[i] is not None]
            if sample_values:
                try:
                    [float(val) for val in sample_values[:3]]
                    numeric_cols.append(header)
                except (ValueError, TypeError):
                    continue
        return numeric_cols