"""Parser for SAS label files."""

import re
from pathlib import Path
from typing import Dict, List, Tuple


def parse_sas_labels(sas_file_path: Path) -> Tuple[Dict[str, str], Dict[str, Dict[str, str]]]:
    """Parse SAS file to extract variable labels and value formats.
    
    Args:
        sas_file_path: Path to SAS labels file
        
    Returns:
        Tuple of (variable_labels, value_formats)
        - variable_labels: Dict mapping variable names to descriptions
        - value_formats: Dict mapping variable names to value->label mappings
    """
    with open(sas_file_path, 'r', encoding='latin-1') as f:
        content = f.read()
    
    # Extract variable labels (e.g., AGE = 'Age at Diagnosis')
    variable_labels = {}
    label_pattern = r"(\w+)\s*=\s*'([^']+)'"
    
    # Find the label section
    label_section = re.search(r'label\s+(.*?);', content, re.DOTALL | re.IGNORECASE)
    if label_section:
        for match in re.finditer(label_pattern, label_section.group(1)):
            var_name = match.group(1)
            var_label = match.group(2)
            variable_labels[var_name] = var_label
    
    # Extract value formats (simplified - would need more robust parsing)
    value_formats = {}
    
    # Look for proc format value statements
    format_blocks = re.findall(r'value\s+(\w+)(.*?);\s*(?=value|\s*run|$)', content, re.DOTALL | re.IGNORECASE)
    
    for format_name, format_content in format_blocks:
        # Extract value mappings
        value_map = {}
        
        # Pattern for numeric or string values
        value_pattern = r"(['\"]?)([^'\"=]+)\1\s*=\s*['\"]([^'\"]+)['\"]"
        
        for match in re.finditer(value_pattern, format_content):
            value = match.group(2).strip()
            label = match.group(3).strip()
            value_map[value] = label
        
        if value_map:
            # Try to find which variables use this format
            # Look for format statements
            format_usage = re.findall(rf'(\w+)\s+{format_name}\.', content)
            for var in format_usage:
                value_formats[var] = value_map
    
    return variable_labels, value_formats


def parse_column_positions(sas_file_path: Path) -> List[Dict[str, any]]:
    """Parse column positions from SAS input statement.
    
    Args:
        sas_file_path: Path to SAS file
        
    Returns:
        List of column definitions with name, start, end positions
    """
    with open(sas_file_path, 'r', encoding='latin-1') as f:
        content = f.read()
    
    columns = []
    
    # Find input statement
    input_section = re.search(r'input\s+(.*?);\s*', content, re.DOTALL | re.IGNORECASE)
    if not input_section:
        return columns
    
    input_text = input_section.group(1)
    
    # Parse column definitions
    # Handle both formats: "AGE $ 50-52" and "@50 AGE $3."
    
    # First try the position-name format: @50 AGE $3.
    at_pattern = r'@(\d+)\s+(\w+)\s+\$?(\d+)\.'
    for match in re.finditer(at_pattern, input_text):
        start_pos = int(match.group(1)) - 1  # Convert to 0-based
        name = match.group(2)
        width = int(match.group(3))
        
        columns.append({
            'name': name,
            'start': start_pos,
            'end': start_pos + width,
            'width': width
        })
    
    # If no columns found, try the range format: AGE $ 50-52
    if not columns:
        range_pattern = r'(\w+)\s*\$?\s*(\d+)-(\d+)'
        for match in re.finditer(range_pattern, input_text):
            name = match.group(1)
            start = int(match.group(2)) - 1  # Convert to 0-based
            end = int(match.group(3))
            
            columns.append({
                'name': name,
                'start': start,
                'end': end,
                'width': end - start
            })
    
    return columns