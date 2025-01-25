import pandas as pd
import re
from typing import Dict, Union

def expand_product_fields(data):
    # Call the extract_product_fields function for each row

    result = extract_product_fields(data)
    # Convert the dictionary result into a DataFrame (one row with multiple columns)
    return pd.DataFrame([result])

# Pre-compile all regular expressions
DUZIA_PATTERNS = [
    (re.compile(r'uma\s+d[uú]zia'), 1),
    (re.compile(r'meia\s+d[uú]zia'), 0.5),
    (re.compile(r'1\s+d[uú]zia'), 1),
]

WEIGHT_PATTERN = re.compile(r'(\d+(?:[.,]\d+)?)\s*(?:\(\d+(?:[.,]\d+)?\))?\s*(kg|g|ml|l|cl|mg|dl)')
NX_PATTERN = re.compile(r'(\d+)x\d+')
PLUS_PATTERN = re.compile(r'(\d+)\s*\+\s*\d+')

# Define constants
QTY_UNITS_MAP = {
    variant: normalized
    for normalized, variants in {
        'duzia': ['duzia', 'dúzia', 'dúzias', 'duzias'],
        'un': ['un', 'uni', 'unidade', 'unidades'],
        'saquetas': ['saqueta', 'saquetas'],
        'rolos': ['rolo', 'rolos'],
        'doses': ['dose', 'doses', 'd'],
        'lata': ['lata', 'latas'],
        'caixas': ['caixa', 'caixas'],
        'pastilhas': ['pastilha', 'pastilhas'],
        'par': ['par', 'pares'],
    }.items()
    for variant in variants
}

# Create quantity pattern once
ALL_QTY_UNITS = '|'.join(QTY_UNITS_MAP.keys())
QTY_PATTERN = re.compile(fr'(\d+)\s*(?:{ALL_QTY_UNITS})')

def extract_product_fields(text: str) -> Dict[str, Union[int, float, str, None]]:
    """
    Extract quantity, quantity unit, weight, weight unit and cleaned name from product description.
    Optimized version with pre-compiled patterns and early returns.
    
    Args:
        text (str): Product description text
        
    Returns:
        dict: Dictionary containing extracted fields
    """
    # Initialize with default values
    result = {
        'quantity': 1,
        'qty_unit': 'un',
        'weight': None,
        'weight_unit': None,
        'product_name': ''
    }
    
    if not text:
        return result
    
    text = str(text).lower().strip()
    result['product_name'] = text
    
    # Check duzia patterns first (early return)
    for pattern, value in DUZIA_PATTERNS:
        if pattern.search(text):
            result['quantity'] = value
            result['qty_unit'] = 'duzia'
            return result
    
    # Extract weight
    if weight_match := WEIGHT_PATTERN.search(text):
        result['weight'] = int(float(weight_match.group(1).replace(',', '.')))
        result['weight_unit'] = weight_match.group(2).lower()
    
    # Extract quantity - check NxM pattern first
    if nx_match := NX_PATTERN.search(text):
        result['quantity'] = int(nx_match.group(1))
        return result
    
    # Check plus pattern
    if plus_match := PLUS_PATTERN.search(text):
        result['quantity'] = int(plus_match.group(1))
        return result
    
    # Check other quantity patterns
    if qty_match := QTY_PATTERN.search(text):
        result['quantity'] = int(qty_match.group(1))
        # Find the matching unit
        matched_text = text[qty_match.start():qty_match.end()]
        for unit_variant in QTY_UNITS_MAP:
            if unit_variant in matched_text:
                result['qty_unit'] = QTY_UNITS_MAP[unit_variant]
                break
    
    return result