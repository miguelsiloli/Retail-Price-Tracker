def convert_volume_to_liters(weight, weight_unit):
    """
    Convert volume to liters.
    Supported units: 'cl', 'ml', 'L'.
    """
    conversion_factors = {
        'cl': 0.01,  # centiliters to liters
        'ml': 0.001, # milliliters to liters
        'L': 1.0     # liters remain unchanged
    }
    if weight_unit in conversion_factors:
        return weight * conversion_factors[weight_unit], 'L'
    raise ValueError(f"Unsupported volume unit: {weight_unit}")

def convert_mass_to_kg(weight, weight_unit):
    """
    Convert mass to kilograms.
    Supported units: 'g', 'mg', 'kg'.
    """
    conversion_factors = {
        'g': 0.001,  # grams to kilograms
        'mg': 1e-3,  # milligrams to kilograms
        'kg': 1.0    # kilograms remain unchanged
    }
    if weight_unit in conversion_factors:
        return weight * conversion_factors[weight_unit], 'kg'
    raise ValueError(f"Unsupported mass unit: {weight_unit}")


def normalize_units(df):
    def normalize(row):
        weight, weight_unit = row['weight'], row['weight_unit']
        if weight_unit in ['cl', 'ml', 'L']:
            return pd.Series(convert_volume_to_liters(weight, weight_unit))
        elif weight_unit in ['g', 'mg', 'kg']:
            return pd.Series(convert_mass_to_kg(weight, weight_unit))
        return pd.Series([weight, weight_unit])  # Return unchanged if no match

    # Apply normalization and update columns
    df[['weight', 'weight_unit']] = df.apply(normalize, axis=1)
    return df

def compute_columns(df):
    df["total_weight"] = df["weight"]*df["quantity"]
    # df["price_per_weight"] = df["total_weight"]/
    # df["price_per_unit"] = df["qty_unit"]/
    return df

if __name__ == "__main__":
    import pandas as pd
    data = pd.read_csv("structured_pingo_doce.csv")
    data = normalize_units(data)
    # data.to_csv("data/snapshots/compiled_continente_data.csv", index=False)
    print(data)
