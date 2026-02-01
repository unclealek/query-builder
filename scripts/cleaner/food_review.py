# scripts/cleaners/food_review.py
def clean_food_review_row(row):
    """Food review specific cleaning."""
    cleaned = {}

    for key, value in row.items():
        # Universal
        if value in ['', '""']:
            cleaned[key] = None
        elif isinstance(value, str) and value.startswith('"') and value.endswith('"'):
            cleaned[key] = value[1:-1].strip()
        else:
            cleaned[key] = value

        # Food review specific
        if key == "score" and cleaned[key] is not None:
            try:
                cleaned[key] = float(cleaned[key])
            except (ValueError, TypeError):
                cleaned[key] = None
        elif key == "date" and cleaned[key]:
            # Ensure proper timestamp format
            cleaned[key] = cleaned[key].replace('T', ' ')

    return cleaned
