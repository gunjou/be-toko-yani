from datetime import date, datetime

def serialize_datetime(obj):
    """
    Mengonversi semua objek datetime/date di dalam list atau dict 
    menjadi string format ISO agar bisa di-serialize ke JSON.
    """
    if isinstance(obj, list):
        return [serialize_datetime(item) for item in obj]
    elif isinstance(obj, dict):
        return {k: serialize_datetime(v) for k, v in obj.items()}
    elif isinstance(obj, (datetime, date)):
        return obj.isoformat()
    return obj
