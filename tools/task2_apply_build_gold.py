"""Task 2: ghi build_gold vao templates/student_impl.py (chi thay dong TODO 1).
Chay tu thu muc goc du an: python tools/task2_apply_build_gold.py
An toan khi chay lai (da co thi in SKIP)."""
from pathlib import Path
p = Path('templates/student_impl.py')
s = p.read_text(encoding='utf-8')
old = "    raise NotImplementedError('TODO 1: build_gold')"
if old not in s:
    print('SKIP: build_gold đã được hiện thực (không còn NotImplementedError)')
    raise SystemExit
new = '''    from pyspark.sql import functions as F

    dec2 = 'decimal(18,2)'
    dec6 = 'decimal(18,6)'
    late_flag = F.when(F.col('is_late'), F.lit(1)).otherwise(F.lit(0))
    agg = (silver_df
           .groupBy('site', 'event_date')
           .agg(F.count(F.lit(1)).cast('bigint').alias('n_observations'),
                F.sum('temperature_c').cast(dec2).alias('sum_temperature_c'),
                F.min('temperature_c').cast(dec2).alias('min_temperature_c'),
                F.max('temperature_c').cast(dec2).alias('max_temperature_c'),
                F.sum(late_flag).cast('bigint').alias('n_late'),
                F.sum(F.when(F.col('temperature_c') > F.lit(25), F.lit(1)).otherwise(F.lit(0)))
                 .cast('bigint').alias('n_above_25'),
                F.countDistinct('sensor_id').cast('bigint').alias('n_sensors')))
    # mean/rate lấy từ thành phần cộng được (SUM / COUNT), không dùng AVG.
    return agg.select(
        'site', 'event_date', 'n_observations', 'sum_temperature_c',
        (F.col('sum_temperature_c') / F.col('n_observations')).cast(dec6).alias('mean_temperature_c'),
        'min_temperature_c', 'max_temperature_c', 'n_late',
        (F.col('n_late') / F.col('n_observations')).cast(dec6).alias('late_rate'),
        'n_above_25', 'n_sensors')'''
p.write_text(s.replace(old, new, 1), encoding='utf-8')
print('PATCHED: build_gold đã được ghi vào templates/student_impl.py')
