"""
Test script — ส่ง Discord alert ทดสอบโดยไม่ต้องรอตลาด
รันใน container: docker exec -it realtime_consumer python test_alert.py
"""
from alert_notifier import send_discord_alert, send_critical_pipeline_alert

print("Test 1: Price alert (alert_spike)")
result = send_discord_alert(
    symbol="TEST",
    alert_type="alert_spike",
    price=123.45,
    impact_pct=7.5,
    sector="Energy",
    volume=500000,
    baseline_volume=100000,
    war_impact="positive",
)
print(f"  → Result: {result}")

print("\nTest 2: Extreme down alert")
result = send_discord_alert(
    symbol="TEST2",
    alert_type="alert_extreme_down",
    price=50.00,
    impact_pct=-12.3,
    sector="Technology",
    war_impact="negative",
)
print(f"  → Result: {result}")

print("\nTest 3: Pipeline failure alert")
result = send_critical_pipeline_alert(
    message="Test failure — silver_layer transform failed\nError: DQ CRITICAL - 5% nulls in close",
    stage="test_pipeline",
)
print(f"  → Result: {result}")

print("\n✅ Done — เช็ค Discord channel")
