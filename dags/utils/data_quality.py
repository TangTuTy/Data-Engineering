"""
Data Quality Module
รัน DQ checks ในแต่ละ layer (Bronze/Silver/Gold)

ใช้งาน:
    from utils.data_quality import DataQualityChecker

    dq = DataQualityChecker(stage="silver_historical_daily")
    dq.check_completeness(records, required_fields=["symbol", "date", "close"])
    dq.check_uniqueness(records, key_fields=["symbol", "date"])
    dq.check_validity(records, field="close", min_value=0)
    dq.run()  # raise exception ถ้ามี CRITICAL
"""

import logging
from datetime import datetime, timezone
from collections import Counter
from pymongo import MongoClient

logger = logging.getLogger(__name__)

MONGO_URI = "mongodb://mongodb:27017/"
DB_NAME = "stock_database"
DQ_RESULTS_COLLECTION = "dq_check_results"


class DQResult:
    """Result of a single DQ check"""
    INFO = "INFO"
    WARNING = "WARNING"
    CRITICAL = "CRITICAL"

    def __init__(self, rule, severity, message, details=None):
        self.rule = rule
        self.severity = severity
        self.message = message
        self.details = details or {}
        self.timestamp = datetime.now(timezone.utc).isoformat()

    def to_dict(self):
        return {
            "rule": self.rule,
            "severity": self.severity,
            "message": self.message,
            "details": self.details,
            "timestamp": self.timestamp,
        }

    def __repr__(self):
        return f"[{self.severity}] {self.rule}: {self.message}"


class DataQualityChecker:
    """
    Quality gate ที่รวบรวม DQ check results และ:
    - Log ทุก check
    - บันทึกประวัติลง MongoDB (สำหรับ dashboard / audit)
    - Raise exception ถ้ามี CRITICAL fail
    """

    def __init__(self, stage):
        self.stage = stage
        self.results = []
        self.run_id = datetime.now(timezone.utc).isoformat()

    # ───────────────── Rule 1: Completeness (Null check) ─────────────────
    def check_completeness(self, records, required_fields, warning_threshold=0.05):
        """
        ตรวจสอบว่า fields สำคัญไม่มี null
        warning_threshold: % ที่ยอมรับได้ (default 5%)
        """
        rule = "completeness"
        if not records:
            self.results.append(DQResult(rule, DQResult.CRITICAL,
                f"No records in {self.stage}",
                {"record_count": 0}))
            return

        total = len(records)
        for field in required_fields:
            null_count = sum(1 for r in records if r.get(field) in (None, "", []))
            null_pct = null_count / total

            if null_count == 0:
                self.results.append(DQResult(rule, DQResult.INFO,
                    f"{field}: no nulls ({total} records)"))
            elif null_pct > warning_threshold:
                self.results.append(DQResult(rule, DQResult.CRITICAL,
                    f"{field}: {null_count}/{total} nulls ({null_pct:.1%}) exceeds threshold",
                    {"field": field, "null_count": null_count, "total": total}))
            else:
                self.results.append(DQResult(rule, DQResult.WARNING,
                    f"{field}: {null_count}/{total} nulls ({null_pct:.1%})",
                    {"field": field, "null_count": null_count, "total": total}))

    # ───────────────── Rule 2: Uniqueness (Duplicate check) ───────────────
    def check_uniqueness(self, records, key_fields):
        """ตรวจสอบ duplicate ตาม composite key"""
        rule = "uniqueness"
        if not records:
            return

        keys = [tuple(r.get(f) for f in key_fields) for r in records]
        counter = Counter(keys)
        duplicates = {k: c for k, c in counter.items() if c > 1}

        if not duplicates:
            self.results.append(DQResult(rule, DQResult.INFO,
                f"No duplicates on {'+'.join(key_fields)} ({len(records)} records)"))
        else:
            dup_count = sum(c - 1 for c in duplicates.values())
            example = list(duplicates.items())[:3]
            self.results.append(DQResult(rule, DQResult.CRITICAL,
                f"Found {dup_count} duplicate records on {'+'.join(key_fields)}",
                {"duplicate_groups": len(duplicates), "examples": [
                    {"key": str(k), "count": c} for k, c in example
                ]}))

    # ───────────────── Rule 3: Validity (Range check) ─────────────────────
    def check_validity(self, records, field, min_value=None, max_value=None):
        """ตรวจสอบว่า field อยู่ใน valid range"""
        rule = "validity"
        if not records:
            return

        invalid = []
        checked = 0
        for r in records:
            val = r.get(field)
            if val is None:
                continue
            checked += 1
            if min_value is not None and val < min_value:
                invalid.append({"value": val, "min": min_value})
            elif max_value is not None and val > max_value:
                invalid.append({"value": val, "max": max_value})

        if not invalid:
            self.results.append(DQResult(rule, DQResult.INFO,
                f"{field}: all {checked} values within valid range"))
        else:
            self.results.append(DQResult(rule, DQResult.WARNING,
                f"{field}: {len(invalid)}/{checked} values out of range",
                {"field": field, "invalid_count": len(invalid),
                 "examples": invalid[:3]}))

    # ───────────────── Rule 4: Freshness ──────────────────────────────────
    def check_freshness(self, records, date_field, max_age_days=7):
        """ตรวจสอบว่าข้อมูลล่าสุดไม่เก่าเกินไป"""
        rule = "freshness"
        if not records:
            return

        from datetime import timedelta
        dates = []
        for r in records:
            v = r.get(date_field)
            if isinstance(v, datetime):
                dates.append(v.replace(tzinfo=None))
            elif isinstance(v, str):
                try:
                    dates.append(datetime.fromisoformat(v.replace("Z", "")))
                except ValueError:
                    pass

        if not dates:
            self.results.append(DQResult(rule, DQResult.WARNING,
                f"Cannot determine freshness — no parseable dates in {date_field}"))
            return

        latest = max(dates)
        age_days = (datetime.now() - latest).days

        if age_days <= max_age_days:
            self.results.append(DQResult(rule, DQResult.INFO,
                f"Data is fresh (latest: {latest.date()}, {age_days} days old)"))
        else:
            self.results.append(DQResult(rule, DQResult.WARNING,
                f"Data may be stale (latest: {latest.date()}, {age_days} days old)",
                {"max_age_days": max_age_days, "actual_age_days": age_days}))

    # ───────────────── Rule 5: Referential Integrity ──────────────────────
    def check_referential_integrity(self, records, field, reference_set, sample_size=5):
        """ตรวจสอบว่า value ใน field มีอยู่ใน reference (เช่น symbol ที่ไม่อยู่ใน sp500)"""
        rule = "referential_integrity"
        if not records or not reference_set:
            return

        ref = set(reference_set)
        orphans = []
        for r in records:
            v = r.get(field)
            if v is not None and v not in ref:
                orphans.append(v)

        unique_orphans = list(set(orphans))

        if not unique_orphans:
            self.results.append(DQResult(rule, DQResult.INFO,
                f"All {field} values exist in reference ({len(ref)} ref entries)"))
        else:
            self.results.append(DQResult(rule, DQResult.WARNING,
                f"{len(unique_orphans)} {field}(s) not in reference",
                {"field": field, "orphan_count": len(unique_orphans),
                 "examples": unique_orphans[:sample_size]}))

    # ───────────────── Run + Persist ──────────────────────────────────────
    def run(self):
        """
        Log สรุปผล + บันทึกลง MongoDB + raise exception ถ้ามี CRITICAL
        """
        n_info = sum(1 for r in self.results if r.severity == DQResult.INFO)
        n_warn = sum(1 for r in self.results if r.severity == DQResult.WARNING)
        n_crit = sum(1 for r in self.results if r.severity == DQResult.CRITICAL)

        logger.info("─" * 60)
        logger.info(f"Data Quality Report — {self.stage}")
        logger.info(f"   ✅ {n_info} passed  ⚠️  {n_warn} warnings  🚨 {n_crit} critical")
        logger.info("─" * 60)
        for r in self.results:
            icon = {"INFO": "  ✅", "WARNING": "  ⚠️ ", "CRITICAL": "  🚨"}.get(r.severity, "  •")
            logger.info(f"{icon} [{r.rule}] {r.message}")
        logger.info("─" * 60)

        # บันทึกลง MongoDB
        self._save_to_mongo(n_info, n_warn, n_crit)

        # ถ้ามี CRITICAL → raise (Airflow จะ retry/alert)
        if n_crit > 0:
            critical_msgs = [r.message for r in self.results if r.severity == DQResult.CRITICAL]
            raise ValueError(
                f"Data Quality CRITICAL failures in {self.stage}: " + " | ".join(critical_msgs)
            )

        return self.results

    def _save_to_mongo(self, n_info, n_warn, n_crit):
        """เก็บประวัติ DQ ทุกครั้งที่รัน — ใช้สำหรับ dashboard + report"""
        try:
            client = MongoClient(MONGO_URI)
            db = client[DB_NAME]
            db[DQ_RESULTS_COLLECTION].insert_one({
                "run_id": self.run_id,
                "stage": self.stage,
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "summary": {
                    "passed": n_info,
                    "warnings": n_warn,
                    "critical": n_crit,
                    "total_checks": len(self.results),
                },
                "results": [r.to_dict() for r in self.results],
            })
            db[DQ_RESULTS_COLLECTION].create_index("timestamp")
            db[DQ_RESULTS_COLLECTION].create_index("stage")
            client.close()
        except Exception as e:
            logger.warning(f"Failed to save DQ results to MongoDB: {e}")