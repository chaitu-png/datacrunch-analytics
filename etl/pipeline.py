"""
ETL Pipeline - Extract, Transform, Load operations.

BUG INVENTORY:
- BUG-061: Transform step mutates source data (side effects)
- BUG-062: No checkpointing - crash requires full restart
- BUG-063: Date parsing doesn't handle timezone
- BUG-064: Memory leak in batch accumulator
"""

import copy
import time
from datetime import datetime
from typing import List, Dict, Any, Callable, Optional


class ETLJob:
    def __init__(self, job_id: str, name: str):
        self.job_id = job_id
        self.name = name
        self.status = "idle"
        self.records_processed = 0
        self.records_failed = 0
        self.started_at = None
        self.completed_at = None
        self.errors: List[str] = []


class ETLPipeline:
    """Batch ETL pipeline for data processing."""

    def __init__(self):
        self.jobs: Dict[str, ETLJob] = {}
        self._job_counter = 0
        # BUG-064: Accumulator grows indefinitely across job runs
        self._batch_accumulator: List[Dict] = []

    def create_job(self, name: str) -> ETLJob:
        """Create a new ETL job."""
        self._job_counter += 1
        job_id = f"ETL-{self._job_counter:06d}"
        job = ETLJob(job_id, name)
        self.jobs[job_id] = job
        return job

    def extract(self, job_id: str, source_data: List[Dict]) -> List[Dict]:
        """
        Extract phase - read data from source.

        BUG-064: Accumulator never cleared between runs.
        """
        job = self.jobs.get(job_id)
        if not job:
            raise ValueError(f"Job not found: {job_id}")

        job.status = "extracting"
        job.started_at = datetime.utcnow()

        # BUG-064: Accumulates across ALL runs - memory leak
        self._batch_accumulator.extend(source_data)

        return source_data

    def transform(self, job_id: str, data: List[Dict],
                  transformations: List[Callable] = None) -> List[Dict]:
        """
        Transform phase - apply data transformations.

        BUG-061: Transforms modify data IN PLACE.
        The original source list is mutated as a side effect.

        BUG-063: Date fields parsed without timezone awareness.
        """
        job = self.jobs.get(job_id)
        if not job:
            raise ValueError(f"Job not found: {job_id}")

        job.status = "transforming"
        transformed = []

        for record in data:
            try:
                # BUG-061: Should be copy.deepcopy(record) to avoid mutation
                # Instead, modifies the original record
                row = record  # BUG: reference, not copy

                # Apply default transformations
                row = self._normalize_dates(row)
                row = self._clean_strings(row)
                row = self._fill_defaults(row)

                if transformations:
                    for transform_fn in transformations:
                        row = transform_fn(row)

                transformed.append(row)
                job.records_processed += 1

            except Exception as e:
                job.records_failed += 1
                job.errors.append(f"Transform error on record: {str(e)}")

        return transformed

    def load(self, job_id: str, data: List[Dict],
             destination: str = "database") -> int:
        """
        Load phase - write transformed data to destination.

        BUG-062: No checkpointing - if load fails midway,
        entire pipeline must restart from extract.
        """
        job = self.jobs.get(job_id)
        if not job:
            raise ValueError(f"Job not found: {job_id}")

        job.status = "loading"
        loaded = 0

        # BUG-062: No batch checkpointing
        for i, record in enumerate(data):
            try:
                # Simulate DB write
                self._write_record(record, destination)
                loaded += 1
            except Exception as e:
                # BUG-062: Fails without recording progress
                job.errors.append(f"Load error at record {i}: {str(e)}")
                # All previous successful writes are lost context

        job.status = "completed"
        job.completed_at = datetime.utcnow()
        return loaded

    def _normalize_dates(self, record: dict) -> dict:
        """
        Normalize date fields.

        BUG-063: Doesn't handle timezones.
        "2024-01-15T10:00:00+05:30" and "2024-01-15T10:00:00Z"
        are treated as the same time.
        """
        date_fields = ["created_at", "updated_at", "event_date", "timestamp"]
        for field in date_fields:
            if field in record and isinstance(record[field], str):
                try:
                    # BUG-063: Strips timezone info
                    dt_str = record[field]
                    # Naive parsing - ignores timezone offset
                    if "T" in dt_str:
                        dt_str = dt_str.split("+")[0].split("Z")[0]
                    record[field] = datetime.fromisoformat(dt_str).isoformat()
                except (ValueError, AttributeError):
                    pass
        return record

    def _clean_strings(self, record: dict) -> dict:
        """Clean string fields."""
        for key, value in record.items():
            if isinstance(value, str):
                record[key] = value.strip()
        return record

    def _fill_defaults(self, record: dict) -> dict:
        """Fill missing fields with defaults."""
        defaults = {
            "status": "unknown",
            "priority": "medium",
            "source": "unspecified",
        }
        for key, default in defaults.items():
            if key not in record:
                record[key] = default
        return record

    def _write_record(self, record: dict, destination: str):
        """Simulate writing a record to destination."""
        # Simulate occasional write failures (5% rate)
        if hash(str(record)) % 20 == 0:
            raise IOError("Simulated write failure")

    def get_job_status(self, job_id: str) -> dict:
        """Get ETL job status."""
        job = self.jobs.get(job_id)
        if not job:
            return {}

        return {
            "job_id": job.job_id,
            "name": job.name,
            "status": job.status,
            "processed": job.records_processed,
            "failed": job.records_failed,
            "errors": job.errors[-5:],  # Last 5 errors
            "started_at": job.started_at.isoformat() if job.started_at else None,
            "completed_at": job.completed_at.isoformat() if job.completed_at else None,
        }

    def get_pipeline_health(self) -> dict:
        """Get overall pipeline health metrics."""
        total_processed = sum(j.records_processed for j in self.jobs.values())
        total_failed = sum(j.records_failed for j in self.jobs.values())

        return {
            "total_jobs": len(self.jobs),
            "total_processed": total_processed,
            "total_failed": total_failed,
            "success_rate": (
                total_processed / (total_processed + total_failed) * 100
                if (total_processed + total_failed) > 0 else 0
            ),
            "accumulator_size": len(self._batch_accumulator),  # Shows leak
        }
