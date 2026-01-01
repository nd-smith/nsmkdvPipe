"""Tests for Eventhouse deduplication."""

from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

import polars as pl
import pytest

from kafka_pipeline.eventhouse.dedup import (
    DedupConfig,
    EventhouseDeduplicator,
    get_recent_trace_ids_sync,
)


class TestDedupConfig:
    """Tests for DedupConfig."""

    def test_default_values(self):
        """Test default configuration values."""
        config = DedupConfig(xact_events_table_path="abfss://test/xact_events")

        assert config.xact_events_window_hours == 24
        assert config.eventhouse_query_window_hours == 1
        assert config.overlap_minutes == 5
        assert config.max_trace_ids_per_query == 50_000

    def test_custom_values(self):
        """Test custom configuration values."""
        config = DedupConfig(
            xact_events_table_path="abfss://test/xact_events",
            xact_events_window_hours=48,
            eventhouse_query_window_hours=2,
            overlap_minutes=10,
            max_trace_ids_per_query=10_000,
        )

        assert config.xact_events_window_hours == 48
        assert config.eventhouse_query_window_hours == 2
        assert config.overlap_minutes == 10
        assert config.max_trace_ids_per_query == 10_000


class TestEventhouseDeduplicator:
    """Tests for EventhouseDeduplicator."""

    @pytest.fixture
    def config(self):
        """Create test configuration."""
        return DedupConfig(
            xact_events_table_path="abfss://test/xact_events",
            xact_events_window_hours=24,
            eventhouse_query_window_hours=1,
            overlap_minutes=5,
            max_trace_ids_per_query=1000,
        )

    @pytest.fixture
    def dedup(self, config):
        """Create deduplicator instance."""
        return EventhouseDeduplicator(config)

    def test_build_kql_anti_join_filter_empty(self, dedup):
        """Test anti-join filter with no trace_ids."""
        result = dedup.build_kql_anti_join_filter([])
        assert result == ""

    def test_build_kql_anti_join_filter_single(self, dedup):
        """Test anti-join filter with single trace_id."""
        result = dedup.build_kql_anti_join_filter(["trace-123"])
        assert result == "trace_id !in (dynamic(['trace-123']))"

    def test_build_kql_anti_join_filter_multiple(self, dedup):
        """Test anti-join filter with multiple trace_ids."""
        result = dedup.build_kql_anti_join_filter(["trace-1", "trace-2", "trace-3"])
        assert result == "trace_id !in (dynamic(['trace-1', 'trace-2', 'trace-3']))"

    def test_build_kql_anti_join_filter_escapes_quotes(self, dedup):
        """Test anti-join filter escapes single quotes."""
        result = dedup.build_kql_anti_join_filter(["trace-with-'quote"])
        assert result == "trace_id !in (dynamic(['trace-with-\\'quote']))"

    def test_get_poll_window_first_poll(self, dedup):
        """Test poll window calculation for first poll."""
        poll_from, poll_to = dedup.get_poll_window(last_poll_time=None)

        now = datetime.now(timezone.utc)

        # poll_from should be ~1 hour ago (eventhouse_query_window_hours)
        expected_from = now - timedelta(hours=1)
        assert abs((poll_from - expected_from).total_seconds()) < 2

        # poll_to should be ~now
        assert abs((poll_to - now).total_seconds()) < 2

    def test_get_poll_window_subsequent_poll(self, dedup):
        """Test poll window calculation for subsequent poll."""
        last_poll = datetime.now(timezone.utc) - timedelta(minutes=30)

        poll_from, poll_to = dedup.get_poll_window(last_poll_time=last_poll)

        now = datetime.now(timezone.utc)

        # poll_from should be last_poll - overlap (5 min)
        expected_from = last_poll - timedelta(minutes=5)
        assert abs((poll_from - expected_from).total_seconds()) < 2

        # poll_to should be ~now
        assert abs((poll_to - now).total_seconds()) < 2

    def test_build_deduped_query_no_trace_ids(self, config):
        """Test building query when no trace_ids exist."""
        with patch.object(
            EventhouseDeduplicator,
            "get_recent_trace_ids",
            return_value=[],
        ):
            dedup = EventhouseDeduplicator(config)

            poll_from = datetime(2024, 1, 15, 10, 0, 0, tzinfo=timezone.utc)
            poll_to = datetime(2024, 1, 15, 11, 0, 0, tzinfo=timezone.utc)

            query = dedup.build_deduped_query(
                base_table="Events",
                poll_from=poll_from,
                poll_to=poll_to,
                limit=500,
            )

            # Should have time filters but no anti-join
            assert "Events" in query
            assert "ingestion_time() >= datetime(2024-01-15T10:00:00Z)" in query
            assert "ingestion_time() < datetime(2024-01-15T11:00:00Z)" in query
            assert "trace_id !in" not in query
            assert "take 500" in query

    def test_build_deduped_query_with_trace_ids(self, config):
        """Test building query with trace_ids for deduplication."""
        with patch.object(
            EventhouseDeduplicator,
            "get_recent_trace_ids",
            return_value=["trace-1", "trace-2"],
        ):
            dedup = EventhouseDeduplicator(config)

            poll_from = datetime(2024, 1, 15, 10, 0, 0, tzinfo=timezone.utc)
            poll_to = datetime(2024, 1, 15, 11, 0, 0, tzinfo=timezone.utc)

            query = dedup.build_deduped_query(
                base_table="Events",
                poll_from=poll_from,
                poll_to=poll_to,
            )

            # Should have time filters AND anti-join
            assert "Events" in query
            assert "ingestion_time() >= datetime(2024-01-15T10:00:00Z)" in query
            assert "trace_id !in (dynamic(['trace-1', 'trace-2']))" in query
            assert "take 1000" in query

    def test_build_deduped_query_with_additional_filters(self, config):
        """Test building query with additional filters."""
        with patch.object(
            EventhouseDeduplicator,
            "get_recent_trace_ids",
            return_value=[],
        ):
            dedup = EventhouseDeduplicator(config)

            poll_from = datetime(2024, 1, 15, 10, 0, 0, tzinfo=timezone.utc)

            query = dedup.build_deduped_query(
                base_table="Events",
                poll_from=poll_from,
                additional_filters="event_type == 'xact'",
            )

            assert "event_type == 'xact'" in query

    def test_build_deduped_query_time_filter_first(self, config):
        """Test that time filter comes before anti-join."""
        with patch.object(
            EventhouseDeduplicator,
            "get_recent_trace_ids",
            return_value=["trace-1"],
        ):
            dedup = EventhouseDeduplicator(config)

            poll_from = datetime(2024, 1, 15, 10, 0, 0, tzinfo=timezone.utc)

            query = dedup.build_deduped_query(
                base_table="Events",
                poll_from=poll_from,
            )

            lines = query.split("\n")

            # Find indices of time filter and anti-join
            time_filter_idx = None
            anti_join_idx = None

            for i, line in enumerate(lines):
                if "ingestion_time()" in line:
                    time_filter_idx = i
                    break

            for i, line in enumerate(lines):
                if "trace_id !in" in line:
                    anti_join_idx = i
                    break

            # Time filter should come before anti-join
            assert time_filter_idx is not None
            assert anti_join_idx is not None
            assert time_filter_idx < anti_join_idx

    def test_build_deduped_query_truncates_large_trace_ids(self, config):
        """Test that large trace_id lists are truncated."""
        # Create more trace_ids than max
        large_trace_ids = [f"trace-{i}" for i in range(1500)]

        with patch.object(
            EventhouseDeduplicator,
            "get_recent_trace_ids",
            return_value=large_trace_ids,
        ):
            dedup = EventhouseDeduplicator(config)

            poll_from = datetime(2024, 1, 15, 10, 0, 0, tzinfo=timezone.utc)

            query = dedup.build_deduped_query(
                base_table="Events",
                poll_from=poll_from,
            )

            # Should have anti-join filter with truncated list
            assert "trace_id !in" in query

            # Count trace_ids in query - should be max_trace_ids_per_query (1000)
            # by counting occurrences of "trace-"
            trace_count = query.count("'trace-")
            assert trace_count == config.max_trace_ids_per_query

    def test_get_recent_trace_ids_with_mock_delta(self, dedup):
        """Test getting trace_ids from mocked Delta table."""
        # Create mock DataFrame
        mock_df = pl.DataFrame(
            {
                "trace_id": ["trace-1", "trace-2", "trace-3"],
            }
        )

        with (
            patch.object(dedup, "_get_storage_options", return_value={}),
            patch("polars.scan_delta") as mock_scan,
        ):
            # Setup mock chain
            mock_lazy = MagicMock()
            mock_lazy.filter.return_value = mock_lazy
            mock_lazy.select.return_value = mock_lazy
            mock_lazy.collect.return_value = mock_df
            mock_scan.return_value = mock_lazy

            result = dedup.get_recent_trace_ids()

            assert result == ["trace-1", "trace-2", "trace-3"]
            mock_scan.assert_called_once()

    def test_get_recent_trace_ids_table_not_found(self, dedup):
        """Test graceful handling when table doesn't exist."""
        with (
            patch.object(dedup, "_get_storage_options", return_value={}),
            patch("polars.scan_delta") as mock_scan,
        ):
            mock_scan.side_effect = FileNotFoundError("Table not found")

            result = dedup.get_recent_trace_ids()

            # Should return empty list, not raise
            assert result == []


class TestGetRecentTraceIdsSync:
    """Tests for standalone get_recent_trace_ids_sync function."""

    def test_returns_empty_on_file_not_found(self):
        """Test returns empty list when table doesn't exist."""
        with patch("polars.scan_delta") as mock_scan:
            mock_scan.side_effect = FileNotFoundError("Table not found")

            result = get_recent_trace_ids_sync(
                table_path="abfss://test/xact_events",
                storage_options={},
            )

            assert result == []

    def test_returns_trace_ids(self):
        """Test returns trace_ids from table."""
        mock_df = pl.DataFrame({"trace_id": ["id1", "id2"]})

        with patch("polars.scan_delta") as mock_scan:
            mock_lazy = MagicMock()
            mock_lazy.filter.return_value = mock_lazy
            mock_lazy.select.return_value = mock_lazy
            mock_lazy.collect.return_value = mock_df
            mock_scan.return_value = mock_lazy

            result = get_recent_trace_ids_sync(
                table_path="abfss://test/xact_events",
                storage_options={},
            )

            assert result == ["id1", "id2"]
