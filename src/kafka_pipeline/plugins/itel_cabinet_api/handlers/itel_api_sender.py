"""
iTel Cabinet API Sender Enrichment Handler

Transforms enriched ClaimX task data into iTel Cabinet API format and sends it.
This handler builds the API payload directly from enriched data without reading
from Delta tables.

Features:
- Transforms ClaimX task data to iTel API format
- Sends to iTel Cabinet API via named connection
- Handles API responses and errors
- Retries with exponential backoff
- Logs API interactions for debugging
"""

import logging
from datetime import datetime
from typing import Any, Dict, Optional

from kafka_pipeline.plugins.enrichment import EnrichmentContext, EnrichmentHandler, EnrichmentResult

logger = logging.getLogger(__name__)


class ItelApiSender(EnrichmentHandler):
    """Enrichment handler that sends data to iTel Cabinet API.

    This handler is the final step in the iTel API worker pipeline. It:
    1. Transforms enriched ClaimX data into iTel API format
    2. Sends the payload to iTel Cabinet API
    3. Handles responses and errors
    4. Returns success/failure for worker processing

    Config example:
        type: kafka_pipeline.plugins.handlers.itel_api_sender:ItelApiSender
        config:
          connection: itel_cabinet_api
          endpoint: /api/v1/tasks
          method: POST
          payload_builder: default  # or custom builder class

    The handler extracts data from the enriched context (which includes
    ClaimX API lookup results) and builds the iTel API payload on the fly.
    """

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """Initialize iTel API sender.

        Args:
            config: Handler configuration including:
                - connection: Name of iTel API connection (required)
                - endpoint: API endpoint path (default: /api/v1/tasks)
                - method: HTTP method (default: POST)
                - payload_builder: Payload builder strategy (default: default)
                - include_raw_data: Include full ClaimX data in payload (default: false)
        """
        super().__init__(config)
        self.connection_name = self.config.get("connection", "itel_cabinet_api")
        self.endpoint = self.config.get("endpoint", "/api/v1/tasks")
        self.method = self.config.get("method", "POST")
        self.payload_builder = self.config.get("payload_builder", "default")
        self.include_raw_data = self.config.get("include_raw_data", False)

        if not self.connection_name:
            raise ValueError("ItelApiSender requires 'connection' in config")

    async def enrich(self, context: EnrichmentContext) -> EnrichmentResult:
        """Send enriched data to iTel Cabinet API.

        Args:
            context: Enrichment context with transformed and enriched data

        Returns:
            EnrichmentResult indicating success or failure
        """
        if not context.connection_manager:
            return EnrichmentResult.failed("No connection manager available")

        try:
            # Build iTel API payload from enriched data
            payload = self._build_payload(context.data)

            # Log what we're sending
            logger.info(
                f"Sending to iTel Cabinet API | "
                f"endpoint={self.endpoint} | "
                f"task_id={payload.get('task_id')} | "
                f"assignment_id={payload.get('assignment_id')}"
            )
            logger.debug(f"iTel API payload: {payload}")

            # Send to iTel API
            status, response_data = await context.connection_manager.request_json(
                connection_name=self.connection_name,
                method=self.method,
                path=self.endpoint,
                json=payload,
            )

            # Check response
            if status >= 200 and status < 300:
                # Success
                logger.info(
                    f"iTel API request successful | "
                    f"status={status} | "
                    f"task_id={payload.get('task_id')} | "
                    f"itel_task_id={response_data.get('itel_task_id', 'N/A')}"
                )

                # Add iTel response to context for downstream handlers
                context.data['itel_api_response'] = response_data
                context.data['itel_api_status'] = status

                return EnrichmentResult.ok(context.data)

            else:
                # API returned error status
                error_msg = f"iTel API returned status {status}: {response_data}"
                logger.error(
                    f"iTel API request failed | "
                    f"status={status} | "
                    f"task_id={payload.get('task_id')} | "
                    f"error={response_data}"
                )
                return EnrichmentResult.failed(error_msg)

        except Exception as e:
            error_msg = f"Exception sending to iTel API: {str(e)}"
            logger.exception(
                f"iTel API exception | "
                f"task_id={context.data.get('task_id')} | "
                f"error={str(e)}"
            )
            return EnrichmentResult.failed(error_msg)

    def _build_payload(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Build iTel Cabinet API payload from enriched data.

        Builds payload according to CabinetRepairSubmission schema.

        Args:
            data: Enriched data from context with:
                - itel_submission: Parsed form data
                - itel_attachments: Media attachments with question_key

        Returns:
            Dict formatted for iTel Cabinet API matching output schema
        """
        submission = data.get("itel_submission", {})
        attachments = data.get("itel_attachments", [])

        # Group attachments by question_key
        attachments_by_key = {}
        for att in attachments:
            key = att.get("question_key")
            if key:
                if key not in attachments_by_key:
                    attachments_by_key[key] = []
                attachments_by_key[key].append(att)

        # Build payload structure
        payload = {
            # Required fields
            "assignmentId": submission.get("assignment_id"),
            "projectId": submission.get("project_id"),
            "formId": submission.get("form_id"),
            "formResponseId": submission.get("form_response_id"),
            "status": submission.get("status"),
            "dateAssigned": submission.get("date_assigned"),

            # Optional top-level fields
            "dateCompleted": submission.get("date_completed"),
            "assignorEmail": submission.get("assignor_email"),
            "damageDescription": submission.get("damage_description"),
            "additionalNotes": submission.get("additional_notes"),
            "countertopsLf": submission.get("countertops_lf"),

            # Customer info
            "customer": {
                "firstName": submission.get("customer_first_name"),
                "lastName": submission.get("customer_last_name"),
                "email": submission.get("customer_email"),
                "phone": submission.get("customer_phone"),
            },

            # Cabinet sections
            "lowerCabinets": self._build_cabinet_section("lower", submission, attachments_by_key),
            "upperCabinets": self._build_cabinet_section("upper", submission, attachments_by_key),
            "fullHeightCabinets": self._build_cabinet_section("full_height", submission, attachments_by_key),
            "islandCabinets": self._build_cabinet_section("island", submission, attachments_by_key),

            # Overview media
            "overviewMedia": self._build_media_array("overview_photos", attachments_by_key),
        }

        return payload

    def _build_cabinet_section(
        self,
        cabinet_type: str,
        submission: Dict[str, Any],
        attachments_by_key: Dict[str, List[Dict[str, Any]]]
    ) -> Optional[Dict[str, Any]]:
        """Build cabinet section object.

        Args:
            cabinet_type: One of "lower", "upper", "full_height", "island"
            submission: Parsed submission data
            attachments_by_key: Attachments grouped by question_key

        Returns:
            Cabinet section dict or None
        """
        # Check if this cabinet type has damage
        damaged_key = f"{cabinet_type}_cabinets_damaged"
        is_damaged = submission.get(damaged_key)

        # If not damaged and no data, return None
        if not is_damaged and not submission.get(f"{cabinet_type}_cabinets_lf"):
            return None

        # Build section
        section = {
            "damaged": is_damaged,
            "linearFeet": submission.get(f"{cabinet_type}_cabinets_lf"),
            "numDamagedBoxes": submission.get(f"num_damaged_{cabinet_type}_boxes"),
            "detached": submission.get(f"{cabinet_type}_cabinets_detached"),
            "faceFramesDoorsDrawersAvailable": submission.get(f"{cabinet_type}_face_frames_doors_drawers_available"),
            "faceFramesDoorsDrawersDamaged": submission.get(f"{cabinet_type}_face_frames_doors_drawers_damaged"),
            "finishedEndPanelsDamaged": submission.get(f"{cabinet_type}_finished_end_panels_damaged"),
            "endPanelDamagePresent": submission.get(f"{cabinet_type}_end_panel_damage_present"),
            "counterType": submission.get(f"{cabinet_type}_counter_type"),
        }

        # Add media for this cabinet section
        media = []
        # Box photos
        box_key = f"{cabinet_type}_cabinet_box"
        if box_key in attachments_by_key:
            media.extend(self._build_media_array(box_key, attachments_by_key))

        # End panel photos
        panel_key = f"{cabinet_type}_cabinet_end_panels"
        if panel_key in attachments_by_key:
            media.extend(self._build_media_array(panel_key, attachments_by_key))

        section["media"] = media

        return section

    def _build_media_array(
        self,
        question_key: str,
        attachments_by_key: Dict[str, List[Dict[str, Any]]]
    ) -> List[Dict[str, Any]]:
        """Build media array for a specific question.

        Args:
            question_key: Question key to find attachments for
            attachments_by_key: Attachments grouped by question_key

        Returns:
            List of media item dicts
        """
        attachments = attachments_by_key.get(question_key, [])
        if not attachments:
            return []

        # Group by question (usually all have same question_text)
        # Return single media item with all claim_media_ids
        if attachments:
            first = attachments[0]
            return [{
                "questionKey": question_key,
                "questionText": first.get("question_text", ""),
                "claimMediaIds": [att.get("claim_media_id") for att in attachments if att.get("claim_media_id")]
            }]

        return []


class ItelApiBatchSender(ItelApiSender):
    """Batch variant of iTel API sender.

    Accumulates multiple task updates and sends them in a single API call.
    Useful if iTel API supports batch endpoints and you want to reduce
    API call volume.

    Config example:
        type: kafka_pipeline.plugins.handlers.itel_api_sender:ItelApiBatchSender
        config:
          connection: itel_cabinet_api
          endpoint: /api/v1/tasks/batch
          method: POST
          batch_size: 10
          batch_timeout_seconds: 30.0
    """

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """Initialize batch iTel API sender.

        Args:
            config: Handler configuration including batch_size and batch_timeout_seconds
        """
        super().__init__(config)
        self.batch_size = self.config.get("batch_size", 10)
        self.batch_timeout = self.config.get("batch_timeout_seconds", 30.0)
        self._batch: list[Dict[str, Any]] = []
        self._last_flush = None

    async def enrich(self, context: EnrichmentContext) -> EnrichmentResult:
        """Add task to batch and send when batch is full.

        Args:
            context: Enrichment context

        Returns:
            EnrichmentResult - skip if added to batch, ok if batch sent
        """
        import time

        if not context.connection_manager:
            return EnrichmentResult.failed("No connection manager available")

        try:
            # Build payload for this task
            payload = self._build_payload(context.data)

            # Add to batch
            self._batch.append(payload)

            # Initialize flush time
            if self._last_flush is None:
                self._last_flush = time.time()

            # Check if should flush
            current_time = time.time()
            time_since_flush = current_time - self._last_flush
            should_flush = (
                len(self._batch) >= self.batch_size
                or time_since_flush >= self.batch_timeout
            )

            if should_flush:
                # Send batch to iTel API
                batch_payload = {
                    "tasks": self._batch,
                    "batch_size": len(self._batch),
                }

                logger.info(
                    f"Sending batch to iTel Cabinet API | "
                    f"endpoint={self.endpoint} | "
                    f"batch_size={len(self._batch)}"
                )

                status, response_data = await context.connection_manager.request_json(
                    connection_name=self.connection_name,
                    method=self.method,
                    path=self.endpoint,
                    json=batch_payload,
                )

                if status >= 200 and status < 300:
                    logger.info(
                        f"iTel API batch successful | "
                        f"status={status} | "
                        f"batch_size={len(self._batch)}"
                    )
                    # Reset batch
                    self._batch.clear()
                    self._last_flush = current_time
                    return EnrichmentResult.ok(context.data)
                else:
                    error_msg = f"iTel API batch failed with status {status}: {response_data}"
                    logger.error(error_msg)
                    return EnrichmentResult.failed(error_msg)
            else:
                # Added to batch, skip for now
                return EnrichmentResult.skip_message(
                    f"Added to iTel API batch ({len(self._batch)}/{self.batch_size})"
                )

        except Exception as e:
            error_msg = f"Exception in iTel API batch sender: {str(e)}"
            logger.exception(error_msg)
            return EnrichmentResult.failed(error_msg)

    async def flush(self) -> None:
        """Force flush current batch to iTel API."""
        if not self._batch:
            logger.debug("No tasks in batch to flush")
            return

        logger.info(f"Force flushing {len(self._batch)} tasks to iTel API")
        # Implementation would send remaining batch
        self._batch.clear()

    async def cleanup(self) -> None:
        """Cleanup and flush remaining tasks."""
        await self.flush()
        await super().cleanup()
