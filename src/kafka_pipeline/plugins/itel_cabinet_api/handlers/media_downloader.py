"""
iTel Cabinet Media Downloader Handler.

Downloads media files referenced in iTel form attachments and stores them
to OneLake blob storage at a plugin-specific path.
"""

import asyncio
import logging
from pathlib import Path
from typing import Any, Dict, List
from datetime import datetime
import aiohttp

from kafka_pipeline.plugins.enrichment import EnrichmentHandler, EnrichmentContext, EnrichmentResult


class ItelMediaDownloader(EnrichmentHandler):
    """
    Downloads media files for iTel Cabinet form attachments.

    This handler:
    1. Reads attachment data from context.data['itel_attachments']
    2. For each attachment, fetches media download URL from ClaimX API
    3. Downloads media file
    4. Uploads to OneLake at specified path pattern
    5. Updates blob_path in attachment data

    Configuration:
        blob_path_template: Path template with {project_id}, {task_id}, {media_id} placeholders
        claimx_connection: Named connection for ClaimX API
        concurrent_downloads: Max concurrent downloads (default: 5)
    """

    def __init__(self, config: Dict[str, Any]):
        """
        Initialize media downloader.

        Args:
            config: Handler configuration
        """
        super().__init__(config)
        self.blob_path_template = config.get(
            "blob_path_template",
            "abfss://7729c159-0674-42ba-abf7-a6756723c62f@onelake.dfs.fabric.microsoft.com/"
            "584315ee-2f41-4943-949f-901d438bcd36/Files/veriskPipeline/"
            "itel_cabinet_form_attachments/{project_id}/{task_id}/"
        )
        self.claimx_connection = config.get("claimx_connection", "claimx_api")
        self.concurrent_downloads = config.get("concurrent_downloads", 5)
        self.skip_download = config.get("skip_download", False)  # For testing

    async def enrich(self, context: EnrichmentContext) -> EnrichmentResult:
        """
        Download media files for all attachments.

        Expected data structure:
            context.data['itel_attachments'] - List of attachment dicts with claim_media_id
            context.data['project_id'] - Project ID
            context.data['task_id'] - Task ID

        Args:
            context: Enrichment context with attachment data

        Returns:
            EnrichmentResult with updated blob_path in attachments
        """
        try:
            attachments = context.data.get("itel_attachments", [])
            if not attachments:
                self._log(
                    logging.INFO,
                    "No attachments to download",
                    event_id=context.data.get("event_id"),
                )
                return EnrichmentResult.ok(context.data)

            project_id = context.data.get("project_id")
            task_id = context.data.get("task_id")
            event_id = context.data.get("event_id", "unknown")

            if not project_id or not task_id:
                self._log(
                    logging.WARNING,
                    "Missing project_id or task_id for media download",
                    event_id=event_id,
                )
                return EnrichmentResult.failed("Missing project_id or task_id")

            # Download all media files concurrently
            updated_attachments = await self._download_all_media(
                attachments=attachments,
                project_id=project_id,
                task_id=task_id,
                event_id=event_id,
                connection_manager=context.connection_manager,
            )

            # Update attachments in context
            context.data['itel_attachments'] = updated_attachments

            downloaded_count = sum(
                1 for a in updated_attachments if a.get("blob_path")
            )

            self._log(
                logging.INFO,
                "Media download complete",
                event_id=event_id,
                project_id=project_id,
                task_id=task_id,
                total_attachments=len(attachments),
                downloaded=downloaded_count,
                failed=len(attachments) - downloaded_count,
            )

            return EnrichmentResult.ok(context.data)

        except Exception as e:
            self._log_exception(
                e,
                "Failed to download media",
                event_id=context.data.get("event_id"),
            )
            return EnrichmentResult.failed(f"Media download error: {str(e)}")

    async def _download_all_media(
        self,
        attachments: List[Dict[str, Any]],
        project_id: int,
        task_id: int,
        event_id: str,
        connection_manager: Any,
    ) -> List[Dict[str, Any]]:
        """
        Download all media files concurrently.

        Args:
            attachments: List of attachment dicts
            project_id: Project ID
            task_id: Task ID
            event_id: Event ID for logging
            connection_manager: Connection manager for API calls

        Returns:
            Updated attachments with blob_path populated
        """
        # Create semaphore for concurrency control
        semaphore = asyncio.Semaphore(self.concurrent_downloads)

        # Download all media concurrently
        tasks = [
            self._download_single_media(
                attachment=attachment,
                project_id=project_id,
                task_id=task_id,
                event_id=event_id,
                connection_manager=connection_manager,
                semaphore=semaphore,
            )
            for attachment in attachments
        ]

        updated_attachments = await asyncio.gather(*tasks, return_exceptions=True)

        # Filter out exceptions and return successful downloads
        result = []
        for i, updated in enumerate(updated_attachments):
            if isinstance(updated, Exception):
                self._log(
                    logging.WARNING,
                    f"Failed to download media: {str(updated)}",
                    event_id=event_id,
                    media_id=attachments[i].get("claim_media_id"),
                )
                # Keep original attachment without blob_path
                result.append(attachments[i])
            else:
                result.append(updated)

        return result

    async def _download_single_media(
        self,
        attachment: Dict[str, Any],
        project_id: int,
        task_id: int,
        event_id: str,
        connection_manager: Any,
        semaphore: asyncio.Semaphore,
    ) -> Dict[str, Any]:
        """
        Download a single media file.

        Args:
            attachment: Attachment dict with claim_media_id
            project_id: Project ID
            task_id: Task ID
            event_id: Event ID for logging
            connection_manager: Connection manager for API calls
            semaphore: Semaphore for concurrency control

        Returns:
            Updated attachment dict with blob_path
        """
        async with semaphore:
            media_id = attachment.get("claim_media_id")
            if not media_id:
                return attachment

            if self.skip_download:
                # For testing - just set a fake path
                blob_path = self._build_blob_path(
                    project_id, task_id, media_id, "test.jpg"
                )
                attachment["blob_path"] = blob_path
                return attachment

            try:
                # Get media download URL from ClaimX API
                # Endpoint: /export/project/{projectId}/media?mediaIds={mediaId}
                endpoint = f"/export/project/{project_id}/media"

                status, response = await connection_manager.request_json(
                    connection_name=self.claimx_connection,
                    method="GET",
                    path=endpoint,
                    params={"mediaIds": media_id},
                )

                if status != 200 or not response:
                    raise Exception(f"ClaimX API returned status {status}")

                # Extract download URL from response
                media_list = response.get("data", []) if isinstance(response, dict) else response
                if not media_list or len(media_list) == 0:
                    raise Exception(f"No media found for media_id {media_id}")

                media_data = media_list[0]
                download_url = media_data.get("fullDownloadLink")
                file_name = media_data.get("mediaName", f"{media_id}.jpg")

                if not download_url:
                    raise Exception("No download URL in media response")

                # Download media file
                media_bytes = await self._download_from_url(download_url)

                # Build blob path
                blob_path = self._build_blob_path(
                    project_id, task_id, media_id, file_name
                )

                # Upload to OneLake
                await self._upload_to_blob(blob_path, media_bytes)

                # Update attachment with blob path
                attachment["blob_path"] = blob_path

                self._log(
                    logging.DEBUG,
                    "Media downloaded successfully",
                    event_id=event_id,
                    media_id=media_id,
                    blob_path=blob_path,
                )

                return attachment

            except Exception as e:
                self._log(
                    logging.WARNING,
                    f"Failed to download media {media_id}: {str(e)}",
                    event_id=event_id,
                    media_id=media_id,
                )
                # Return original attachment without blob_path
                return attachment

    async def _download_from_url(self, url: str) -> bytes:
        """
        Download file from URL.

        Args:
            url: Download URL

        Returns:
            File content as bytes
        """
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as response:
                response.raise_for_status()
                return await response.read()

    async def _upload_to_blob(self, blob_path: str, data: bytes):
        """
        Upload data to OneLake blob storage.

        Args:
            blob_path: Full ABFSS path
            data: File content as bytes
        """
        # Use Spark to write to ABFSS
        from pyspark.sql import SparkSession
        spark = SparkSession.builder.getOrCreate()

        # Write bytes to blob using Spark
        # Note: This is a simplified approach - you may want to use
        # Azure SDK or fsspec for more control
        rdd = spark.sparkContext.parallelize([data])
        rdd.saveAsTextFile(blob_path)

    def _build_blob_path(
        self, project_id: int, task_id: int, media_id: int, file_name: str
    ) -> str:
        """
        Build blob storage path from template.

        Args:
            project_id: Project ID
            task_id: Task ID
            media_id: Media ID
            file_name: Original file name

        Returns:
            Full blob path
        """
        # Fill in template placeholders
        base_path = self.blob_path_template.format(
            project_id=project_id,
            task_id=task_id,
        )

        # Sanitize file name
        safe_file_name = file_name.replace(" ", "_").replace("/", "_")

        # Append media_id and file name
        blob_path = f"{base_path}{media_id}_{safe_file_name}"

        return blob_path
