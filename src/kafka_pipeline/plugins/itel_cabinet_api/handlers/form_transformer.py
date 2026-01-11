"""
iTel Cabinet Form Response Transformer.

Transforms ClaimX task responses for iTel Cabinet Repair Form (task 32513)
into structured data for submissions and attachments tables.
"""

from datetime import datetime
from typing import Any, Dict, List, Optional


class ItelFormTransformer:
    """
    Transforms iTel Cabinet task responses into database rows.

    Parses the nested form response structure from ClaimX API and extracts:
    - Submission data (form metadata and cabinet damage details)
    - Media attachments (photos linked to specific questions)
    """

    # Question control ID to question_key mapping
    MEDIA_QUESTION_MAPPING = {
        "control-110496963": "overview_photos",
        "control-989806632": "lower_cabinet_box",
        "control-374047064": "lower_cabinet_end_panels",
        "control-767670035": "upper_cabinet_box",
        "control-488956803": "upper_cabinet_end_panels",
        "control-734859955": "full_height_cabinet_box",
        "control-959393098": "full_height_end_panels",
        "control-393858054": "island_cabinet_box",
        "control-532071774": "island_cabinet_end_panels",
    }

    @staticmethod
    def to_submission_row(task_data: Dict[str, Any], event_id: str) -> Dict[str, Any]:
        """
        Transform task response to submission row.

        Args:
            task_data: Full task data from ClaimX API
            event_id: Event ID for traceability

        Returns:
            Submission row dict matching claimx_itel_forms schema
        """
        response = task_data.get("response", {})
        external_link = task_data.get("externalLinkData", {})

        # Parse all groups and questions into flat structure
        form_data = ItelFormTransformer._parse_form_response(response)

        now = datetime.utcnow()

        return {
            # Primary identifiers
            "assignment_id": task_data.get("assignmentId"),
            "project_id": task_data.get("projectId"),
            "form_id": task_data.get("formId"),
            "form_response_id": task_data.get("formResponseId"),
            "status": task_data.get("status"),

            # Dates
            "date_assigned": task_data.get("dateAssigned"),
            "date_completed": task_data.get("dateCompleted"),

            # Customer information
            "customer_first_name": external_link.get("firstName"),
            "customer_last_name": external_link.get("lastName"),
            "customer_email": external_link.get("email"),
            "customer_phone": str(external_link.get("phone")) if external_link.get("phone") else None,
            "assignor_email": task_data.get("assignor"),

            # General damage information
            "damage_description": form_data.get("damage_description"),
            "additional_notes": form_data.get("additional_notes"),
            "countertops_lf": form_data.get("countertops_lf"),

            # Lower Cabinets
            "lower_cabinets_damaged": form_data.get("lower_cabinets_damaged"),
            "lower_cabinets_lf": form_data.get("lower_cabinets_lf"),
            "num_damaged_lower_boxes": form_data.get("num_damaged_lower_boxes"),
            "lower_cabinets_detached": form_data.get("lower_cabinets_detached"),
            "lower_face_frames_doors_drawers_available": form_data.get("lower_face_frames_doors_drawers_available"),
            "lower_face_frames_doors_drawers_damaged": form_data.get("lower_face_frames_doors_drawers_damaged"),
            "lower_finished_end_panels_damaged": form_data.get("lower_finished_end_panels_damaged"),
            "lower_end_panel_damage_present": form_data.get("lower_end_panel_damage_present"),
            "lower_counter_type": form_data.get("lower_counter_type"),

            # Upper Cabinets
            "upper_cabinets_damaged": form_data.get("upper_cabinets_damaged"),
            "upper_cabinets_lf": form_data.get("upper_cabinets_lf"),
            "num_damaged_upper_boxes": form_data.get("num_damaged_upper_boxes"),
            "upper_cabinets_detached": form_data.get("upper_cabinets_detached"),
            "upper_face_frames_doors_drawers_available": form_data.get("upper_face_frames_doors_drawers_available"),
            "upper_face_frames_doors_drawers_damaged": form_data.get("upper_face_frames_doors_drawers_damaged"),
            "upper_finished_end_panels_damaged": form_data.get("upper_finished_end_panels_damaged"),
            "upper_end_panel_damage_present": form_data.get("upper_end_panel_damage_present"),

            # Full Height Cabinets
            "full_height_cabinets_damaged": form_data.get("full_height_cabinets_damaged"),
            "full_height_cabinets_lf": form_data.get("full_height_cabinets_lf"),
            "num_damaged_full_height_boxes": form_data.get("num_damaged_full_height_boxes"),
            "full_height_cabinets_detached": form_data.get("full_height_cabinets_detached"),
            "full_height_face_frames_doors_drawers_available": form_data.get("full_height_face_frames_doors_drawers_available"),
            "full_height_face_frames_doors_drawers_damaged": form_data.get("full_height_face_frames_doors_drawers_damaged"),
            "full_height_finished_end_panels_damaged": form_data.get("full_height_finished_end_panels_damaged"),

            # Island Cabinets
            "island_cabinets_damaged": form_data.get("island_cabinets_damaged"),
            "island_cabinets_lf": form_data.get("island_cabinets_lf"),
            "num_damaged_island_boxes": form_data.get("num_damaged_island_boxes"),
            "island_cabinets_detached": form_data.get("island_cabinets_detached"),
            "island_face_frames_doors_drawers_available": form_data.get("island_face_frames_doors_drawers_available"),
            "island_face_frames_doors_drawers_damaged": form_data.get("island_face_frames_doors_drawers_damaged"),
            "island_finished_end_panels_damaged": form_data.get("island_finished_end_panels_damaged"),
            "island_end_panel_damage_present": form_data.get("island_end_panel_damage_present"),
            "island_counter_type": form_data.get("island_counter_type"),

            # Metadata
            "created_at": now,
            "updated_at": now,
        }

    @staticmethod
    def to_attachment_rows(
        task_data: Dict[str, Any],
        assignment_id: int,
        event_id: str,
    ) -> List[Dict[str, Any]]:
        """
        Extract media attachments from task response.

        Args:
            task_data: Full task data from ClaimX API
            assignment_id: Assignment ID
            event_id: Event ID for traceability

        Returns:
            List of attachment row dicts matching claimx_itel_attachments schema
        """
        response = task_data.get("response", {})
        groups = response.get("groups", [])

        attachments = []
        display_order = 0
        now = datetime.utcnow()

        for group in groups:
            questions = group.get("questionAndAnswers", [])

            for question in questions:
                control_id = question.get("formControl", {}).get("id")
                question_text = question.get("questionText", "")

                # Check if this is a media question we care about
                question_key = ItelFormTransformer.MEDIA_QUESTION_MAPPING.get(control_id)
                if not question_key:
                    continue

                # Extract claimMediaIds from response
                answer_export = question.get("responseAnswerExport", {})
                claim_media_ids = answer_export.get("claimMediaIds", [])

                # Create attachment row for each media ID
                for media_id in claim_media_ids:
                    if media_id:
                        attachments.append({
                            "id": None,  # Will be auto-generated
                            "assignment_id": assignment_id,
                            "question_key": question_key,
                            "question_text": question_text,
                            "claim_media_id": media_id,
                            "blob_path": None,  # Populated by ItelMediaDownloader handler
                            "display_order": display_order,
                            "created_at": now,
                        })
                        display_order += 1

        return attachments

    @staticmethod
    def _parse_form_response(response: Dict[str, Any]) -> Dict[str, Any]:
        """
        Parse nested form response structure into flat key-value dict.

        Args:
            response: The 'response' object from task data

        Returns:
            Flat dict with form field names as keys
        """
        form_data = {}
        groups = response.get("groups", [])

        for group in groups:
            questions = group.get("questionAndAnswers", [])

            for question in questions:
                question_text = question.get("questionText", "")
                answer_export = question.get("responseAnswerExport", {})

                # Parse based on answer type
                answer_type = answer_export.get("type")

                if answer_type == "option":
                    # Dropdown/radio answers
                    option_answer = answer_export.get("optionAnswer", {})
                    value = option_answer.get("name")
                    field_name = ItelFormTransformer._question_to_field_name(question_text)

                    # Convert Yes/No to boolean for boolean fields
                    if value in ("Yes", "No"):
                        if any(keyword in question_text.lower() for keyword in ["damaged", "detached", "present"]):
                            form_data[field_name] = (value == "Yes")
                        else:
                            form_data[field_name] = value
                    else:
                        form_data[field_name] = value

                elif answer_type == "number":
                    # Numeric answers
                    number_answer = answer_export.get("numberAnswer")
                    field_name = ItelFormTransformer._question_to_field_name(question_text)
                    if number_answer is not None:
                        form_data[field_name] = int(number_answer)

                elif answer_type == "text":
                    # Text answers
                    text_answer = answer_export.get("textAnswer")
                    field_name = ItelFormTransformer._question_to_field_name(question_text)
                    form_data[field_name] = text_answer

                # Skip image type - handled separately in to_attachment_rows

        return form_data

    @staticmethod
    def _question_to_field_name(question_text: str) -> str:
        """
        Convert question text to database field name.

        Examples:
            "Lower Cabinets Damaged?" -> "lower_cabinets_damaged"
            "Number of Damaged Lower Cabinet Boxes" -> "num_damaged_lower_boxes"
        """
        # Simple mapping - extend as needed
        field_mapping = {
            "Lower Cabinets Damaged?": "lower_cabinets_damaged",
            "Linear Feet of Lower Cabinets": "lower_cabinets_lf",
            "Number of Damaged Lower Cabinet Boxes": "num_damaged_lower_boxes",
            "Lower Cabinets Detached from Wall?": "lower_cabinets_detached",
            "Lower Cabinets - Face Frames, Doors, and Drawer Fronts Available?": "lower_face_frames_doors_drawers_available",
            "Lower Cabinets - Face Frames, Doors, and Drawer Fronts Damaged?": "lower_face_frames_doors_drawers_damaged",
            "Lower Cabinets - Finished End Panels Damaged?": "lower_finished_end_panels_damaged",
            "Lower Cabinets - End Panel Damage Present?": "lower_end_panel_damage_present",
            "Lower Cabinets - Counter Type": "lower_counter_type",

            "Upper Cabinets Damaged?": "upper_cabinets_damaged",
            "Linear Feet of Upper Cabinets": "upper_cabinets_lf",
            "Number of Damaged Upper Cabinet Boxes": "num_damaged_upper_boxes",
            "Upper Cabinets Detached from Wall?": "upper_cabinets_detached",
            "Upper Cabinets - Face Frames, Doors, and Drawer Fronts Available?": "upper_face_frames_doors_drawers_available",
            "Upper Cabinets - Face Frames, Doors, and Drawer Fronts Damaged?": "upper_face_frames_doors_drawers_damaged",
            "Upper Cabinets - Finished End Panels Damaged?": "upper_finished_end_panels_damaged",
            "Upper Cabinets - End Panel Damage Present?": "upper_end_panel_damage_present",

            "Full Height Cabinets Damaged?": "full_height_cabinets_damaged",
            "Linear Feet of Full Height Cabinets": "full_height_cabinets_lf",
            "Number of Damaged Full Height Cabinet Boxes": "num_damaged_full_height_boxes",
            "Full Height Cabinets Detached from Wall?": "full_height_cabinets_detached",
            "Full Height Cabinets - Face Frames, Doors, and Drawer Fronts Available?": "full_height_face_frames_doors_drawers_available",
            "Full Height Cabinets - Face Frames, Doors, and Drawer Fronts Damaged?": "full_height_face_frames_doors_drawers_damaged",
            "Full Height Cabinets - Finished End Panels Damaged?": "full_height_finished_end_panels_damaged",

            "Island Cabinets Damaged?": "island_cabinets_damaged",
            "Linear Feet of Island Cabinets": "island_cabinets_lf",
            "Number of Damaged Island Cabinet Boxes": "num_damaged_island_boxes",
            "Island Cabinets Detached from Floor?": "island_cabinets_detached",
            "Island Cabinets - Face Frames, Doors, and Drawer Fronts Available?": "island_face_frames_doors_drawers_available",
            "Island Cabinets - Face Frames, Doors, and Drawer Fronts Damaged?": "island_face_frames_doors_drawers_damaged",
            "Island Cabinets - Finished End Panels Damaged?": "island_finished_end_panels_damaged",
            "Island Cabinets - End Panel Damage Present?": "island_end_panel_damage_present",
            "Island Cabinets - Counter Type": "island_counter_type",

            "Describe the Damage": "damage_description",
            "Additional Notes": "additional_notes",
            "Countertops - Linear Feet": "countertops_lf",
        }

        return field_mapping.get(question_text, question_text.lower().replace(" ", "_").replace("?", ""))
