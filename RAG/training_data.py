
# =============================================================================================
# TRAINING_DATA.PY - SINGLE SOURCE OF TRUTH
# This file is the definitive source for all system configuration, including schema definitions,
# business rules, query templates, and few-shot training examples. To ensure portability and
# maintainability, all system knowledge should be centralized here.
# =============================================================================================
import json
from typing import Dict, List, Set, FrozenSet, Any, Optional
import re

# =============================================================================================
SCHEMA_DESCRIPTIONS = {
    'alerts': {
        'description': "Central table for all system alerts that may require user action. Use for queries about open/closed status, severity, and user assignments.",
        'columns': {
            'bu': "Business Unit (e.g., region or division)",
            'sap_id': "SAP identifier for the entity/location",
            'sop_id': "SOP ID related to the alert", 
            'location_name': "Name of the location where alert originated",
            'severity': "Alert severity. Valid values: 'CRITICAL', 'HIGH', 'MEDIUM', 'LOW'.",
            'alert_category': "Category of the alert. Valid values: 'Device Offline', 'Device Tamper', 'Power Disconnect', 'SOS', 'Over Speed'.",
            'alert_status': "Current status. Valid values: 'Open', 'Close', 'In Progress'. CRITICAL: Use 'Close' NOT 'Closed'. DO NOT use 'FALSE ALARM'.",
            'alert_state': "State of the alert (Active, Resolved, Escalated)",
            'unique_id': "Unique identifier for the alert",
            'alert_section': "Section/area where alert belongs : alert_section = 'VTS'",
            'external_id': "External system identifier",
            'interlock_name': "Name of interlock device",
            'interlock_id': "ID of interlock device",
            'device_id': "Device identifier",
            'equipment_id': "Equipment identifier", 
            'sensor_id': "Sensor identifier",
            'device_type': "Type of device",
            'equipment_type': "Type of equipment",
            'device_name': "Name of the device",
            'equipment_name': "Name of the equipment",
            'device_msg': "Message from device",
            'vehicle_number': "Vehicle ID associated with alert",
            'violation_type': "Type of violation detected",
            'clear_count': "Whether count has been cleared", 
            'maintenance_time': "Time required for maintenance",
            'tt_load_number': "Alternative vehicle/TT load number",
            'is_flagged_false': "Whether alert is flagged as false",
            'rca': "Root Cause Analysis",
            'rca_type': "Type of RCA",
            'alert_history': "Historical data of alert changes",
            'last_sms_to': "Last SMS recipients",
            'last_mailed_to': "Last email recipients",
            'last_escalated_to': "Last escalation recipients",
            'last_notified_to': "Last notification recipients",
            'assigned_to': "Person assigned to alert",
            'assigned_to_role': "Role of assigned person",
            'assigned_users': "All assigned users",
            'assigned_user_roles': "Roles of all assigned users",
            'district': "District location",
            'zone': "Zone/area",
            'region': "Geographic region",
            'state': "State location",
            'city': "City location",
            'sales_area': "Sales territory area",
            'raw_data': "Raw alert data in JSON format",
            'r1_time': "First response time",
            'r2_time': "Second response time",
            'r3_time': "Third response time",
            'indent_status': "Status of indent/order",
            'product_code': "Product code",
            'indent_no': "Indent/order number",
            'dealer_id': "Dealer identifier",
            'workflow_instance_id': "Workflow instance ID",
            'workflow_datetime': "Workflow date and time",
            'terminal_plant_id': "Terminal plant ID",
            'terminal_plant_name': "Terminal plant name",
            'servicing_plant_id': "Servicing plant ID",
            'servicing_plant_name': "Servicing plant name",
            'progress_rate': "Progress rate percentage",
            'category': "Alert category",
            'indent_raised_date': "Date when indent was raised",
            'dry_out_in_days': "Dry out period in days",
            'origin_altid': "Original alternative ID",
            'alert_message': "Alert message content",
            'external_timestamp': "Timestamp from external system",
            'atg_ack': "ATG system acknowledgment",
            'emlock_ack': "Emlock acknowledgment",
            'vts_return': "VTS return status",
            'atg_ack_time': "ATG acknowledgment time",
            'transporter_name': "Transporter company name",
            'transporter_code': "Transporter code",
            'vehicle_blocked_start_date': "Start date of vehicle blocking",
            'vehicle_blocked_end_date': "End date of vehicle blocking",
            'mark_as_false': "Mark alert as false. CRITICAL: Use this column for false alert queries (NOT is_flagged_false).",
            'id': "Primary key ID",
            'created_at': "Record creation timestamp. Canonical time column for this table.",
            'updated_at': "Record update timestamp",
            'entity_id': "Entity identifier",
            'closed_at': "Alert closure timestamp",
            'cause_effect': "Cause and effect analysis",
            'workflow_url': "Workflow URL",
            'workflow_port': "Workflow port",
            'tas_device_name': "TAS device name",
            'terminal_loc_code': "Terminal location code",
            'vts_alert_history_ids': "VTS alert history IDs",
            'dry_out_start_time': "Dry out period start time",
            'dry_out_end_time': "Dry out period end time",
            'intra_day_dry_out_start_time': "Intra-day dry out start time",
            'intra_day_dry_out_end_time': "Intra-day dry out end time",
            'vehicle_unblocked_date': "Vehicle unblocked date",
            'action_on': "Action taken on",
            'ro_offline': "Regional office offline status",
            'temporary_close': "Temporary closure flag",
            'permanent_close': "Permanent closure flag",
            'remarks': "General user remarks",
            'file_uploaded_path': "Storage path for uploaded documentation",
            'remarks_unblocked': "Remarks specifically provided during unblocking"
        }
    },
    'vts_alert_history': {
        'description': "Historical log of all violations and events, aggregated over time. Use for trend analysis and counting past violations.",
        'columns': {
            'vendor_id': "Vendor identifier",
            'location_id': "Location identifier",
            'location_type': "Type of location",
            'tl_number': "Transport License number. Vehicle ID in this table.",
            'report_duration': "Duration covered by report",
            'vts_start_datetime': "VTS alert start datetime",
            'vts_end_datetime': "VTS alert end datetime. Canonical time column for this table.",
            'total_trips': "Total trips in period",
            'stoppage_violations_count': "Count of stoppage violations.",
            'route_deviation_count': "Count of route deviations.",
            'speed_violation_count': "Count of speed violations.",
            'main_supply_removal_count': "Count of main supply removals.",
            'night_driving_count': "Count of night driving violations.",
            'no_halt_zone_count': "Count of no-halt zone violations.",
            'device_offline_count': "Count of device offline incidents.",
            'device_tamper_count': "Count of device tampering incidents.",
            'continuous_driving_count': "Count of continuous driving violations.", 
            'approved_by': "Person who approved",
            'auto_unblock': "Auto unblock status",
            'alert_id': "Alert identifier",
            'invoice_number': "Invoice number",
            'tt_type': "Type of TT (Transport Truck)",
            'violation_type': "Array of violation types.",
            'id': "Primary key ID",
            'created_at': "Record creation timestamp",
            'updated_at': "Record update timestamp", 
            'entity_id': "Entity identifier",
            'scheduled_trip_start_datetime': "Scheduled trip start datetime",
            'scheduled_trip_end_datetime': "Scheduled trip end datetime",
            'region': "Geographic region",
            'bu': "Business Unit",
            'zone': "Zone/area",
            'sap_id': "SAP identifier",
            'location_name': "Location name",
            'route_deviation_count_orig': "Original route deviation count before correction",
            'base_location_id': "Home location ID",
            'base_zone': "Home zone", 
            'base_region': "Home region",
            'base_location_name': "Home location name",
            'destination_code': "Target destination code"
        }
    },
    'vts_ongoing_trips': {
        'description': "Live tracking of trips that are currently in progress. Use for questions about 'now', 'current status', or 'live location'.",
        'columns': {
            'violation_type': "Type of violation",
            'event_start_datetime': "Event start datetime. Canonical time column for this table.", 
            'event_end_datetime': "Event end datetime",
            'sap_id': "SAP identifier",
            'region': "Geographic region",
            'zone': "Zone/area",
            'destination_code': "Destination code",
            'location_type': "Type of location",
            'tt_type': "Type of TT (Transport Truck)",
            'tt_number': "TT/vehicle number. Vehicle ID in this table.",
            'trip_id': "Trip identifier",
            'transporter_id': "Transporter identifier",
            'transporter_name': "Transporter company name",
            'invoice_no': "Invoice number",
            'load_no': "Load number",
            'route_no': "Route number",
            'driver_name': "Driver name",
            'scheduled_start_datetime': "Scheduled start datetime",
            'scheduled_end_datetime': "Scheduled end datetime",
            'actual_trip_start_datetime': "The actual timestamp when the trip started.",
            'actual_end_start_datetime': "Actual end/start datetime",
            'vehicle_latitude': "Vehicle latitude coordinate",
            'vehicle_longitude': "Vehicle longitude coordinate",
            'vehicle_location': "Vehicle location description",
            'id': "Primary key ID",
            'created_at': "Record creation timestamp",
            'updated_at': "Record update timestamp", 
            'entity_id': "Entity identifier",
            'bu': "Business Unit"
        }
    },
    'vts_truck_master': {
        'description': "Master data for all trucks. Contains definitive details like transporter, home zone, ownership, and blacklist status. Join to this table to get contextual information.",
        'columns': {
            'seq_key': "Sequence key",
            'ukid': "Unique key identifier",
            'mandt': "Mandant/client", 
            'truck_no': "Vehicle ID/Truck number. The canonical Vehicle ID for joining with other tables.",
            'sap_id': "SAP identifier",
            'vehicle_type': "Type of vehicle",
            'vehicle_type_desc': "Vehicle type description",
            'transporter_code': "Unique code for the transporter.",
            'customer_code': "Customer code",
            'ownership_type': "Ownership type (Company/Owned/Third Party).",
            'weight_uom': "Weight unit of measure",
            'volume_uom': "Volume unit of measure",
            'capacity_of_the_truck': "The truck's carrying capacity.",
            'empty_weight': "Empty weight of truck",
            'filled_weight': "Filled/loaded weight",
            'whether_truck_blacklisted': "Blacklisted status ('Y' or 'N').",
            'no_of_compartments': "Number of compartments in the truck.",
            'syncdt': "Synchronization date",
            'last_changed_date': "Last changed date",
            'clsgrp': "Class group",
            'load_dt': "Load date",
            'load_ts': "Load timestamp",
            'create_by': "Created by user",
            'transporter_name': "Name of the transporter company.",
            'location_name': "Location name",
            'bu': "Business Unit",
            'region': "Geographic region",
            'zone': "Zone/area",
            'engine_no': "Vehicle engine number",
            'chassis_no': "Vehicle chassis number"
        }
    },
    'vts_tripauditmaster': {
        'description': "Audit trail for trips, especially for emlock and swipe in/out events. Use for security and compliance checks.",
        'columns': {
            'id': "Primary key ID",
            'sap_id': "SAP identifier",
            'trucknumber': "Truck/vehicle number. Vehicle ID in this table.",
            'lockid1': "First lock ID", 
            'lockid2': "Second lock ID",
            'loadnumber': "Load number",
            'invoicenumber': "Invoice number associated with the trip.",
            'isemlocktrip': "Emlock trip flag ('Y' or 'N').",
            'swipeinl1': "Status of swipe-in at location 1 ('True' or 'False').",
            'swipeinl2': "Swipe in at location 2 ('True' or 'False').",
            'swipeoutl1': "Swipe out at location 1 ('True' or 'False' for failure). A 'False' value indicates a failure.",
            'swipeoutl2': "Swipe out at location 2 ('True' or 'False' for failure).",
            'createdat': "Record creation timestamp. Canonical time column for this table.",
            'bu': "Business Unit",
            'location_name': "Location name",
            'zone': "Zone/area",
            'region': "Geographic region"
        }
    },
    'tt_risk_score': {
        'description': "Calculated risk scores for each individual vehicle.",
        'columns': {
            'tt_number': "TT/vehicle number. Vehicle ID in this table.",
            'transporter_code': "Transporter code", 
            'transporter_name': "Transporter company name",
            'total_trips': "Total number of trips",
            'risk_score': "The overall risk score for the vehicle.",
            'device_removed': "Risk component score for device removal.",
            'power_disconnect': "Risk component score for power disconnects.",
            'route_deviation': "Risk component score for route deviations.",
            'stoppage_violation': "Risk component score for stoppage violations."
        }
    },
    'transporter_risk_score': {
        'description': "Aggregated risk scores for each transporter fleet.",
        'columns': {
            'transporter_code': "Unique code for the transporter.",
            'transporter_name': "Transporter company name", 
            'total_trips': "Total number of trips",
            'device_removed': "Device removed risk score",
            'power_disconnect': "Power disconnect risk score",
            'route_deviation': "Route deviation risk score",
            'stoppage_violation': "Stoppage violation risk score",
            'risk_score': "The overall risk score for the entire transporter fleet."
        }
    }
}

QUERY_TEMPLATES = {
    "vehicle_timeline": """
    WITH master_data AS (
        SELECT 
            truck_no,
            zone AS master_zone,
            location_name AS master_location,
            transporter_name AS master_transporter,
            transporter_code AS master_transporter_code
        FROM vts_truck_master 
        WHERE truck_no ILIKE '{vehicle_id}' 
        LIMIT 1
    )
    SELECT * FROM (
        -- Compliance (from vts_alert_history)
        SELECT
            'vts_alert_history'::TEXT AS "Source Table",
            'Compliance'::TEXT AS "Data Table",
            CASE 
                WHEN vah.violation_type IS NOT NULL 
                AND array_length(vah.violation_type, 1) > 0 
                THEN array_to_string(vah.violation_type, ', ')
                ELSE 'No Specific Violations'
            END AS "Event Type", 
            vah.vts_end_datetime::TIMESTAMP AS "Event Time",
            COALESCE(vah.location_name, md.master_location, 'N/A') AS "Location Info",
            vah.invoice_number::TEXT AS "Invoice Number",
            'zone: ' || COALESCE(vah.zone::text, md.master_zone, 'N/A') ||
            ', location: ' || COALESCE(vah.location_name::text, md.master_location, 'N/A') ||
            ', transporter_name: ' || COALESCE(md.master_transporter, 'N/A') AS "Additional Detail",
            vah.tl_number::TEXT AS "vehicle_identifier"
        FROM vts_alert_history vah
        LEFT JOIN master_data md ON vah.tl_number ILIKE md.truck_no
        WHERE vah.tl_number ILIKE '{vehicle_id}' {time_filter_vah} -- Use exact ILIKE
        AND (vah.violation_type IS NOT NULL AND array_length(vah.violation_type, 1) > 0)
 
        UNION ALL

        -- VTS Dashboard (from alerts)
        SELECT
            'alerts'::TEXT AS "Source Table",
            'VTS Dashboard'::TEXT AS "Data Table",
            COALESCE(a.violation_type, 'Alert') AS "Event Type",
            a.created_at::TIMESTAMP AS "Event Time",
            COALESCE(a.location_name, md.master_location, 'N/A') AS "Location Info",
            NULL AS "Invoice Number",
            'zone: ' || COALESCE(a.zone::text, md.master_zone, 'N/A') || 
            ', location_name: ' || COALESCE(a.location_name::text, md.master_location, 'N/A') ||
            ', transporter_name: ' || COALESCE(a.transporter_name, md.master_transporter, 'N/A') ||
            ', severity: ' || COALESCE(a.severity::TEXT, 'N/A') AS "Additional Detail",
            COALESCE(a.vehicle_number, a.tt_load_number)::TEXT AS "vehicle_identifier"
        FROM alerts a
        LEFT JOIN master_data md ON (a.vehicle_number ILIKE md.truck_no OR a.tt_load_number ILIKE md.truck_no)
        WHERE (a.vehicle_number ILIKE '{vehicle_id}' OR a.tt_load_number ILIKE '{vehicle_id}') {time_filter_a} -- Use exact ILIKE
        AND a.alert_section = 'VTS'
        AND a.violation_type IS NOT NULL

        UNION ALL

        -- VTS Live (from vts_ongoing_trips)
        SELECT
            'vts_ongoing_trips'::TEXT AS "Source Table",
            'VTS Live'::TEXT AS "Data Table",
            CASE vot.violation_type
                WHEN 'HS' THEN 'Unauthorized stoppage at Hotspots'
                WHEN 'RD' THEN 'Route deviation beyond 2 km'
                WHEN 'TC' THEN 'Trip pending closure (2+ hrs)'
                WHEN 'WR' THEN 'TT without VTS device'
                ELSE COALESCE(vot.violation_type, 'Ongoing Trip')
            END AS "Event Type",
            vot.created_at::TIMESTAMP AS "Event Time",
            COALESCE(vot.vehicle_location, md.master_location, 'N/A') AS "Location Info",
            vot.invoice_no::TEXT AS "Invoice Number",
            'zone: ' || COALESCE(vot.zone::text, md.master_zone, 'N/A') ||
            ', location_name: ' || COALESCE(vot.vehicle_location, md.master_location, 'N/A') ||
            ', transporter_name: ' || COALESCE(vot.transporter_name, md.master_transporter, 'N/A') AS "Additional Detail",
            vot.tt_number::TEXT AS "vehicle_identifier"
        FROM vts_ongoing_trips vot
        LEFT JOIN master_data md ON vot.tt_number ILIKE md.truck_no
        WHERE vot.tt_number ILIKE '{vehicle_id}' {time_filter_vot} -- Use exact ILIKE
        AND vot.violation_type IS NOT NULL


        UNION ALL

        -- Trip Audit (from vts_tripauditmaster)
        SELECT
            'vts_tripauditmaster'::TEXT AS "Source Table",
            'Trip Audit'::TEXT AS "Data Table",
            TRIM(BOTH ', ' FROM
                CONCAT_WS(', ',
                    CASE WHEN tam.swipeoutl1 = 'False' THEN 'Swipe Out L1 Failed' ELSE NULL END,
                    CASE WHEN tam.swipeoutl2 = 'False' THEN 'Swipe Out L2 Failed' ELSE NULL END
                )
            ) AS "Event Type",
            tam.createdat::TIMESTAMP AS "Event Time",
            COALESCE(tam.location_name, md.master_location, 'N/A') AS "Location Info",
            tam.invoicenumber::TEXT AS "Invoice Number",
            'zone: ' || COALESCE(tam.zone::text, md.master_zone, 'N/A') ||
            ', location_name: ' || COALESCE(tam.location_name::text, md.master_location, 'N/A') ||
            ', transporter_name: ' || COALESCE(md.master_transporter, 'N/A') AS "Additional Detail",
            tam.trucknumber::TEXT AS "vehicle_identifier"
        FROM vts_tripauditmaster tam
        LEFT JOIN master_data md ON tam.trucknumber ILIKE md.truck_no
        WHERE tam.trucknumber ILIKE '{vehicle_id}' {time_filter_tam} -- Use exact ILIKE
          AND (tam.swipeoutl1 = 'False' OR tam.swipeoutl2 = 'False')

        UNION ALL

        -- Risk Score (from tt_risk_score)
        SELECT
            'tt_risk_score'::TEXT AS "Source Table",
            'Risk Score'::TEXT AS "Data Table",
            'Vehicle Risk Score: ' || COALESCE(trs.risk_score::TEXT, 'N/A') AS "Event Type",
            CURRENT_TIMESTAMP AS "Event Time", -- tt_risk_score has no timestamp
            md.master_location AS "Location Info",
            NULL::TEXT AS "Invoice Number",
            'risk_score: ' || trs.risk_score::TEXT AS "Additional Detail",
            trs.tt_number::TEXT AS "vehicle_identifier"
        FROM tt_risk_score trs
        JOIN master_data md ON trs.tt_number ILIKE md.truck_no
        WHERE trs.tt_number ILIKE '{vehicle_id}' -- Use exact ILIKE

        UNION ALL

        -- Transporter Risk (from transporter_risk_score)
        SELECT
            'transporter_risk_score'::TEXT AS "Source Table",
            'Transporter Risk'::TEXT AS "Data Table",
            'Transporter Risk Score: ' || COALESCE(trs.risk_score::TEXT, 'N/A') AS "Event Type",
            CURRENT_TIMESTAMP AS "Event Time",
            md.master_location::TEXT AS "Location Info",
            NULL::TEXT AS "Invoice Number",
            'transporter_code: ' || COALESCE(trs.transporter_code::TEXT, 'N/A') || ', risk_score: ' || COALESCE(trs.risk_score::TEXT, 'N/A') AS "Additional Detail",
            md.truck_no::TEXT AS "vehicle_identifier"
        FROM master_data md
        LEFT JOIN transporter_risk_score trs ON trs.transporter_code = md.master_transporter_code
    ) AS all_data
    ORDER BY "Event Time" DESC
    LIMIT 100;
    """,



    
    "compare_vehicle_risk_scores": """
    SELECT
        'tt_risk_score' AS "Data Table",
        tt_number,
        risk_score,
        transporter_name
    FROM tt_risk_score
    WHERE tt_number ILIKE ANY(ARRAY[:vehicle_ids])
    ORDER BY risk_score DESC;
    """,

    "blocked_vehicles_duration": """
    SELECT
        'alerts' AS "Data Table",
        a.vehicle_number,
        (CURRENT_DATE - a.vehicle_blocked_start_date::date) AS days_blocked,
        labb.last_alert_time,
        array_to_string(labb.last_violation_type, ', ') AS last_violation_type
    FROM alerts a
    LEFT JOIN (
        SELECT DISTINCT ON (a_inner.vehicle_number)
            a_inner.vehicle_number,
            vah.vts_end_datetime AS last_alert_time,
            vah.violation_type AS last_violation_type
        FROM alerts a_inner
        JOIN vts_alert_history vah ON a_inner.vehicle_number = vah.tl_number
        WHERE a_inner.vehicle_blocked_start_date IS NOT NULL
          AND vah.vts_end_datetime < a_inner.vehicle_blocked_start_date
        ORDER BY a_inner.vehicle_number, vah.vts_end_datetime DESC
    ) labb ON a.vehicle_number = labb.vehicle_number
    WHERE a.vehicle_blocked_start_date IS NOT NULL
      AND a.vehicle_unblocked_date IS NULL
      AND a.alert_status = 'Open' AND a.alert_section = 'VTS'
      AND (CURRENT_DATE - a.vehicle_blocked_start_date::date) > :days;
    """,

    "no_data_reporting": """
    SELECT 
        'vts_truck_master' AS "Data Table",
        vtm.truck_no,
        vtm.transporter_name,
        vtm.transporter_code,
        MAX(vah.vts_end_datetime) as last_reported_time
    FROM vts_truck_master vtm
    LEFT JOIN vts_alert_history vah ON vtm.truck_no = vah.tl_number
    WHERE vtm.whether_truck_blacklisted IS FALSE 
      AND (vah.vts_end_datetime < CURRENT_DATE - INTERVAL '{days} days' OR vah.vts_end_datetime IS NULL)
    GROUP BY vtm.truck_no, vtm.transporter_name, vtm.transporter_code
    ORDER BY last_reported_time ASC NULLS FIRST;
    """
}

# =============================================================================================
# LEGACY DATA (To be phased out or integrated into the new structure)
# The following data is from the previous implementation. It should be reviewed and either
# migrated into the new structured format (CONCEPTS, TEMPLATES, etc.) or removed.
# For now, it's kept for reference.
# =============================================================================================

def get_comprehensive_query_for_vehicle(vehicle_id: str) -> str:
    """DEPRECATED: Use get_comprehensive_query_for_training instead."""
    return get_comprehensive_query_for_training(vehicle_id=vehicle_id)
 
def get_comprehensive_query_for_training(vehicle_id=None, rules=None, query_type=None, interval_sql=None, specific_date=None, **kwargs) -> str:
    """
    Generates complex SQL queries for training data or runtime generation.
    Handles both vehicle timeline and performance improvement queries.
    """
    # Handle vehicle timeline queries
    if query_type == "vehicle_timeline" or (vehicle_id and not query_type):
        vid = vehicle_id or kwargs.get("vehicle_id")
        if vid:
            # Initialize empty filters
            time_filter_vah, time_filter_a, time_filter_vot, time_filter_tam = "", "", "", ""

            if specific_date:
                # Specific date filter takes precedence and is applied to each subquery
                time_filter_vah = f"AND vah.vts_end_datetime::DATE = '{specific_date}'"
                time_filter_a = f"AND a.created_at::DATE = '{specific_date}'"
                time_filter_vot = f"AND vot.created_at::DATE = '{specific_date}'"
                time_filter_tam = f"AND tam.createdat::DATE = '{specific_date}'"
            elif interval_sql:
                # Fallback to interval if no specific date
                time_filter_vah = f"AND vah.vts_end_datetime >= CURRENT_DATE - {interval_sql}"
                time_filter_a = f"AND a.created_at >= CURRENT_DATE - {interval_sql}"
                time_filter_vot = f"AND vot.created_at >= CURRENT_DATE - {interval_sql}"
                time_filter_tam = f"AND tam.createdat >= CURRENT_DATE - {interval_sql}"

            return QUERY_TEMPLATES["vehicle_timeline"].format(
                vehicle_id=vid,
                time_filter_vah=time_filter_vah,
                time_filter_a=time_filter_a,
                time_filter_vot=time_filter_vot,
                time_filter_tam=time_filter_tam
            )
            
    # Handle No Data Reporting (Anomaly Detection)
    if query_type == "no_data_reporting":
        days = kwargs.get("days", 7) # Default to 7 days if not specified
        return QUERY_TEMPLATES["no_data_reporting"].format(days=days)

    # Handle performance improvement queries
    if query_type == "performance_improvement":
        limit = kwargs.get("limit", 5)
        return f"""
WITH vehicle_performance AS (
    -- Step 1: Calculate violations per vehicle for the current and previous quarters
    SELECT
        vtm.transporter_name,
        vah.tl_number AS vehicle_number,
        SUM(CASE
            WHEN vah.vts_end_datetime >= DATE_TRUNC('quarter', CURRENT_DATE)
            THEN vah.stoppage_violations_count + vah.route_deviation_count + vah.speed_violation_count + vah.night_driving_count + vah.device_offline_count + vah.device_tamper_count + vah.continuous_driving_count + vah.no_halt_zone_count + vah.main_supply_removal_count
            ELSE 0
        END) AS current_quarter_violations,
        SUM(CASE
            WHEN vah.vts_end_datetime >= DATE_TRUNC('quarter', CURRENT_DATE) - INTERVAL '3 months' AND vah.vts_end_datetime < DATE_TRUNC('quarter', CURRENT_DATE)
            THEN vah.stoppage_violations_count + vah.route_deviation_count + vah.speed_violation_count + vah.night_driving_count + vah.device_offline_count + vah.device_tamper_count + vah.continuous_driving_count + vah.no_halt_zone_count + vah.main_supply_removal_count
            ELSE 0
        END) AS previous_quarter_violations
    FROM vts_alert_history vah
    JOIN vts_truck_master vtm ON vah.tl_number = vtm.truck_no
    WHERE vah.vts_end_datetime >= DATE_TRUNC('quarter', CURRENT_DATE) - INTERVAL '3 months'
    GROUP BY vtm.transporter_name, vah.tl_number
),
ranked_improvement AS (
    -- Step 2: Calculate improvement and rank vehicles within each transporter
    SELECT
        transporter_name,
        vehicle_number,
        previous_quarter_violations,
        current_quarter_violations,
        (previous_quarter_violations - current_quarter_violations) AS improvement,
        ROW_NUMBER() OVER(PARTITION BY transporter_name ORDER BY (previous_quarter_violations - current_quarter_violations) DESC) as rn
    FROM vehicle_performance
    WHERE previous_quarter_violations > current_quarter_violations
)
SELECT
    'performance_improvement' AS "Data Table",
    transporter_name,
    vehicle_number,
    previous_quarter_violations,
    current_quarter_violations,
    improvement
FROM ranked_improvement
WHERE rn <= {limit} -- Show top N improving vehicles per transporter
ORDER BY transporter_name, improvement DESC;
"""
    return ""

# =============================================================================================
# ===== NEW: INTELLIGENT RETRIEVAL & KEYWORD EXTRACTION =====
# =============================================================================================

# Common English stop words to be ignored during keyword extraction.
# This helps focus on the meaningful parts of the question for vector search.
STOP_WORDS = {
    'a', 'about', 'above', 'after', 'again', 'against', 'all', 'am', 'an', 'and', 'any', 'are', 'as', 'at',
    'be', 'because', 'been', 'before', 'being', 'below', 'between', 'both', 'but', 'by',
    'can', 'did', 'do', 'does', 'doing', 'down', 'during',
    'each', 'few', 'for', 'from', 'further',
    'had', 'has', 'have', 'having', 'he', 'her', 'here', 'hers', 'herself', 'him', 'himself', 'his', 'how',
    'i', 'if', 'in', 'into', 'is', 'it', 'its', 'itself',
    'just', 'me', 'more', 'most', 'my', 'myself',
    'no', 'nor', 'not', 'now', 'of', 'off', 'on', 'once', 'only', 'or', 'other', 'our', 'ours', 'ourselves', 'out', 'over', 'own',
    's', 'same', 'she', 'should', 'so', 'some', 'such',
    't', 'than', 'that', 'the', 'their', 'theirs', 'them', 'themselves', 'then', 'there', 'these', 'they', 'this', 'those', 'through', 'to', 'too',
    'under', 'until', 'up',
    'very',
    'was', 'we', 'were', 'what', 'when', 'where', 'which', 'while', 'who', 'whom', 'why', 'will', 'with',
    'you', 'your', 'yours', 'yourself', 'yourselves',
    'give', 'show', 'me', 'list', 'find', 'get', 'tell'
}

def extract_keywords(question: str) -> str:
    """
    Removes stop words and cleans the question to extract meaningful keywords.
    This creates a better search query for the vector database.
    
    Example: "what is the risk score for vehicle KA13AA2040" -> "risk score vehicle KA13AA2040"
    """
    # Convert to lowercase and remove punctuation
    question = question.lower()
    question = re.sub(r'[^\w\s]', '', question)
    
    # Tokenize and filter out stop words
    words = question.split()
    keywords = [word for word in words if word not in STOP_WORDS]
    
    return ' '.join(keywords)

def is_vehicle_id_query(question: str) -> Optional[str]:
    """
    Extracts a vehicle ID from the question if it contains keywords indicating a comprehensive report.
    Returns the vehicle ID if a match is found, otherwise None.
    This handles cases like "vehicle data of...", "summary for...", or just the vehicle number itself.
    
    Pattern: 2 letters, 2 digits, 1-2 letters, 4 digits. e.g., RJ19GD6553, HR46F0066
    """
    # Trim whitespace and quotes
    cleaned_question = question.strip().strip("'\"")
    
    # Regex to match common Indian vehicle number plates
    vehicle_id_pattern = r'\b([A-Z]{2}\d{1,2}[A-Z]{1,2}\d{4})\b'
    vehicle_match = re.search(vehicle_id_pattern, cleaned_question, re.IGNORECASE)

    if not vehicle_match:
        return None

    # Keywords that indicate a request for a comprehensive vehicle timeline.
    timeline_keywords = ['data', 'details', 'information', 'history', 'summary', 'report', 'everything', 'timeline', 'know about']
    
    # Trigger if the question is just the vehicle ID, or if it contains timeline keywords.
    if re.fullmatch(vehicle_id_pattern, cleaned_question, re.IGNORECASE) or any(keyword in cleaned_question.lower() for keyword in timeline_keywords):
        return vehicle_match.group(1)
    
    return None

def retrieve_similar_qa_pairs(question_embedding, qa_embeddings, qa_pairs, k=3):
    """
    Retrieves the top-k most similar QA pairs from the training data.
    
    NOTE: This is a conceptual function. You would replace this with a call
    to your actual vector database (e.g., FAISS, ChromaDB).
    """
    # In a real implementation, this would be a vector search.
    # For now, we'll just return the first k examples to show the flow.
    return qa_pairs[:k]

QUESTION_SYNONYMS = {
    # date formats
    r"\b(\d{4})-(\d{1,2})-(\d{1,2})\b": "\\1-\\2-\\3",
    r"\b(\d{1,2})/(\d{1,2})/(\d{4})\b": "\\3-\\2-\\1",
    r"\b(\d{1,2})-(\d{1,2})-(\d{4})\b": "\\3-\\2-\\1",

    # ===== NEW: PRODUCTION-GRADE DETERMINISTIC FIX =====
    # Forcefully replace "not blacklisted" with the correct SQL condition to prevent LLM errors.
    r"\b(are|is)?\s*not\s+blacklisted\b": "have whether_truck_blacklisted != 'Y'",
    # CRITICAL FIX: Prevent LLM confusion between 'blocked' and 'blacklisted'.
    # This deterministically maps the user's intent of "blocked" to the correct SQL columns in the 'alerts' table.
    r"\b(is|are)?\s*blocked\b": "have alert_status = 'Open' AND vehicle_unblocked_date IS NULL",

    # Normalize common time-based phrases for better caching and pattern matching
    r"\b(in|for|over)\s+the\s+last\b": "in the last",
    r"\b(in|for|during)\s+the\s+past\b": "in the last",
    r"\bthis\s+month\b": "in the last 1 month",
    r"\bthis\s+week\b": "in the last 7 days",
    r"\byesterday\b": "in the last 1 day",
    r"\bcompare\s+([\w\s]+)\s+with\s+([\w\s]+)\b": "comparison of \\1 and \\2",
    r"\bcompare\s+([\w\s]+)\s+and\s+([\w\s]+)\b": "comparison of \\1 and \\2",

    # Normalize various ways of asking for a vehicle report
    r"\b(details|data|information|history|summary|report)\s+(about|for)\b": "comprehensive report for",
    r"\bviolations\b": "total violations",
}

# =============================================================================================
# ===== PRODUCTION-GRADE COLUMN & CONCEPT SYNONYMS (SCHEMA-ALIGNED) =====
# =============================================================================================
# These synonyms map natural language terms to specific database columns or concepts.
# This is a critical layer for both RAG retrieval and LLM prompt accuracy.
COLUMN_SYNONYMS = {
    # --- Entity Identifiers ---
    r"\bvehicle number\b|\bvehicle no\b|\bvehicle id\b|\bvehicle\b|\btt number\b|\btl number\b|\btruck number\b|\btruck no\b": "vehicle_identifier",
    r"\binvoice\b|\binvoice no\b|\binvoice number\b|\binvoicenumber\b": "invoice_identifier",
    r"\btransporter id\b|\btransporter code\b": "transporter_code",
    r"\btransporter name\b|\btransporter\b|\bcarrier\b": "transporter_name",
    r"\bdriver name\b|\bdriver\b": "driver_name",
    r"\bsap id\b|\bsap_id\b": "sap_id",
    r"\btrip id\b": "trip_id",
    r"\block id\b": "lockid",

    # --- Location Concepts ---
    r"\blocation\b|\baddress\b|\bplace\b": "location_name",
    r"\bcurrent location\b|\bvehicle location\b": "vehicle_location",
    r"\bzone\b": "zone",
    r"\bregion\b": "region",
    r"\bstate\b": "state",

    # --- Status & State Concepts ---
    r"\balert status\b|\balert state\b|\bstatus\b": "alert_status",
    r"\bopen alerts?\b|\bpending alerts?\b|\bunresolved alerts?\b": "alert_status = 'OPEN'",
    r"alert_section": "alert_section = 'VTS'",
    r"\bclosed alerts?\b": "alert_status = 'Close'",
    r"\bblacklisted\b|\bblacklist status\b": "whether_truck_blacklisted",
    r"\bemlock trip\b": "isemlocktrip",

    # --- Violation & Alert Concepts (vts_alert_history) ---
    r"\bstoppage violations?\b|\bstoppage count\b": "stoppage_violations_count",
    r"\broute deviations?\b|\broute deviation count\b": "route_deviation_count",
    r"\bspeed violations?\b|\bspeeding\b|\bspeed violation count\b": "speed_violation_count",
    r"\bnight driving violations?\b|\bnight driving count\b": "night_driving_count",
    r"\bdevice offline violations?\b|\bdevice offline count\b|\bgps offline\b": "device_offline_count",
    r"\bdevice tamper violations?\b|\bdevice tampering\b|\bdevice tamper count\b": "device_tamper_count",
    r"\bcontinuous driving\b|\bfatigue driving\b|\bcontinuous driving count\b": "continuous_driving_count",
    r"\bpower disconnect\b|\bmain supply removal\b|\bpower cut\b|\bmain supply removal count\b": "main_supply_removal_count",
    r"\bno halt zone violations?\b|\bno halt zone\b|\bno halt zone count\b": "no_halt_zone_count",
    r"\bviolation types?\b|\btypes? of violations?\b|\bviolation categor(y|ies)\b": "violation_type",
    r"\bany violations?\b|\ball violations?\b": "any_violation",

    # --- Risk Score Concepts (tt_risk_score, transporter_risk_score) ---
    r"\brisk score\b|\brisky\b|\brisk\b": "risk_score",
    r"\bdevice removed risk\b": "device_removed",
    r"\bpower disconnect risk\b": "power_disconnect",
    r"\broute deviation risk\b": "route_deviation",
    r"\bstoppage violation risk\b": "stoppage_violation",

    # --- Trip Audit Concepts (vts_tripauditmaster) ---
    r"\bswipe in failure\b|\bswipe-in failure\b": "swipeinl_failed", # Logical concept
    r"\bswipe out failure\b|\bswipe-out failure\b": "swipeoutl_failed", # Logical concept

    # --- Alert Concepts (alerts table) ---
    r"\bseverity\b|\balert level\b": "severity",
    r"\bhigh severity\b": "severity = 'HIGH'",
    r"\bcritical severity\b": "severity = 'CRITICAL'",
    r"\balert category\b": "alert_category",
    r"\broot cause\b|\brca\b": "rca",

    # --- General Actions & Descriptors ---
    r"\bhow many\b|\bcount of\b|\btotal number of\b": "count",
    r"\baverage\b|\bavg\b": "avg",
    r"\btop\s*(\d+)?\b|\bhighest\s*(\d+)?\b|\bmost\b": "top_ranking",
    r"\bbottom\s*(\d+)?\b|\blowest\s*(\d+)?\b|\bleast\b": "bottom_ranking",
    r"\bmore than\b|\bgreater than\b|\babove\b": "greater_than",
    r"\bless than\b|\bsmaller than\b|\bbelow\b": "less_than",
    r"\bcompare\b|\bvs\b|\bversus\b": "comparison",
    r"\btrend\b|\bover time\b": "time_series",
}

# Patterns that replace keywords with full SQL snippets.
# These are intentionally separated and should be used cautiously.
SQL_REPLACEMENT_PATTERNS = {
    r"\btotal violations\b": "stoppage_violations_count + route_deviation_count + speed_violation_count + night_driving_count + device_offline_count + device_tamper_count + continuous_driving_count + no_halt_zone_count + main_supply_removal_count",
    r"\ball violations\b": "stoppage_violations_count + route_deviation_count + speed_violation_count + night_driving_count + device_offline_count + device_tamper_count + continuous_driving_count + no_halt_zone_count + main_supply_removal_count",
    r"\bviolation count\b": "stoppage_violations_count + route_deviation_count + speed_violation_count + night_driving_count + device_offline_count + device_tamper_count + continuous_driving_count + no_halt_zone_count + main_supply_removal_count",
}


# ===== NEW: CONCEPT-BASED TABLE DETECTION LOGIC =====
# This is a more robust way to identify necessary tables for complex queries.
concept_patterns = {
    "risk": [r"\brisk\b", r"\brisky\b", r"\brisk score\b"], # Matches risk concepts
    "violation": [r"\bviolation\b", r"\bevent\b", r"\bincident\b"], # General violation term
    "alert": [r"\balert\b"], # Specifically for the 'alerts' table (open/closed status)
    "transporter": [r"\btransporter\b", r"\bcarrier\b"], # Matches transporter entities
    "driver": [r"\bdriver\b"], # Matches driver entities
    "trip": [r"\btrip\b", r"\bjourney\b"], # General trip concept
    "live_status": [r"\bcurrent\b", r"\bongoing\b", r"\blive\b", r"\bwhere is\b", r"\bright now\b"], # For live data
    "audit": [r"\baudit\b", r"\bswipe\b", r"\bemlock\b", r"\block\b"], # For trip audit data
    "geospatial": [r"\blocation\b", r"\bzone\b", r"\bregion\b", r"\bhotspot\b", r"\barea\b"], # Geospatial concepts
    "performance": [r"\bperformance\b", r"\bimprov(e|ing)\b", r"\btrend\b", r"\bcomparison\b", r"\brate\b", r"\befficiency\b"], # Performance concepts
    "device_health": [r"device", r"tamper", r"offline", r"power", r"supply"], # For device-related violations
    "driver_behavior": [r"speeding", r"speed", r"stoppage", r"fatigue", r"continuous driving", r"night driving"], # For driver actions
    "route_compliance": [r"deviation", r"route", r"stoppage", r"halt"], # For adherence to planned routes
    "time_analysis": [r"daily", r"weekly", r"monthly", r"hourly", r"trend", r"over time", r"history of", r"timeline of", r"seasonality", "by month", "by day"], # For time-series analysis
    "data_quality": [r"missing", r"incorrect", r"invalid", r"orphan", r"mismatch"], # For data audit questions,
    "master_data_filter": [r"\b(not\s+marked\s+as\s+)?blacklisted\b", r"\bownership\b", r"\btransporter\b"],
    "no_data_anomaly": [r"not reported any data", r"no data from", r"silent vehicles"],
}

concept_to_table_map = {
    "risk": "tt_risk_score", # Default risk table is for vehicles
    "violation": "vts_alert_history", # Default violation table
    "alert": "alerts", # Explicitly map 'alert' to the alerts table
    "trip": "vts_ongoing_trips", # Default trip table is for ongoing trips
    "live_status": "vts_ongoing_trips", # Live status comes from ongoing trips
    "audit": "vts_tripauditmaster", # Audit concepts map to the audit master
    "performance": "vts_alert_history", # Performance is usually measured by historical violations/events
    "device_health": "vts_alert_history", # Device health violations are in alert history
    "driver_behavior": "vts_alert_history", # Driver behavior violations are in alert history
    "route_compliance": "vts_alert_history", # Route compliance violations are in alert history
    "time_analysis": "vts_alert_history", # Time-based analysis usually uses the history table
    "master_data_filter": "vts_truck_master",
    "no_data_anomaly": "vts_truck_master",
    "sap_filter": "vts_truck_master", # Explicit mapping for sap_id
    "schema_inquiry": "information_schema.columns", # Special case
    # 'transporter', 'driver', 'geospatial', 'data_quality' are contextual and primarily used to trigger joins.
}

concept_combination_rules = {
    # Core Join Logic
    frozenset(["risk", "transporter"]): "transporter_risk_score", # If risk + transporter, MUST use transporter_risk_score
    frozenset(["violation", "transporter"]): "vts_truck_master", # To link violations to a transporter name
    frozenset(["violation", "driver"]): "vts_truck_master", # To link violations to a driver name (via truck master)
    frozenset(["alert", "transporter"]): "vts_truck_master", # To link open alerts to a transporter name
    frozenset(["live_status", "risk"]): "tt_risk_score", # To get risk for a live vehicle
    frozenset(["audit", "driver"]): "vts_ongoing_trips", # To link audit events to a driver (via invoice join, as driver is only in ongoing)
    frozenset(["audit", "transporter"]): "vts_truck_master", # To link audit events to a transporter

    # Advanced Analytical Joins
    frozenset(["performance", "transporter"]): "vts_truck_master", # To analyze performance by transporter
    frozenset(["performance", "driver"]): "vts_truck_master", # To analyze performance by driver
    frozenset(["driver_behavior", "driver"]): "vts_truck_master", # To analyze a specific driver's behavior
    frozenset(["device_health", "risk"]): "tt_risk_score", # To correlate device events with vehicle risk
    frozenset(["geospatial", "risk"]): "tt_risk_score", # To find risk by location (e.g., riskiest zones)
    frozenset(["sap_filter", "risk"]): "vts_truck_master", # CRITICAL: sap_id is in master, so must join with master for risk
    frozenset(["geospatial", "violation"]): "vts_alert_history", # To analyze violations by location
    frozenset(["data_quality", "trip"]): "vts_ongoing_trips", # To audit trip data
    frozenset(["data_quality", "alert"]): "alerts", # To audit alert data
}


SQL_RULES = {
    "table_configurations": {
        "vts_alert_history": {
            "vehicle_id_column": "tl_number",
            "zone_column": "zone",
            "location_column": "location_name",
            "transporter_column": None,
            "datetime_column": "vts_end_datetime",
            "column_categories": {
                "violations": [
                    "stoppage_violations_count",
                    "route_deviation_count",
                    "speed_violation_count",
                    "main_supply_removal_count",
                    "night_driving_count",
                    "no_halt_zone_count",
                    "device_offline_count",
                    "device_tamper_count",
                    "continuous_driving_count"
                ]
            },
            "fallback_datetime_column": "created_at",
            "display_name": "Compliance",
            "stoppage": "stoppage_violations_count",
            "route_deviation": "route_deviation_count",
            "speed": "speed_violation_count",
            "night driving": "night_driving_count",
            "device offline": "device_offline_count",
            "device tamper": "device_tamper_count",
            "continuous driving": "continuous_driving_count",
            "no halt": "no_halt_zone_count",
            "power disconnect": "main_supply_removal_count",
            "main_supply": "main_supply_removal_count",
            "filters": ["(violation_type IS NOT NULL AND array_length(violation_type, 1) > 0)"]
        },
        "alerts": {
            "vehicle_id_column": "vehicle_number",
            "alt_vehicle_id_column": "tt_load_number",
            "zone_column": "zone",
            "location_column": "location_name",
            "transporter_column": "transporter_name",
            "datetime_column": "created_at",
            "display_name": "VTS Dashboard",
            "filters": ["alert_section = 'VTS'", "alert_status = 'Open'", "violation_type IS NOT NULL"]
        },
        "vts_ongoing_trips": {
            "vehicle_id_column": "tt_number",
            "zone_column": "zone",
            "location_column": "location_name",
            "transporter_column": "transporter_name",
            "datetime_column": "created_at",
            "display_name": "VTS Live",
            "filters": ["violation_type IS NOT NULL"]
        },
        "vts_tripauditmaster": {
            "vehicle_id_column": "trucknumber",
            "zone_column": "zone",
            "location_column": "location_name",
            "transporter_column": "transporter_name",
            "datetime_column": "createdat",
            "display_name": "Trip Audit",
            "filters": ["(swipeoutL1 = 'False' OR swipeoutL2 = 'False')"]
        },
        "vts_truck_master": {
            "vehicle_id_column": "truck_no",
            "zone_column": "zone",
            "location_column": "location_name", 
            "transporter_column": "transporter_name",
            "fallback_datetime_column": "last_changed_date"
        }
    },

    
    "valid_tables": ['vts_alert_history', 'vts_truck_master', 'vts_ongoing_trips', 'alerts', 'vts_tripauditmaster','tt_risk_score','transporter_risk_score'],
    "invalid_data_values": ['n/a', 'null', 'none', 'unknown'],
    # This provides keywords for each table, used as a fallback for table detection.
    # It's now more comprehensive and aligned with the full schema.
    "table_patterns": {
        "vts_alert_history": ["violation", "history", "past event", "stoppage", "deviation", "speed", "night driving", "offline", "tamper", "continuous driving", "supply removal", "no halt", "compliance", "report"],
        "vts_ongoing_trips": ["ongoing", "live", "current", "active", "journey", "where is", "right now", "tracking", "location", "driver", "eta", "delay"],
        "alerts": ["alert", "status", "Open", "Close", "pending", "severity", "critical", "high", "rca", "workflow", "dashboard", "notification"],
        "vts_truck_master": ["master", "transporter name", "truck no", "vehicle type", "ownership", "capacity", "compartments", "reference"],
        "vts_tripauditmaster": ["audit", "swipe", "emlock", "lock", "seal", "security", "check"],
        "tt_risk_score": ["vehicle risk", "risk score", "safety score", "driving score"],
        "transporter_risk_score": ["transporter risk", "carrier risk", "fleet risk"]
    },

    # Add the new concept-based rules to the main SQL_RULES dictionary
    "concept_patterns": {
        "risk": [r"\brisk\b", r"\brisky\b", r"\brisk score\b", r"\bhazard\b", r"\bdanger\b", r"\bsafety rating\b", r"\brisk profile\b"],
        "violation": [r"\bviolations?\b", r"\bevents?\b", r"\bincidents?\b", r"\boffenses?\b", r"\bbreach(?:es)?\b", r"\binfringements?\b", r"\bnon-compliance\b", r"\binfractions?\b"],
        "alert": [r"\balerts?\b", r"\balarms?\b", r"\bwarnings?\b", r"\bnotifications?\b", r"\btickets?\b", r"\bcases?\b", r"\bblocked\b"],
        "transporter": [r"\btransporters?\b", r"\bcarriers?\b", r"\bhaulers?\b", r"\boperators?\b", r"\bfleet owners?\b", r"\blogistics providers?\b", r"\bvendors?\b"],
        "driver": [r"\bdrivers?\b", r"\bpilots?\b", r"\bchauffeurs?\b"],
        "trip": [r"\btrips?\b", r"\bjourneys?\b", r"\broutes?\b", r"\bshipments?\b", r"\bloads?\b", r"\bconsignments?\b"],
        "live_status": [r"\bcurrent\b", r"\bongoing\b", r"\blive\b", r"\bwhere is\b", r"\bright now\b", r"\breal-time\b", r"\bactive\b", r"\blatest status\b", r"\bcurrent position\b"],
        "audit": [r"\baudits?\b", r"\bswipes?\b", r"\bemlocks?\b", r"\blocks?\b", r"\bseals?\b", r"\blockid\b", r"\bsecurity checks?\b"],
        "geospatial": [r"\blocations?\b", r"\bzones?\b", r"\bregions?\b", r"\bhotspots?\b", r"\bareas?\b", r"\bplaces?\b", r"\bcit(?:y|ies)\b", r"\bstates?\b", r"\bdistricts?\b", r"\bgeography\b"],
        "performance": [r"\bperformance\b", r"\bimprov(?:e|ing)\b", r"\btrend\b", r"\bcomparison\b", r"\brate\b", r"\befficiency\b", r"\bproductivity\b", r"\butiliz(?:e|ing|ation)\b", r"\bbenchmark\b"],
        "device_health": [r"device", r"tamper", r"offline", r"power", r"supply", r"battery", r"voltage", r"health", r"disconnect", r"gps status"],
        "driver_behavior": [r"speeding", r"speed", r"stoppage", r"fatigue", r"continuous driving", r"night driving", r"harsh", r"braking", r"acceleration", r"driving style"],
        "route_compliance": [r"deviation", r"route", r"stoppage", r"halt", r"detour", r"path", r"geofence"],
        "time_analysis": [r"daily", r"weekly", r"monthly", r"hourly", r"trend", r"over time", r"last \d+ days?", r"history", r"timeline", r"period", r"duration", r"seasonality"],
        "data_quality": [r"missing", r"incorrect", r"invalid", r"orphan", r"mismatch", r"null", r"empty", r"quality", r"integrity", r"completeness", r"discrepancy"],
        "sap_filter": [r"\bsap_id\b", r"\bsap id\b", r"\bsap\b", r"\bplant\b"],
        "schema_inquiry": [r"columns?", r"schema", r"structure", r"describe", r"datatypes?", r"types?", r"fields?", r"table definition", r"metadata"],
        "documentation_inquiry": [r"what is the purpose", r"explain", r"how is.*calculated", r"difference between", r"define", r"meaning of", r"what does.*mean"]
    },
    "concept_to_table_map": concept_to_table_map,
    "concept_combination_rules": concept_combination_rules,
    
    "table_column_mappings": {
        "vts_alert_history": {
            "vehicle_id": "tl_number",
            "vehicle": "tl_number",
            "vehicle_number": "tl_number",
            "truck_number": "tl_number",
            "truck": "tl_number",
            "zone": "zone", 
            "location": "location_name",
            "sap_id": "sap_id"  
        },
        "vts_truck_master": {
            "vehicle_id": "truck_no",
            "vehicle": "truck_no",
            "vehicle_number": "truck_no",
            "truck_number": "truck_no",
            "truck": "truck_no",
            "zone": "zone",
            "location": "location_name",
            "sap_id": "sap_id"  
        },
        "alerts": {
            "vehicle_id": "vehicle_number",
            "vehicle": "vehicle_number",
            "vehicle_number": "vehicle_number",
            "truck_number": "vehicle_number",
            "truck": "vehicle_number",
            "zone": "zone",
            "location": "location_name",
            "sap_id": "sap_id"  
        },
        "vts_ongoing_trips": {
            "vehicle_id": "tt_number",
            "vehicle_number": "tt_number",
            "vehicle": "tt_number",
            "truck_number": "tt_number",
            "truck": "tt_number",
            "zone": "zone",
            "sap_id": "sap_id",
            "transporter_id": "transporter_code"
        },
        "vts_tripauditmaster": {
            "vehicle_id": "trucknumber",
            "vehicle": "trucknumber",
            "vehicle_number": "trucknumber",
            "truck_number": "trucknumber",
            "truck": "trucknumber",
            "zone": "zone",
            "location": "location_name",
            "sap_id": "sap_id"
        },
        "tt_risk_score": {
            "vehicle_id": "tt_number",
            "transporter_code": "transporter_code"
        },
        "transporter_risk_score": {
            "transporter_code": "transporter_code"
        },
    },
    

    "cross_table_patterns": {
        "zone": ["zone", "zones", "geographic"],
        "region": ["region", "regions", "area", "territory"],
        "all_tables": ["all tables", "every table", "across tables", "each table"],
    },
   
    "union_templates": {
        "zone_across_tables": """
            SELECT 'vts_alert_history' AS "Data Table", zone FROM vts_alert_history WHERE zone IS NOT NULL
            UNION SELECT 'vts_truck_master' AS "Data Table", zone FROM vts_truck_master WHERE zone IS NOT NULL
            UNION SELECT 'vts_ongoing_trips' AS "Data Table", zone FROM vts_ongoing_trips WHERE zone IS NOT NULL
        """,
        "region_across_tables": """
            SELECT 'vts_alert_history' AS "Data Table", region FROM vts_alert_history WHERE region IS NOT NULL
            UNION SELECT 'vts_truck_master' AS "Data Table", region FROM vts_truck_master WHERE region IS NOT NULL
            UNION SELECT 'vts_ongoing_trips' AS "Data Table", region FROM vts_ongoing_trips WHERE region IS NOT NULL
        """
    },



    "minimal_schema": """
-- ACTUAL SCHEMA BASED ON YOUR DATABASE
CREATE TABLE alerts ( -- Central table for all system alerts that may require user action.
    bu character varying, 
    sap_id character varying,
    sop_id character varying,
    location_name character varying,
    severity character varying, 
    alert_category character varying,
    alert_status character varying,
    alert_state character varying,
    unique_id character varying,
    alert_section character varying,
    external_id character varying,
    interlock_name character varying,
    interlock_id character varying,
    device_id character varying,
    equipment_id character varying,
    sensor_id character varying,
    device_type character varying,
    equipment_type character varying,
    device_name character varying,
    equipment_name character varying,
    device_msg character varying,
    vehicle_number character varying,  -- Vehicle ID in this table
    violation_type character varying,
    clear_count boolean,
    maintenance_time character varying,
    tt_load_number character varying,  -- Alternative vehicle ID
    is_flagged_false boolean,
    rca character varying,
    rca_type character varying,
    alert_history jsonb,
    last_sms_to text[],
    last_mailed_to text[],
    last_escalated_to text[],
    last_notified_to text[],
    assigned_to character varying,
    assigned_to_role character varying,
    assigned_users text[],
    assigned_user_roles text[],
    district character varying,
    zone character varying,
    region character varying,
    state character varying,
    city character varying,
    sales_area character varying,
    raw_data jsonb,
    r1_time timestamp without time zone,
    r2_time timestamp without time zone,
    r3_time timestamp without time zone,
    indent_status character varying,
    product_code character varying,
    indent_no character varying,
    dealer_id character varying,
    workflow_instance_id character varying,
    workflow_datetime timestamp,
    terminal_plant_id character varying,
    terminal_plant_name character varying,
    servicing_plant_id character varying,
    servicing_plant_name character varying,
    progress_rate integer,
    category character varying,
    indent_raised_date timestamp without time zone,
    dry_out_in_days character varying,
    origin_altid character varying,
    alert_message character varying,
    external_timestamp timestamp without time zone,
    atg_ack boolean,
    emlock_ack boolean,
    vts_return boolean,
    atg_ack_time timestamp without time zone,
    transporter_name character varying,
    transporter_code character varying,
    vehicle_blocked_start_date timestamp with time zone,
    vehicle_blocked_end_date timestamp with time zone,
    mark_as_false boolean,
    id bigint,
    created_at timestamp,
    updated_at timestamp,
    entity_id character varying,
    closed_at timestamp,
    cause_effect character varying,
    workflow_url character varying,
    workflow_port character varying,
    tas_device_name character varying,
    terminal_loc_code character varying,
    vts_alert_history_ids text[],
    dry_out_start_time timestamp without time zone,
    dry_out_end_time timestamp without time zone,
    intra_day_dry_out_start_time timestamp without time zone,
    intra_day_dry_out_end_time timestamp without time zone,
    vehicle_unblocked_date timestamp without time zone,
    action_on character varying,
    ro_offline boolean,
    temporary_close boolean,
    permanent_close boolean
);

CREATE TABLE vts_alert_history ( -- Historical log of all violations and alerts.
    vendor_id character varying,
    location_id character varying,
    location_type character varying,
    tl_number character varying,
    report_duration character varying,
    vts_start_datetime timestamp without time zone, 
    vts_end_datetime timestamp without time zone,
    total_trips integer,
    stoppage_violations_count integer,
    route_deviation_count integer, 
    speed_violation_count integer,
    main_supply_removal_count integer,
    night_driving_count integer,
    no_halt_zone_count integer,
    device_offline_count integer,
    device_tamper_count integer,
    continuous_driving_count integer,
    approved_by character varying,
    auto_unblock boolean,
    alert_id character varying,
    invoice_number character varying,
    tt_type character varying,
    violation_type text[],
    id bigint,
    created_at timestamp without time zone,
    updated_at timestamp without time zone,
    entity_id character varying,
    scheduled_trip_start_datetime timestamp without time zone,
    scheduled_trip_end_datetime timestamp without time zone,
    region character varying,
    bu character varying,
    zone character varying,
    sap_id character varying,
    location_name character varying,
    route_deviation_count_orig integer
);

CREATE TABLE vts_ongoing_trips ( -- Live tracking of trips that are currently in progress.
    violation_type character varying,
    event_start_datetime timestamp with time zone,
    event_end_datetime timestamp with time zone,
    sap_id character varying,
    region character varying,
    zone character varying,
    destination_code character varying,
    location_type character varying,
    tt_type character varying,
    tt_number character varying,
    trip_id character varying,
    transporter_id character varying,
    transporter_name character varying,
    invoice_no character varying,
    load_no character varying,
    route_no character varying,
    driver_name character varying,
    scheduled_start_datetime timestamp with time zone,
    scheduled_end_datetime timestamp with time zone,
    actual_trip_start_datetime timestamp with time zone,
    actual_end_start_datetime timestamp with time zone,
    vehicle_latitude numeric,
    vehicle_longitude numeric,
    vehicle_location character varying,
    id bigint,
    created_at timestamp without time zone,
    updated_at timestamp without time zone,
    entity_id character varying,
    bu character varying
);

CREATE TABLE vts_truck_master ( -- Master data for all trucks, contains definitive details like transporter, home zone, etc.
    seq_key bigint,
    ukid bigint,
    mandt text,
    truck_no text,  -- Vehicle ID in this table
    sap_id text,
    vehicle_type text,
    vehicle_type_desc text,
    transporter_code text,
    customer_code text,
    ownership_type text,
    weight_uom text,
    volume_uom text,
    capacity_of_the_truck double precision,
    empty_weight double precision,
    filled_weight double precision,
    whether_truck_blacklisted text,
    no_of_compartments bigint,
    syncdt timestamp,
    last_changed_date timestamp,
    clsgrp text,
    load_dt timestamp,
    load_ts timestamp,
    create_by text,
    transporter_name text,
    location_name text,
    bu text,
    region text,
    zone text
);

CREATE TABLE vts_tripauditmaster ( -- Audit trail for trips, especially for emlock and swipe in/out events.
    id bigint,
    sap_id text,
    trucknumber text,
    lockid1 text,
    lockid2 text,
    loadnumber text,
    invoicenumber text,
    isemlocktrip text,
    swipeinl1 text, -- 'True' or 'False'
    swipeinl2 text, -- 'True' or 'False'
    swipeoutl1 text, -- 'True' or 'False' for failure
    swipeoutl2 text, -- 'True' or 'False' for failure
    createdat timestamp without time zone,
    bu text,
    location_name text,
    zone text,
    region text
);

CREATE TABLE tt_risk_score ( -- Risk scores for each vehicle (tt_number).
    tt_number character varying,
    transporter_code character varying,
    transporter_name character varying,
    total_trips integer,
    risk_score double precision,
    device_removed double precision,
    power_disconnect double precision,
    route_deviation double precision,
    stoppage_violation double precision
);

CREATE TABLE transporter_risk_score ( -- Aggregated risk scores for each transporter. 
    transporter_code character varying, 
    transporter_name character varying,
    total_trips integer,
    device_removed double precision,
    power_disconnect double precision,
    route_deviation double precision,
    stoppage_violation double precision,
    risk_score double precision
);

""",

"column_types": {
        "sap_id": "text",
        "TT_NUMBER": "text", 
        "tl_number": "text",
        "truck_no": "text",
        "vehicle_number": "text",
        "invoice_number": "text",
        "invoicenumber": "text",
        "zone": "text",
        "region": "text",
    },

    "business_rules": """
CRITICAL SCHEMA RULES - BASED ON ACTUAL DATABASE:

===== INTENT DISAMBIGUATION (Schema vs. Data vs. Documentation) =====
- **Schema Inquiry**: If the user asks for "columns", "schema", "structure", "fields", or "describe table", the query is about the database's design. The SQL should query `information_schema.columns`. This is a high-priority rule.
  - Example: "what are the columns in alerts" -> SELECT column_name, data_type FROM information_schema.columns WHERE table_name = 'alerts';
- **Data Inquiry**: If the user asks for the data *itself* (e.g., "what are the alerts", "show me alerts"), the query is about the data within the tables. The SQL should query the actual table (e.g., `alerts`).
  - Example: "what are the alerts" -> SELECT * FROM alerts WHERE alert_section = 'VTS' LIMIT 10;
- **Documentation Inquiry**: If the user asks "what is", "explain", "how is", or "what is the purpose of", they are likely asking for a definition, not data. The system should provide a textual answer instead of generating SQL. This is handled by a 'no-SQL' mode.

===== JOIN RULES (How to connect tables) =====
- **CRITICAL**: The `vts_truck_master` table is for master data. Use it in `JOIN` clauses to get details like `transporter_name` for a vehicle found in another table (e.g., `vts_alert_history`). AVOID using it as the main `FROM` table unless the query is specifically about master data itself (e.g., 'list all transporters').
- To get region, zone, or transporter_name for a vehicle, JOIN with `vts_truck_master`.
- `vts_truck_master` is the master table for vehicle details.
- JOIN `tt_risk_score` and `vts_truck_master` ON `tt_risk_score.tt_number` = `vts_truck_master.truck_no`.
- JOIN `vts_alert_history` and `vts_truck_master` ON `vts_alert_history.tl_number` = `vts_truck_master.truck_no`.
- JOIN `alerts` and `vts_truck_master` ON `alerts.vehicle_number` = `vts_truck_master.truck_no`.
- JOIN `vts_ongoing_trips` and `vts_truck_master` ON `vts_ongoing_trips.tt_number` = `vts_truck_master.truck_no`.
-Take violation_type from vts_alert_history when use ask violations.
- To get vehicle risk scores, JOIN `tt_risk_score` ON `tt_risk_score.tt_number` = `[table].vehicle_id`.
- **CRITICAL**: For "risk score of a vehicle" or "vehicles with a risk score", you MUST use the `tt_risk_score` table. For "risk score of a transporter", you MUST use the `transporter_risk_score` table. DO NOT confuse them. `transporter_risk_score` does NOT contain `tt_number`.
- **CRITICAL**: When joining multiple tables, ALWAYS prefix column names with their table alias (e.g., `trs.risk_score`, `vtm.transporter_name`) to avoid ambiguous column errors. This is especially important for columns like `risk_score` or `total_trips` that exist in multiple tables.
- **CRITICAL**: A query for "vehicles with transporter risk score" requires joining `vts_truck_master` with `transporter_risk_score`.
  - CORRECT: `... FROM vts_truck_master vtm JOIN transporter_risk_score trs ON vtm.transporter_code = trs.transporter_code WHERE trs.risk_score > 70`
  - INCORRECT: `... FROM vts_truck_master vtm JOIN tt_risk_score trs ...` (This uses vehicle risk, not transporter risk).

- **CRITICAL NEGATIVE INTENT**: For questions containing "not generated", "no alerts", "without alerts", or "never", you MUST use an exclusion pattern like `NOT EXISTS` or `LEFT JOIN ... IS NULL`.
  - **PREFERRED PATTERN**: Use `NOT EXISTS`. Example: `FROM vts_truck_master vtm WHERE NOT EXISTS (SELECT 1 FROM alerts a WHERE a.vehicle_number = vtm.truck_no ...)`
  - **CRITICAL**: These queries MUST start from a master table (`vts_truck_master`) and check for non-existence in a transactional table (`alerts` or `vts_alert_history`).
  - **CRITICAL**: For "no alerts", you MUST check the `alerts` table using the `created_at` column. DO NOT use `vts_alert_history`.
  - **AVOID `NOT IN`** if possible, as it can be slow and behave unexpectedly with NULLs.

- **CRITICAL VIOLATION COUNTING**: When asked for "more than X violations" (e.g., "more than 5 speed violations"), you MUST use `SUM(violation_count_column) > X` in the `HAVING` clause. Do NOT use `COUNT(*)`.
  - **CORRECT PATTERN**:
    ```sql
    SELECT tl_number, SUM(speed_violation_count) as total_violations
    FROM vts_alert_history
    GROUP BY tl_number
    HAVING SUM(speed_violation_count) > 5;
    ```
  - **INCORRECT PATTERN**: `... GROUP BY tl_number HAVING COUNT(*) > 5;` (This counts records, not total violations).
  - This applies to all violation count columns in `vts_alert_history` like `speed_violation_count`, `stoppage_violations_count`, etc.

- **CRITICAL 4-TABLE JOIN EXAMPLE (Alerts + Risk + Violations + Master Data)**: To find open, high-severity alerts for high-risk transporters, you must join `alerts` -> `vts_truck_master` -> `transporter_risk_score`.
- **CRITICAL 3-TABLE JOIN EXAMPLE (Risk + Violations + Master Data)**: To find high-risk vehicles for a specific transporter with recent violations, you must join `vts_truck_master` (for transporter info), `tt_risk_score` (for risk score), and `vts_alert_history` (for violations).
  - `vts_truck_master vtm` JOIN `tt_risk_score trs` ON `vtm.truck_no = trs.tt_number`
  - JOIN `vts_alert_history vah` ON `vtm.truck_no = vah.tl_number`
- To get tt risk scores, use the `tt_risk_score` table of column `risk_score`.
- **CRITICAL NEGATIVE INTENT**: For questions containing "not generated", "no alerts", "without alerts", or "never", you MUST use an exclusion pattern like `NOT EXISTS` or `LEFT JOIN ... IS NULL`.
  - **PREFERRED PATTERN**: Use `NOT EXISTS`. Example: `FROM vts_truck_master vtm WHERE NOT EXISTS (SELECT 1 FROM alerts a WHERE a.vehicle_number = vtm.truck_no ...)`
  - **CRITICAL**: These queries MUST start from a master table (`vts_truck_master`) and check for non-existence in a transactional table (`alerts` or `vts_alert_history`).
  - **CRITICAL**: For "no alerts", you MUST check the `alerts` table using the `created_at` column. DO NOT use `vts_alert_history`.
  - **AVOID `NOT IN`** if possible, as it can be slow and behave unexpectedly with NULLs.
- **CRITICAL**: For "open alerts", "high severity alerts", etc., you MUST use the `alerts` table. Filter by `alert_status = 'OPEN'`, `severity = 'HIGH'`, and ALWAYS include `alert_section = 'VTS'`. DO NOT use `vts_alert_history` for this.
- **CRITICAL**: For "swipe out failure", you MUST use the `vts_tripauditmaster` table. Filter by `swipeoutl1 = 'False'` or `swipeoutl2 = 'False' means alerts or violation happened.
- **CRITICAL**: For time differences between scheduled and actual trip start, you MUST use `vts_ongoing_trips` and subtract `actual_trip_start_datetime` from `scheduled_start_datetime`.
- **CRITICAL**: To find vehicles with NO violations, you MUST use a `LEFT JOIN` from `vts_truck_master` to `vts_alert_history` and check for `IS NULL`.
- **CRITICAL**: For "power disconnect events", use the `main_supply_removal_count` column in `vts_alert_history`. The column `power_disconnect` exists in risk tables, but for violation events, use `main_supply_removal_count`.
- **CRITICAL**: To find events that happen sequentially (e.g., tamper then offline), you must use window functions like `LEAD()` or `LAG()` on a time-ordered subquery.
- **CRITICAL**: For "driver performance" or "violations per driver", you MUST join `vts_alert_history` with `vts_truck_master` on `vah.tl_number = vtm.truck_no` to get the `driver_name`.
- **CRITICAL**: For "improving performance", you must compare violation counts between two time periods (e.g., this quarter vs. last quarter) using CTEs.

# --- Array & Data Type Rules ---
- **CRITICAL ARRAY HANDLING**: To count distinct violation types from `vts_alert_history.violation_type` (which is a TEXT[] array), Example: `SELECT tl_number, COUNT(DISTINCT violation_type) FROM vts_alert_history GROUP BY tl_number`. Do NOT use `ARRAY_LENGTH(DISTINCT ...)`.
- **CRITICAL COLUMN DISTINCTION**: The `violation_type` column in `vts_alert_history` is a `text[]` ARRAY and can be unnested. The `violation_type` in `alerts` and `vts_ongoing_trips` is a single `character varying` TEXT field and CANNOT be unnested.

===== CROSS-TABLE LOOKUP RULES (How to find data that could be anywhere) =====
- **To find an INVOICE NUMBER for a VEHICLE**: You MUST search across multiple tables. The vehicle ID could be `tl_number`, `tt_number`, `truck_no`, etc. The invoice column could be `invoice_number`, `invoicenumber`, or `invoice_no`.
  - **Example Pattern**:
    SELECT 'vts_alert_history' AS "Source", invoice_number FROM vts_alert_history WHERE tl_number = 'VEHICLE_ID'
    UNION ALL
    SELECT 'vts_ongoing_trips' AS "Source", invoice_no FROM vts_ongoing_trips WHERE tt_number = 'VEHICLE_ID'
    UNION ALL
    SELECT 'vts_tripauditmaster' AS "Source", invoicenumber FROM vts_tripauditmaster WHERE trucknumber = 'VEHICLE_ID'

===== QUERY TYPE DISAMBIGUATION =====
- "risk score of transporter X" -> USE transporter_risk_score table
- "risk score of vehicle X" -> USE tt_risk_score table
- "current status of vehicle X" -> USE vts_ongoing_trips (NOT vts_truck_master)
- "where is vehicle X" -> USE vts_ongoing_trips for live location
- "violations for vehicle X" -> USE vts_alert_history

===== DATE RANGE QUERIES =====
- Always use BETWEEN for date ranges: WHERE date BETWEEN 'start' AND 'end'
- For "last X months/days": CURRENT_DATE - INTERVAL 'X months/days'
- For "this month": vts_end_datetime >= DATE_TRUNC('month', CURRENT_DATE)
- For "last month": vts_end_datetime >= DATE_TRUNC('month', CURRENT_DATE - INTERVAL '1 month') 
                    AND vts_end_datetime < DATE_TRUNC('month', CURRENT_DATE)

===== AGGREGATION PATTERNS =====
- "average per vehicle" -> AVG(...) with proper grouping
- "percentage of" -> ROUND(100.0 * COUNT(CASE...) / NULLIF(COUNT(*), 0), 2)
- "how many times" -> COUNT(*) with GROUP BY
- "trend over time" -> DATE_TRUNC('month/week/day', datetime) with GROUP BY

===== CURRENT/LIVE DATA =====
- Use vts_ongoing_trips for: current location, current status, active trips
- Order by created_at DESC and LIMIT 1 for "current" queries
- Use ILIKE for case-insensitive vehicle ID matching
-ALWAYS include alert_section = 'VTS' for alerts table queries

===== VEHICLE IDENTIFIER MAPPINGS (CORRECT) =====
- vts_alert_history -> tl_number (character varying)  
- vts_ongoing_trips -> tt_number (character varying)
- alerts -> vehicle_number (character varying)
- vts_truck_master -> truck_no (text)
- vts_tripauditmaster -> trucknumber (text) 


===== TABLE-SPECIFIC FACTS =====
- vts_alert_history.violation_type is TEXT[] (array)
- location_sensitivity table DOES NOT EXIST - this is a hallucination
- "emlock trip" is in vts_tripauditmaster, column isemlocktrip ('Y' or 'N').
- "swipe out" failure is in vts_tripauditmaster, columns swipeoutl1 or swipeoutl2 ('False') means alerts or violation happened.
- `vts_alert_history` DOES NOT have a `violation_severity` column. Severity is ONLY in the `alerts` table.
-"ALWAYS include alert_section = 'VTS' for alerts table queries"
- `vts_alert_history` DOES NOT have a `swipe_out_failure` column. Swipe data is ONLY in `vts_tripauditmaster`. 
-tt_risk_score table has a `risk_score` column.
-transporter_risk_score table has a `risk_score` column.


===== JOINING & DATA FALLBACK =====
- location_name and location_sensitivity are DIFFERENT columns in DIFFERENT tables

For violation counts:
SELECT tl_number, SUM(stoppage_violations_count) as total_stoppage
FROM vts_alert_history 
WHERE tl_number = 'VEHICLE_ID'
GROUP BY tl_number

===== TABLES THAT DON'T EXIST =====
- location_name_sensitivity - HALLUCINATION
- location_sensitivity_table - HALLUCINATION  
- blacklisted - HALLUCINATION
- Any table not listed in the actual schema above
""",

    "column_mapping_guide": """
ACTUAL COLUMN MAPPINGS:

"vehicle number" -> tt_number (vts_ongoing_trips)
"vehicle number" -> tl_number (vts_alert_history) 
"vehicle number" -> vehicle_number (alerts)
"vehicle number" -> truck_no (vts_truck_master)
"vehicle number" -> trucknumber (vts_tripauditmaster)

"stoppage violation count" -> stoppage_violations_count (vts_alert_history)
"route deviation count" -> route_deviation_count (vts_alert_history)
"speed violation count" -> speed_violation_count (vts_alert_history)
"main supply removal count" -> main_supply_removal_count (vts_alert_history)
"night driving count" -> night_driving_count (vts_alert_history)
"continuous driving count" -> continuous_driving_count (vts_alert_history)
"device tamper count" -> device_tamper_count (vts_alert_history) 
"power disconnect count" -> main_supply_removal_count (vts_alert_history)
"no halt zone count" -> no_halt_zone_count (vts_alert_history)
"device_removed" -> device_offline_count (vts_alert_history) -- Clarification: This maps to device_offline_count

"device_removed" -> device_removed(tt_risk_score)
"power_disconnect" -> power_disconnect(tt_risk_score) 
"route_deviation" -> route_deviation(tt_risk_score)
"stoppage_violation" -> stoppage_violation(tt_risk_score)

"device_removed " -> device_removed(transporter_risk_score)
"power_disconnect" -> power_disconnect(transporter_risk_score) 
"route_deviation" -> route_deviation(transporter_risk_score)
"stoppage_violation" -> stoppage_violation(transporter_risk_score)
"blackelisted trucks" -> whether_truck_blacklisted (vts_truck_master)
"blacklisted" -> whether_truck_blacklisted (vts_truck_master)


RISK SCORES (tt_risk_score):
"risk score" -> risk_score

RISK SCORES (transporter_risk_score):
"risk score" -> risk_score

LOCATION FIELDS:
"location name" -> location_name (vts_alert_history, alerts, vts_truck_master)
"vehicle location" -> vehicle_location (vts_ongoing_trips)
"location" -> location_name (most tables) 
""",

    "general_rules": """
1. MANDATORY: When joining multiple tables, ALWAYS prefix column names with their table alias (e.g., `trs.risk_score`, `vtm.transporter_name`) to avoid ambiguous column errors.
2. NEVER write 'SELECT risk_score' in multi-table queries; always use an alias like 'SELECT trs.risk_score'.
3. ALWAYS use the correct vehicle identifier for each table
4. For vts_alert_history: use tl_number
5. For vts_ongoing_trips: use TT_NUMBER
6. Always include 'Data Table' column in SELECT
7. Use ILIKE for case-insensitive matching
8. Cast dates properly with ::date
1. ALWAYS use the correct vehicle identifier for each table
3. For vts_alert_history: use tl_number
4. For vts_ongoing_trips: use TT_NUMBER
5. Always include 'Data Table' column in SELECT
6. Use ILIKE for case-insensitive matching
7. Cast dates properly with ::date
8. Handle case variations in vehicle identifiers using UPPER() or ILIKE
9. For trip audits, emlock, or swipe data, use vts_tripauditmaster.
10. NEVER reference columns that don't exist in a table
11. Check schema carefully before using location columns
12. Take vts_end_datetime from vts_alert_history for date filtering
13. violations are in vts_alert_history table are route_deviation_count, stoppage_violations_count, speed_violation_count, main_supply_removal_count, night_driving_count, no_halt_zone_count, device_offline_count, device_tamper_count, continuous_driving_count.
14. violations in tt_risk_score table are device_removed, power_disconnect, route_deviation, stoppage_violation.
15. violations in transporter_risk_score table are device_removed, power_disconnect, route_deviation, stoppage_violation.
16. GROUP BY RULE: Any column in the SELECT list that is not inside an aggregate function (SUM, COUNT, AVG, MIN, MAX) MUST be included in the GROUP BY clause.
17. SYNTAX: Use PostgreSQL syntax. Use '||' for string concatenation. Use 'LIMIT' for top N results.
18. CRITICAL JOIN RULE: To join 'vts_alert_history' (vah) and 'alerts' (a), USE: `vah.id = a.id`. DO NOT use 'unique_id' for this join.
19. VALUE VALIDATION: Before using any string constant for filtering (e.g. status, severity), CHECK context rules for valid values. DO NOT Hallucinate values like 'FALSE ALARM'.
20. "ALWAYS include alert_section = 'VTS' for alerts table queries"
""",

    "context_guidance": {
        "risk_scores": "For vehicle risk queries, use tt_risk_score. For transporter risk - use transporter_risk_score.",
        "aggregation_rules": "Use SUM() for violation counts, COUNT(*) for record counts",
        "violation_counts": "Use vts_alert_history for violation COUNT queries",
        "time_queries": "Use ::date for date filtering",
        "trip_audit": "For emlock or swipe queries, use vts_tripauditmaster.",
        "alert_queries": "For alert queries, use alerts table with alert_section = 'VTS' and appropriate status filters",
    },

    "array_operations": {
        "unnest_pattern": "FROM table, LATERAL unnest(array_col) as elem",
        "count_distinct": "COUNT(DISTINCT elem) -- after unnest",
        "to_string": "array_to_string(array_col, ', ')",
        "NEVER_USE": ["ARRAY_LENGTH(DISTINCT ...)", "array_col[index]"]
    },

    "critical_rules": {
        "date_filtering": "ALWAYS use created_at::date = 'YYYY-MM-DD' for date filtering for all tables except vts_alert_history,for this vts_alert_history take vts_end_datetime column",
        "violation_sums": "ALWAYS sum ALL violation count columns for total violations from vts_alert_history",
        "vehicle_identifiers": "Use correct vehicle ID column for each table: tt_number, tl_number, truck_no, vehicle_number, trucknumber", # This is now superseded by DATA_MODEL
        "alert_section": "ALWAYS include alert_section = 'VTS' for alerts table queries",
        "violation_columns": "vts_alert_history.violation_type (ARRAY), vts_ongoing_trips.violation_type (TEXT), alerts.violation_type (TEXT)",
        "array_functions": "For vts_alert_history arrays: use array_to_string(violation_type, ', ') not violation_types"
    }
}

NO_SQL_QA_PAIRS = [
    {
        "question": "What is the purpose of this system?",
        "keywords": ["purpose", "system", "about"],
        "answer": "This is a Vehicle Tracking System (VTS) designed to monitor vehicle compliance, track live trips, manage alerts, and analyze risk based on historical data from various sources."
    },
    {
        "question": "How is risk score calculated?",
        "keywords": ["risk score", "calculated", "formula", "how"],
        "answer": "The risk score is a complex metric calculated based on several factors, including the frequency and severity of violations like device removal, power disconnects, route deviations, and unauthorized stoppages. Both individual vehicles (in `tt_risk_score`) and entire transporter fleets (in `transporter_risk_score`) have their own scores."
    },
    {
        "question": "What is the difference between a violation and an alert?",
        "keywords": ["difference", "violation", "alert", "distinction"],
        "answer": "A 'violation' is a specific event recorded in `vts_alert_history` (e.g., `speed_violation_count` > 0). An 'alert' is a record created in the `alerts` table, often triggered by one or more violations, that requires user attention and has a workflow status (Open, Close) and severity."
    },
    {
        "question": "What does a 'TC' violation mean in ongoing trips?",
        "keywords": ["tc", "violation", "mean", "definition"],
        "answer": "In the `vts_ongoing_trips` table, a 'TC' violation stands for 'Trip pending closure'. It indicates a trip that has likely finished but has not been formally closed in the system for over 2 hours."
    },
    {
        "question": "What does a 'swipe out failure' mean?",
        "keywords": ["swipe out failure", "mean", "definition"],
        "answer": "A 'swipe out failure' is recorded in the `vts_tripauditmaster` table when the `swipeoutl1` or `swipeoutl2` column is 'False'. It indicates a security or compliance issue where a driver or operator failed to properly swipe out at a designated location."
    }
]
# =============================================================================================
# NEW: PRODUCTION-GRADE DATA MODEL (SINGLE SOURCE OF TRUTH)
# This model dictates all entity relationships, join paths, and system rules.
# =============================================================================================
DATA_MODEL = {
    "master_entities": {
        "vehicle": {
            "canonical_table": "vts_truck_master",
            "primary_key": "truck_no",
            "identifier_map": {
                "alerts": "vehicle_number",
                "vts_alert_history": "tl_number",
                "vts_ongoing_trips": "tt_number",
                "tt_risk_score": "tt_number",
                "vts_tripauditmaster": "trucknumber",
            }
        },
        "transporter": {
            "canonical_id": "transporter_code",
            "display_column": "transporter_name",
            "table": "vts_truck_master",
            "source_tables": {
                "vts_truck_master": "transporter_code",
                "alerts": "transporter_code",
                "tt_risk_score": "transporter_code",
                "transporter_risk_score": "transporter_code",
            }
        }
    },
    "column_locations": {
        "driver_name": ["vts_ongoing_trips"], # Driver name is ONLY here
        "risk_score": ["tt_risk_score", "transporter_risk_score"],
        "violation_type": ["vts_alert_history", "vts_ongoing_trips", "alerts"]
    },
    "relationships": {
        "alerts.vehicle_number": "vts_truck_master.truck_no",
        "vts_alert_history.tl_number": "vts_truck_master.truck_no",
        "vts_ongoing_trips.tt_number": "vts_truck_master.truck_no",
        "tt_risk_score.tt_number": "vts_truck_master.truck_no",
        "vts_tripauditmaster.trucknumber": "vts_truck_master.truck_no",
        "vts_truck_master.transporter_code": "transporter_risk_score.transporter_code",
    },
    "forbidden_joins": [
        frozenset(["alerts", "vts_alert_history"]),
        frozenset(["alerts", "tt_risk_score"]),
        # This join is allowed, but must go through vts_truck_master
        # frozenset(["alerts", "transporter_risk_score"]),
    ],
    "time_semantics": {
        "alerts": "alerts.created_at",
        "closed_alerts": "alerts.closed_at",
        "violation_history": "vts_alert_history.vts_end_datetime",
        "live_trips": "vts_ongoing_trips.event_start_datetime",
        "trip_audit": "vts_tripauditmaster.createdat",
    },
    "aggregation_rule": "CRITICAL: ALWAYS aggregate data FIRST using a Common Table Expression (CTE), and only THEN join to other tables. This prevents row explosion errors.",
    "canonical_question_map": {
        "alert": "alerts",
        "open alert": "alerts",
        "closed alert": "alerts",
        "violation": "vts_alert_history",
        "history": "vts_alert_history",
        "live": "vts_ongoing_trips",
        "ongoing": "vts_ongoing_trips",
        "current trip": "vts_ongoing_trips",
        "vehicle risk": "tt_risk_score",
        "transporter risk": "transporter_risk_score",
    },
    "sanity_check_rules": {
        "max_rows": 20000,
    }
}

# =============================================================================================
# ===== NEW: SEMANTIC RULES (Defines the meaning and correct usage of data concepts) =====
# =============================================================================================
SEMANTIC_RULES = {
    # =====================================================
    # VEHICLE MASTER SEMANTICS
    # =====================================================
    "vehicle_master": {
        "meaning": "Authoritative vehicle registry and attributes",
        "allowed_tables": ["vts_truck_master"],
        "primary_key": "truck_no",
        "attributes": [
            "truck_no",
            "transporter_name",
            "whether_truck_blacklisted"
        ],
        "notes": "All vehicle-level analysis must anchor to this table"
    },

    # =====================================================
    # VEHICLE ID NORMALIZATION
    # =====================================================
    "vehicle_identity": {
        "canonical_vehicle_id": "truck_no",
        "aliases": {
            "vts_truck_master": "truck_no",
            "vts_alert_history": "tl_number",
            "vts_ongoing_trips": "tt_number",
            "tt_risk_score": "tt_number",
            "alerts": "vehicle_number"
        },
        "enforce_normalization": True,
        "notes": "LLM must normalize vehicle identity before joins"
    },

    # =====================================================
    # TRIP SEMANTICS
    # =====================================================
    "trip_count": {
        "meaning": "Actual number of trips",
        "allowed_tables": ["vts_ongoing_trips", "tt_risk_score"],
        "allowed_columns": ["trip_id", "total_trips"],
        "forbidden_tables": ["vts_alert_history", "alerts"],
        "notes": "Trips must NEVER be inferred from alerts or violations"
    },

    "live_trip": {
        "meaning": "Trips currently in progress",
        "allowed_tables": ["vts_ongoing_trips"],
        "required_columns": ["event_start_datetime"],
        "forbidden_tables": ["vts_alert_history", "alerts"],
        "time_scope": "real_time"
    },

    "completed_trip": {
        "meaning": "Trips that have ended",
        "allowed_tables": ["vts_ongoing_trips", "tt_risk_score"],
        "required_condition": [
            "event_end_datetime IS NOT NULL",
            "trip_status = 'Completed'"
        ],
        "forbidden_tables": ["vts_alert_history"],
        "time_scope": "historical"
    },

    "historical_trip": {
        "meaning": "Aggregated historical trips",
        "allowed_tables": ["tt_risk_score"],
        "required_columns": ["total_trips"],
        "forbidden_tables": ["vts_ongoing_trips"],
        "time_scope": "historical"
    },

    # =====================================================
    # VIOLATION SEMANTICS
    # =====================================================
    "violation_history": {
        "meaning": "Historical violation counts per vehicle/trip",
        "allowed_tables": ["vts_alert_history"],
        "allowed_columns": [
            "stoppage_violations_count",
            "route_deviation_count",
            "speed_violation_count",
            "main_supply_removal_count",
            "night_driving_count",
            "no_halt_zone_count",
            "device_offline_count",
            "device_tamper_count",
            "continuous_driving_count"
        ],
        "aggregation_required": True,
        "forbidden_tables": ["alerts", "vts_ongoing_trips"],
        "notes": "Violations are numeric counts, not alert events"
    },

    "violation_type": {
        "meaning": "Categorical classification of violation",
        "allowed_tables": ["vts_alert_history", "alerts"],
        "allowed_columns": ["violation_type"],
        "forbidden_usage": [
            "COUNT(violation_type)",
            "SUM(violation_type)"
        ],
        "notes": "Violation_type is descriptive only"
    },

    "violation_alert_relation": {
        "meaning": "Relationship between violations and alerts",
        "violation_source": "vts_alert_history",
        "alert_source": "alerts",
        "join_optional": True,
        "notes": [
            "Violations may exist without alerts",
            "Alerts may aggregate multiple violations",
            "Absence of alert != absence of violation"
        ]
    },

    # =====================================================
    # ALERT SEMANTICS
    # =====================================================
    "alert_status": {
        "meaning": "Operational workflow state of alerts",
        "allowed_tables": ["alerts"],
        "allowed_columns": ["alert_status", "alert_state"],
        "valid_values": ["Open", "Close", "In Progress"],
        "forbidden_tables": ["vts_alert_history"]
    },

    "alert_severity": {
        "meaning": "Criticality level of alert",
        "allowed_tables": ["alerts"],
        "allowed_columns": ["severity"],
        "valid_values": ["Low", "Medium", "High", "Critical"]
    },

    "false_alert": {
        "meaning": "Alerts marked as false positive",
        "allowed_tables": ["alerts"],
        "required_columns": ["mark_as_false"],
        "forbidden_columns": ["is_flagged_false"],
        "notes": "Use mark_as_false ONLY"
    },

    "alert_lifecycle": {
        "meaning": "Timing of alert creation and closure",
        "allowed_tables": ["alerts"],
        "allowed_time_columns": ["created_at", "closed_at"],
        "forbidden_time_columns": ["vts_end_datetime"]
    },

    # =====================================================
    # RISK SCORE SEMANTICS
    # =====================================================
    "vehicle_risk_score": {
        "meaning": "Overall calculated vehicle risk score",
        "allowed_tables": ["tt_risk_score"],
        "required_columns": ["risk_score"],
        "forbidden_tables": ["vts_alert_history", "alerts"]
    },

    "risk_component": {
        "meaning": "Individual components contributing to risk score",
        "allowed_tables": ["tt_risk_score"],
        "allowed_columns": [
            "device_removed",
            "power_disconnect",
            "route_deviation",
            "stoppage_violation"
        ]
    },

    # =====================================================
    # BLACKLIST SEMANTICS
    # =====================================================
    "blacklist_status": {
        "meaning": "Vehicle blacklist indicator",
        "allowed_tables": ["vts_truck_master"],
        "required_column": "whether_truck_blacklisted",
        "forbidden_columns": ["blacklisted"]
    }
}

# Integrate the new DATA_MODEL into the legacy SQL_RULES for backward compatibility
SQL_RULES["DATA_MODEL"] = DATA_MODEL
SQL_RULES["business_rules"] = """
1. **JOINING**: All joins must resolve to a master entity. To get transporter details for a vehicle, you MUST join through `vts_truck_master`.
2. **AGGREGATION**: You MUST aggregate data in a CTE first before joining to prevent row explosion.
3. **TIME**: Use the correct time column for the query type: `alerts.created_at` for alerts, `vts_alert_history.vts_end_datetime` for history.
4. **FORBIDDEN**: Do not join `alerts` directly to `vts_alert_history` or `tt_risk_score`.
"""
SQL_RULES["SEMANTIC_RULES"] = SEMANTIC_RULES

SQL_RULES["query_intents"] = {
    "aggregation": ["total", "sum", "count", "average", "avg", "max", "min"],
    "comparison": ["compare", "vs", "versus", "difference between"],
    "ranking": ["top", "bottom", "highest", "lowest", "worst", "best", "most"],
    "filtering": ["where", "with", "having", "that have"],
    "time_series": ["trend", "over time", "daily", "monthly", "weekly"],
    "location": ["zone", "region", "location", "area"],
    "status": ["current", "active", "ongoing", "Open", "Close"]
}

SQL_RULES["time_patterns"] = {
    "today": "vts_end_datetime::date = CURRENT_DATE",
    "yesterday": "vts_end_datetime::date = CURRENT_DATE - INTERVAL '1 day'",
    "this week": "vts_end_datetime >= DATE_TRUNC('week', CURRENT_DATE)",
    "last week": "vts_end_datetime >= CURRENT_DATE - INTERVAL '7 days'",
    "this month": "vts_end_datetime >= DATE_TRUNC('month', CURRENT_DATE)",
    "last month": "vts_end_datetime >= DATE_TRUNC('month', CURRENT_DATE - INTERVAL '1 month') AND vts_end_datetime < DATE_TRUNC('month', CURRENT_DATE)",
    "this year": "vts_end_datetime >= DATE_TRUNC('year', CURRENT_DATE)",
    "last 30 days": "vts_end_datetime >= CURRENT_DATE - INTERVAL '30 days'",
    "last 90 days": "vts_end_datetime >= CURRENT_DATE - INTERVAL '90 days'",
    "in day": "vts_end_datetime::date = CURRENT_DATE",
}

# Training examples - COMPLETELY CORRECTED QA pairs (2000+ lines)
TRAINING_QA_PAIRS_RAW = [

    ("Vehicles with speed violations but NOT in high-risk category",
    """SELECT
    'speed_not_high_risk' AS "Data Table",
    vah.tl_number,
    trs.risk_score
FROM vts_alert_history vah
JOIN tt_risk_score trs ON vah.tl_number = trs.tt_number
WHERE vah.speed_violation_count > 0
  AND trs.risk_score <= 50 -- Low risk definition
ORDER BY trs.risk_score DESC;"""),

    # ===== NEW: GOLD STANDARD - SAP ID Filter for Risk Score =====
    ("show all vehicles risk_score data for sap_id 1128",
    """SELECT 
    'tt_risk_score' AS "Data Table",
    trs.tt_number,
    trs.risk_score,
    vtm.transporter_name,
    vtm.sap_id
FROM tt_risk_score trs
JOIN vts_truck_master vtm ON trs.tt_number = vtm.truck_no
WHERE vtm.sap_id = '1128'
ORDER BY trs.risk_score DESC;"""),

    # ===== NEW: ROBUSTNESS - Distinct Risk Scores =====
    ("What is the risk score for vehicle KA01AB1234?",
     """SELECT
    'tt_risk_score' AS "Data Table",
    trs.tt_number,
    trs.risk_score,
    vtm.transporter_name
FROM tt_risk_score trs
JOIN vts_truck_master vtm ON trs.tt_number = vtm.truck_no
WHERE trs.tt_number ILIKE 'KA01AB1234';"""),

    ("What is the risk score for transporter ABC Logistics?",
     """SELECT
    'transporter_risk_score' AS "Data Table",
    transporter_name,
    transporter_code,
    risk_score
FROM transporter_risk_score
WHERE transporter_name ILIKE '%ABC Logistics%';"""),

    # ===== NEW: ROBUSTNESS - Missing Data / Reporting =====
    ("Show me vehicles that have not reported any data in vts_alert_history for the last 7 days",
     """SELECT 
    'vts_truck_master' AS "Data Table",
    vtm.truck_no,
    vtm.transporter_name,
    MAX(vah.vts_end_datetime) as last_reported_time
FROM vts_truck_master vtm
LEFT JOIN vts_alert_history vah ON vtm.truck_no = vah.tl_number
WHERE vtm.whether_truck_blacklisted IS DISTINCT FROM 'Y' -- Exclude blacklisted
GROUP BY vtm.truck_no, vtm.transporter_name
HAVING MAX(vah.vts_end_datetime) < CURRENT_DATE - INTERVAL '7 days' OR MAX(vah.vts_end_datetime) IS NULL
ORDER BY last_reported_time ASC NULLS FIRST;"""),



    # ===== NEW: GOLD STANDARD - Strict Column Selection (Speed Only) =====
    ("Show me the total speed violations for vehicles with sap_id '1128'",
    """SELECT 
    'vts_alert_history' AS "Data Table",
    vah.tl_number,
    SUM(vah.speed_violation_count) as total_speed_violations
FROM vts_alert_history vah
JOIN vts_truck_master vtm ON vah.tl_number = vtm.truck_no
WHERE vtm.sap_id = '1128'
GROUP BY vah.tl_number
ORDER BY total_speed_violations DESC;"""),

    ("Where are the vehicles with sap_id '1128' right now?",
    """SELECT 
    'vts_ongoing_trips' AS "Data Table",
    vot.tt_number,
    vot.vehicle_location,
    vot.event_start_datetime,
    vtm.sap_id
FROM vts_ongoing_trips vot
JOIN vts_truck_master vtm ON vot.tt_number = vtm.truck_no
WHERE vtm.sap_id = '1128'
ORDER BY vot.event_start_datetime DESC
LIMIT 50;"""),

    ("Find vehicles with violations increasing each month for the past year",
    """WITH monthly_violations AS (
    -- Step 1: Aggregate total violations per vehicle per month for the last year.
    SELECT
        tl_number,
        DATE_TRUNC('month', vts_end_datetime) AS violation_month,
        SUM(stoppage_violations_count + route_deviation_count + speed_violation_count + night_driving_count + device_offline_count + device_tamper_count + continuous_driving_count + no_halt_zone_count + main_supply_removal_count) AS total_monthly_violations
    FROM vts_alert_history
    WHERE vts_end_datetime >= CURRENT_DATE - INTERVAL '12 months'
    GROUP BY tl_number, DATE_TRUNC('month', vts_end_datetime)
    HAVING SUM(stoppage_violations_count + route_deviation_count + speed_violation_count + night_driving_count + device_offline_count + device_tamper_count + continuous_driving_count + no_halt_zone_count + main_supply_removal_count) > 0
),
violation_trends AS (
    -- Step 2: Use LAG() to get the previous month's violation count for comparison.
    SELECT
        tl_number,
        violation_month,
        total_monthly_violations,
        LAG(total_monthly_violations, 1, 0) OVER (PARTITION BY tl_number ORDER BY violation_month) AS previous_month_violations
    FROM monthly_violations
),
trend_check AS (
    -- Step 3: Check if violations are strictly increasing and count the number of data points (months) per vehicle.
    SELECT
        tl_number,
        CASE WHEN total_monthly_violations > previous_month_violations THEN 1 ELSE 0 END AS is_increasing,
        COUNT(*) OVER (PARTITION BY tl_number) as num_months
    FROM violation_trends
)
-- Step 4: Final selection. A vehicle's violations are increasing each month if the number of "increasing" months
-- is one less than the total number of months with data (since the first month cannot be 'increasing').
SELECT
    'increasing_violation_trend' AS "Data Table",
    tl_number
FROM trend_check
GROUP BY tl_number, num_months
HAVING SUM(is_increasing) = num_months - 1 AND num_months > 1;"""),

    # ===== NEW: GOLD STANDARD - Demo Failing Query =====
    ("which transporter has maximum risk score and maximum violations",
    """SELECT 
    vtm.transporter_name, 
    MAX(trs.risk_score) AS max_risk_score,
    SUM(
        COALESCE(vah.stoppage_violations_count, 0) +
        COALESCE(vah.route_deviation_count, 0) +
        COALESCE(vah.speed_violation_count, 0) +
        COALESCE(vah.main_supply_removal_count, 0) +
        COALESCE(vah.night_driving_count, 0) +
        COALESCE(vah.no_halt_zone_count, 0) +
        COALESCE(vah.device_offline_count, 0) +
        COALESCE(vah.device_tamper_count, 0) +
        COALESCE(vah.continuous_driving_count, 0)
    ) AS total_violations
FROM vts_truck_master vtm 
JOIN vts_alert_history vah ON vtm.truck_no = vah.tl_number
JOIN transporter_risk_score trs ON vtm.transporter_code = trs.transporter_code
GROUP BY vtm.transporter_name
ORDER BY max_risk_score DESC, total_violations DESC LIMIT 1
"""),

    # ===== NEW: GOLD STANDARD - Hierarchical Query (Transporter + Vehicle) =====
    ("Give transporter-wise total violations and number of vehicles, along with each vehicle’s violation count, for the last 2 months.",
    """WITH vehicle_violations AS (
    -- Step 1: Aggregate violations for each vehicle in the time period
    SELECT
        vtm.transporter_name,
        vah.tl_number,
        SUM(
            COALESCE(vah.stoppage_violations_count, 0) + COALESCE(vah.route_deviation_count, 0) +
            COALESCE(vah.speed_violation_count, 0) + COALESCE(vah.main_supply_removal_count, 0) +
            COALESCE(vah.night_driving_count, 0) + COALESCE(vah.no_halt_zone_count, 0) +
            COALESCE(vah.device_offline_count, 0) + COALESCE(vah.device_tamper_count, 0) +
            COALESCE(vah.continuous_driving_count, 0)
        ) AS vehicle_total_violations
    FROM vts_alert_history vah
    JOIN vts_truck_master vtm ON vah.tl_number = vtm.truck_no
    WHERE vah.vts_end_datetime >= DATE_TRUNC('month', CURRENT_DATE - INTERVAL '2 months')
      AND vah.vts_end_datetime < DATE_TRUNC('month', CURRENT_DATE)
    GROUP BY vtm.transporter_name, vah.tl_number
)
-- Step 2: Use window functions to add transporter-level aggregates without a bad join
SELECT
    'transporter_vehicle_hierarchy' AS "Data Table",
    transporter_name,
    tl_number,
    vehicle_total_violations,
    SUM(vehicle_total_violations) OVER (PARTITION BY transporter_name) as transporter_total_violations,
    COUNT(*) OVER (PARTITION BY transporter_name) as transporter_vehicle_count
FROM vehicle_violations
ORDER BY transporter_total_violations DESC, vehicle_total_violations DESC;"""),

    # ===== NEW: FIX FOR THE DEMO-FAILING QUERY =====
    ("List vehicles with all violations in the last 7 days",
    """
SELECT
    'vts_alert_history' AS "Data Table",
    tl_number,
    SUM(stoppage_violations_count + route_deviation_count + speed_violation_count + night_driving_count + device_offline_count + device_tamper_count + continuous_driving_count + no_halt_zone_count + main_supply_removal_count) as total_violations
FROM vts_alert_history
WHERE vts_end_datetime >= CURRENT_DATE - INTERVAL '7 days'
  AND (stoppage_violations_count + route_deviation_count + speed_violation_count + night_driving_count + device_offline_count + device_tamper_count + continuous_driving_count + no_halt_zone_count + main_supply_removal_count) > 0
GROUP BY tl_number
ORDER BY total_violations DESC;
"""),

    # ===== NEW: GOLD STANDARD - High Risk Vehicle Analysis =====
    ("Show me high risk vehicles that also had speed violations in the last week",
    """SELECT 
    'tt_risk_score' AS "Data Table",
    vtm.transporter_name,
    vtm.truck_no,
    trs.risk_score,
    SUM(vah.speed_violation_count) as speed_violations
FROM vts_truck_master vtm
JOIN tt_risk_score trs ON vtm.truck_no = trs.tt_number
JOIN vts_alert_history vah ON vtm.truck_no = vah.tl_number
WHERE trs.risk_score > 50  -- Explicit High Risk Definition
  AND vah.vts_end_datetime >= CURRENT_DATE - INTERVAL '7 days'
GROUP BY vtm.transporter_name, vtm.truck_no, trs.risk_score
HAVING SUM(vah.speed_violation_count) > 0
ORDER BY trs.risk_score DESC;"""),

    # ===== NEW: PRIORITY FIX for Temporal Exclusion Logic (Last Month vs This Month) =====
    ("Give the list of vehicles that get violation in last month and not repeated in this month",
    """
SELECT
    'vts_alert_history' AS "Data Table",
    last_month.tl_number AS "Vehicle Number"
FROM (
    -- Vehicles with violations in the previous calendar month
    SELECT DISTINCT tl_number
    FROM vts_alert_history
    WHERE
        (stoppage_violations_count + route_deviation_count + speed_violation_count + night_driving_count + device_offline_count + device_tamper_count + continuous_driving_count + no_halt_zone_count + main_supply_removal_count) > 0
        AND vts_end_datetime >= DATE_TRUNC('month', CURRENT_DATE - INTERVAL '1 month')
        AND vts_end_datetime < DATE_TRUNC('month', CURRENT_DATE)
) AS last_month
LEFT JOIN (
    -- Vehicles with violations in the current calendar month
    SELECT DISTINCT tl_number
    FROM vts_alert_history
    WHERE
        (stoppage_violations_count + route_deviation_count + speed_violation_count + night_driving_count + device_offline_count + device_tamper_count + continuous_driving_count + no_halt_zone_count + main_supply_removal_count) > 0
        AND vts_end_datetime >= DATE_TRUNC('month', CURRENT_DATE)
) AS this_month ON last_month.tl_number = this_month.tl_number
-- Keep only those from last month that do not appear in this month's list
WHERE this_month.tl_number IS NULL
"""),

    # ===== PRIORITY 1: Violation Type Analysis =====
    ("count violation_type type in all tables",
     """SELECT 'vts_ongoing_trips' AS "Source Table",
    violation_type,
    COUNT(*) as count
FROM vts_ongoing_trips 
WHERE violation_type IS NOT NULL
GROUP BY violation_type

UNION ALL

SELECT 'vts_alert_history' AS "Source Table",
    vt as violation_type,
    COUNT(*) as count
FROM vts_alert_history, LATERAL unnest(violation_type) as vt
WHERE vt IS NOT NULL
GROUP BY vt

UNION ALL
 
SELECT 'alerts' AS "Source Table",
    violation_type,
    COUNT(*) as count
FROM alerts 
WHERE violation_type IS NOT NULL AND alert_section = 'VTS'
GROUP BY violation_type

UNION ALL

SELECT 'vts_tripauditmaster' AS "Source Table",
    'Swipe Out Failed' as violation_type,
    COUNT(*) as count
FROM vts_tripauditmaster
WHERE swipeoutl1 = 'False' OR swipeoutl2 = 'False'

ORDER BY "Source Table", count DESC"""),

# REPLACE the problematic array examples with CORRECT syntax:

("find vehicles that had more than 3 different types of violations in a single day", # FIX: Removed incorrect 30-day filter
 """SELECT 
    tl_number,
    vts_end_datetime::date as violation_date,
    COUNT(DISTINCT vt) as distinct_violation_types 
FROM vts_alert_history,
    LATERAL unnest(violation_type) as vt
WHERE violation_type IS NOT NULL
GROUP BY tl_number, vts_end_datetime::date
HAVING COUNT(DISTINCT vt) > 3
ORDER BY distinct_violation_types DESC"""),

("count distinct violation types per vehicle",
 """SELECT 
    tl_number,
    COUNT(DISTINCT vt) as distinct_violation_type_count 
FROM vts_alert_history, 
    LATERAL unnest(violation_type) as vt
WHERE vt IS NOT NULL 
GROUP BY tl_number
HAVING COUNT(DISTINCT vt) > 1
ORDER BY distinct_violation_type_count DESC"""),

("show violation types analysis",
 """SELECT 
    'vts_alert_history' AS "Data Table",
    tl_number,
    vts_end_datetime::date as event_date,
    array_length(violation_type, 1) as total_violation_types,
    array_to_string(violation_type, ', ') as violation_type
FROM vts_alert_history
WHERE violation_type IS NOT NULL 
    AND array_length(violation_type, 1) > 0
ORDER BY vts_end_datetime DESC
LIMIT 10"""),

    ("Show me vehicles with the same violation type repeated more than 5 times",
    """
SELECT
    'vts_alert_history' AS "Data Table",
    vah.tl_number,
    vt.violation_type_element,
    COUNT(*) AS repetition_count
FROM 
    vts_alert_history vah,
    LATERAL UNNEST(vah.violation_type) AS vt(violation_type_element)
WHERE
    vah.violation_type IS NOT NULL AND array_length(vah.violation_type, 1) > 0
GROUP BY 
    vah.tl_number,
    vt.violation_type_element
HAVING
    COUNT(*) > 5
ORDER BY
    repetition_count DESC
"""
    ),

    ("what are the common columns in vts_alert_history and vts_ongoing_trips",
    """SELECT 
    c1.column_name,
    c1.data_type 
FROM information_schema.columns c1
JOIN information_schema.columns c2 
    ON c1.column_name = c2.column_name
   AND c1.data_type = c2.data_type 
WHERE 
    c1.table_name = 'vts_alert_history'
    AND c2.table_name = 'vts_ongoing_trips'
ORDER BY c1.column_name;"""),

    ("Show all vehicles have violation stoppage",
     """SELECT 'vts_alert_history' AS "Data Table",
    tl_number,
    SUM(stoppage_violations_count) as total_stoppage
FROM vts_alert_history
WHERE stoppage_violations_count > 0
GROUP BY tl_number
ORDER BY total_stoppage DESC
LIMIT 50"""),

    ("all vehicles with stoppage violations",
     """SELECT 'vts_alert_history' AS "Data Table",
    tl_number,
    SUM(stoppage_violations_count) as total_stoppage
FROM vts_alert_history
WHERE stoppage_violations_count > 0
GROUP BY tl_number
ORDER BY total_stoppage DESC
LIMIT 50"""),

    ("show me every vehicle that has stoppage violations",
     """SELECT 'vts_alert_history' AS "Data Table",
    tl_number,
    SUM(stoppage_violations_count) as total_stoppage
FROM vts_alert_history
GROUP BY tl_number
HAVING SUM(stoppage_violations_count) > 0
ORDER BY total_stoppage DESC
LIMIT 50"""),

    ("vehicles with route deviations",
     """SELECT 'vts_alert_history' AS "Data Table",
    tl_number,
    SUM(route_deviation_count) as total_route_deviations
FROM vts_alert_history
GROUP BY tl_number
HAVING SUM(route_deviation_count) > 0
ORDER BY total_route_deviations DESC
LIMIT 50"""),

    ("all trucks with speed violations",
     """SELECT 'vts_alert_history' AS "Data Table",
    tl_number,
    SUM(speed_violation_count) as total_speed_violations
FROM vts_alert_history
GROUP BY tl_number
HAVING SUM(speed_violation_count) > 0
ORDER BY total_speed_violations DESC
LIMIT 50"""),

    ("show all vehicles with any violations",
     """SELECT 'vts_alert_history' AS "Data Table",
    tl_number,
    SUM(stoppage_violations_count) as total_stoppage,
    SUM(route_deviation_count) as total_route_deviations,
    SUM(speed_violation_count) as total_speed_violations, SUM(night_driving_count) as total_night_driving,
    SUM(continuous_driving_count) as total_continuous_driving, SUM(device_offline_count) as total_device_offline,
    SUM(device_tamper_count) as total_device_tamper, SUM(no_halt_zone_count) as total_no_halt_zone,
    SUM(main_supply_removal_count) as total_main_supply_removal, SUM(stoppage_violations_count + route_deviation_count + speed_violation_count+night_driving_count+continuous_driving_count+device_offline_count+device_tamper_count+no_halt_zone_count+main_supply_removal_count) as total_violations
FROM vts_alert_history
GROUP BY tl_number
HAVING SUM(stoppage_violations_count + route_deviation_count + speed_violation_count+night_driving_count+continuous_driving_count+device_offline_count+device_tamper_count+no_halt_zone_count+main_supply_removal_count) > 0
ORDER BY total_violations DESC
LIMIT 50"""),

    # ===== PRIORITY 1: Per-Vehicle Aggregation =====
    ("What is the total number of stoppage violations per vehicle?",
     """SELECT 'vts_alert_history' AS "Data Table",
    tl_number,
    SUM(stoppage_violations_count) as total_stoppage_violations
FROM vts_alert_history
WHERE stoppage_violations_count > 0
GROUP BY tl_number
ORDER BY total_stoppage_violations DESC"""),

    # ===== TOP VIOLATORS QUERIES =====
    ("top vehicles with stoppage violations",
     """SELECT 'vts_alert_history' AS "Data Table",
    tl_number,
    SUM(stoppage_violations_count) as total_stoppage
FROM vts_alert_history
GROUP BY tl_number
ORDER BY total_stoppage DESC
LIMIT 20
"""),

    ("vehicles with highest violations",
     """SELECT 'vts_alert_history' AS "Data Table",
    tl_number,
    SUM(stoppage_violations_count + route_deviation_count + speed_violation_count + 
        night_driving_count + device_offline_count + device_tamper_count + 
        continuous_driving_count + no_halt_zone_count + main_supply_removal_count) as total_violations
FROM vts_alert_history
GROUP BY tl_number
ORDER BY total_violations DESC
LIMIT 20
"""),

    ("top 10 vehicles by stoppage count",
     """SELECT 'vts_alert_history' AS "Data Table",
    tl_number,
    SUM(stoppage_violations_count) as total_stoppage
FROM vts_alert_history
GROUP BY tl_number
ORDER BY total_stoppage DESC
LIMIT 10
"""),

    # ===== SPECIFIC VIOLATION TYPE QUERIES =====
    ("vehicles having stoppage violations",
     """SELECT 'vts_alert_history' AS "Data Table",
    tl_number,
    SUM(stoppage_violations_count) as total_stoppage
FROM vts_alert_history
GROUP BY tl_number
HAVING SUM(stoppage_violations_count) > 0
ORDER BY total_stoppage DESC
LIMIT 50
"""),

    ("list vehicles with route deviation",
     """SELECT 'vts_alert_history' AS "Data Table",
    tl_number,
    SUM(route_deviation_count) as total_route_deviations
FROM vts_alert_history
GROUP BY tl_number
HAVING SUM(route_deviation_count) > 0
ORDER BY total_route_deviations DESC
LIMIT 50
"""),

# Add count-specific examples
("count of TC violation",
 """SELECT 'vts_ongoing_trips' AS "Data Table",
    COUNT(*) as tc_violation_count
FROM vts_ongoing_trips
WHERE violation_type = 'TC'"""),

("how many TC violations",
 """SELECT 'vts_ongoing_trips' AS "Data Table",
    COUNT(*) as tc_violation_count
FROM vts_ongoing_trips
WHERE violation_type = 'TC'
"""),

("number of trip closure violations", 
 """SELECT 'vts_ongoing_trips' AS "Data Table",
    COUNT(*) as tc_violation_count
FROM vts_ongoing_trips
WHERE violation_type = 'TC'
"""),

("count all TC violation types",
 """SELECT 'vts_ongoing_trips' AS "Data Table",
    violation_type,
    COUNT(*) as count
FROM vts_ongoing_trips
WHERE violation_type IS NOT NULL
GROUP BY violation_type
ORDER BY count DESC
"""),

# Add specific violation type queries
("Show all vehicles with TC violation type",
 """SELECT 'vts_ongoing_trips' AS "Data Table",
    tt_number,
    violation_type
FROM vts_ongoing_trips
WHERE violation_type = 'TC'
"""),

("vehicles with trip closure violations",
 """SELECT 'vts_ongoing_trips' AS "Data Table",
    tt_number,
    violation_type,
    created_at
FROM vts_ongoing_trips
WHERE violation_type = 'TC'
ORDER BY created_at DESC
"""),

# Violation type analysis queries
("how many types of violation are their in vts_ongoing_trips?",
 """SELECT 'vts_ongoing_trips' AS "Data Table",
    violation_type,
    COUNT(*) as count
FROM vts_ongoing_trips
WHERE violation_type IS NOT NULL
GROUP BY violation_type
ORDER BY count DESC
"""),


("show me different violation types",
 """SELECT 'vts_ongoing_trips' AS "Data Table",
    violation_type,
    COUNT(*) as frequency
FROM vts_ongoing_trips
WHERE violation_type IS NOT NULL
GROUP BY violation_type
ORDER BY frequency DESC
"""),

("violation types breakdown",
 """SELECT 'vts_ongoing_trips' AS "Data Table",
    violation_type,
    COUNT(*) as total_count
FROM vts_ongoing_trips
WHERE violation_type IS NOT NULL
GROUP BY violation_type
ORDER BY total_count DESC
"""),

# Multiple tables violation types
("how many types of violations are their in all tables",
 """SELECT 'vts_ongoing_trips' AS "Source Table",
    violation_type,
    COUNT(*) as count
FROM vts_ongoing_trips
WHERE violation_type IS NOT NULL
GROUP BY violation_type

UNION ALL

SELECT 'vts_alert_history' AS "Source Table",
    vt as violation_type,
    COUNT(*) as count
FROM vts_alert_history, LATERAL unnest(violation_type) as vt
WHERE violation_type IS NOT NULL AND array_length(violation_type, 1) > 0
GROUP BY vt

UNION ALL

SELECT 'alerts' AS "Source Table",
    violation_type,
    COUNT(*) as count
FROM alerts
WHERE violation_type IS NOT NULL AND alert_section = 'VTS'
GROUP BY violation_type

ORDER BY "Source Table", count DESC
"""),

("list all vehicles having TC violations",
 """SELECT 'vts_ongoing_trips' AS "Data Table",
    tt_number,
    COUNT(*) as tc_violation_count
FROM vts_ongoing_trips
WHERE violation_type = 'TC'
GROUP BY tt_number
ORDER BY tc_violation_count DESC
"""),

("Show vehicles with TC violations",
     """SELECT 'vts_ongoing_trips' AS "Data Table",
        tt_number,
        violation_type,
        created_at
    FROM vts_ongoing_trips
    WHERE violation_type = 'TC'
"""),
    
    ("List all trucks with trip closure violation type", 
     """SELECT 'vts_ongoing_trips' AS "Data Table",
        tt_number,
        violation_type, 
        vehicle_location
    FROM vts_ongoing_trips
    WHERE violation_type = 'TC'
"""),
    
    ("TC violation vehicles",
     """SELECT 'vts_ongoing_trips' AS "Data Table",
        tt_number,
        violation_type
    FROM vts_ongoing_trips
    WHERE violation_type = 'TC'
"""),
    # ===== CORRECTED DATE-BASED VIOLATION QUERIES =====
    ("total violations on 2025-10-14",
     """SELECT 'vts_alert_history' AS "Data Table",
    SUM(stoppage_violations_count + route_deviation_count + speed_violation_count + 
        night_driving_count + device_offline_count + device_tamper_count + 
        continuous_driving_count + no_halt_zone_count + main_supply_removal_count) AS total_violations
FROM vts_alert_history
WHERE vts_end_datetime::date = '2025-10-14'
"""),

    ("violations yesterday",
     """SELECT 'vts_alert_history' AS "Data Table",
    SUM(stoppage_violations_count + route_deviation_count + speed_violation_count + 
        night_driving_count + device_offline_count + device_tamper_count + 
        continuous_driving_count + no_halt_zone_count + main_supply_removal_count) AS total_violations
FROM vts_alert_history
WHERE vts_end_datetime::date = CURRENT_DATE - INTERVAL '1 day'
"""),

    # ===== CORRECTED VEHICLE-SPECIFIC VIOLATION QUERIES =====
    ("how many stoppage violation count for BR01GK1527",
     """SELECT 'vts_alert_history' AS "Data Table",
    tl_number,
    SUM(stoppage_violations_count) as total_stoppage_violations
FROM vts_alert_history
WHERE tl_number = 'BR01GK1527'
GROUP BY tl_number
"""),

    ("route deviation count for KA13B3272",
     """SELECT 'vts_alert_history' AS "Data Table",
    tl_number,
    SUM(route_deviation_count) as total_route_deviations
FROM vts_alert_history
WHERE tl_number = 'KA13B3272'
GROUP BY tl_number"""),

    ("total number of violations for CG04PN3333 in vts_alert_history",
     """SELECT 'vts_alert_history' AS "Data Table",
    tl_number,
    SUM(stoppage_violations_count + route_deviation_count + speed_violation_count + 
        night_driving_count + device_offline_count + device_tamper_count + 
        continuous_driving_count + no_halt_zone_count + main_supply_removal_count) AS total_violations
FROM vts_alert_history
WHERE tl_number = 'CG04PN3333'
GROUP BY tl_number
"""),

    ("how many total violations for KA13B3272",
     """SELECT 'vts_alert_history' AS "Data Table",
    tl_number,
    SUM(stoppage_violations_count + route_deviation_count + speed_violation_count + 
        night_driving_count + device_offline_count + device_tamper_count + 
        continuous_driving_count + no_halt_zone_count + main_supply_removal_count) AS total_violations
FROM vts_alert_history
WHERE tl_number = 'KA13B3272'
GROUP BY tl_number
"""),

    # ===== CORRECTED COMPREHENSIVE VEHICLE QUERIES =====
    ("KA13AA2040", get_comprehensive_query_for_training(query_type="vehicle_timeline", vehicle_id="KA13AA2040")),
    ("JH05DH9866", get_comprehensive_query_for_training(query_type="vehicle_timeline", vehicle_id="JH05DH9866")),
    ("CG04PN3333", get_comprehensive_query_for_training(query_type="vehicle_timeline", vehicle_id="CG04PN3333")),
    ("KA13AA2145", get_comprehensive_query_for_training(query_type="vehicle_timeline", vehicle_id="KA13AA2145")),
    ("PB03BR9468", get_comprehensive_query_for_training(query_type="vehicle_timeline", vehicle_id="PB03BR9468")),
    ("AP05TB9465", get_comprehensive_query_for_training(query_type="vehicle_timeline", vehicle_id="AP05TB9465")),
    ("HR46F0066", get_comprehensive_query_for_training(query_type="vehicle_timeline", vehicle_id="HR46F0066")),
    ("RJ19GD6553", get_comprehensive_query_for_training(query_type="vehicle_timeline", vehicle_id="RJ19GD6553")),

    # ===== CORRECTED TIME-BASED QUERIES =====
    ("What's the total number of stoppage count in last month",
     """SELECT 'vts_alert_history' AS "Data Table",
    SUM(stoppage_violations_count) AS total_stoppage_violations
FROM vts_alert_history 
WHERE vts_end_datetime >= DATE_TRUNC('month', CURRENT_DATE - INTERVAL '1 month')
  AND vts_end_datetime < DATE_TRUNC('month', CURRENT_DATE)
"""),

    ("What's the total number of speed violation count in last five months",
     """SELECT 'vts_alert_history' AS "Data Table",
    SUM(speed_violation_count) AS total_speed_violations
FROM vts_alert_history 
WHERE vts_end_datetime >= DATE_TRUNC('month', CURRENT_DATE - INTERVAL '5 months')
"""), 

    ("on which date have more violations",
     """SELECT 'vts_alert_history' AS "Data Table",
    vts_end_datetime::date AS violation_date,
    SUM(stoppage_violations_count + route_deviation_count + speed_violation_count + 
        night_driving_count + device_offline_count + device_tamper_count + 
        continuous_driving_count + no_halt_zone_count + main_supply_removal_count) AS total_violations
FROM vts_alert_history
WHERE vts_end_datetime IS NOT NULL GROUP BY vts_end_datetime::date
ORDER BY total_violations DESC
LIMIT 1
"""),

    # ===== CORRECTED REGION AND LOCATION QUERIES =====
    ("find vehicles in region BATHINDA",
     """SELECT 'vts_ongoing_trips' AS "Data Table",
    TT_NUMBER,
    region 
FROM vts_ongoing_trips 
WHERE region = 'BATHINDA'
"""),

    ("show vehicle numbers from Mumbai region",
     """SELECT 'vts_ongoing_trips' AS "Data Table",
    tt_number,
    region
FROM vts_ongoing_trips 
WHERE region ILIKE '%Mumbai%'
"""),

    ("what are the zones in vts_alert_history",
     """SELECT DISTINCT 'vts_alert_history' AS "Data Table", 
    zone
FROM vts_alert_history 
WHERE zone IS NOT NULL
"""),

    ("what are the zones in vts_ongoing_trips",
     """SELECT DISTINCT 'vts_ongoing_trips' AS "Data Table",
    zone 
FROM vts_ongoing_trips 
WHERE zone IS NOT NULL
"""),

    ("what are the zones in alerts table",
     """SELECT DISTINCT 'alerts' AS "Data Table",
    zone 
FROM alerts 
WHERE zone IS NOT NULL AND alert_section = 'VTS'
"""),

    ("what are the zones in vts_truck_master",
     """SELECT DISTINCT 'vts_truck_master' AS "Data Table",
    zone 
FROM vts_truck_master 
WHERE zone IS NOT NULL
"""),

    # ===== CORRECTED INVOICE AND ARRAY QUERIES =====
    ("what is the violation type for invoice 9015941411-ZF23-1292",
     """SELECT 'vts_alert_history' AS "Data Table",
    invoice_number,
    array_to_string(violation_type, ', ') as violation_types,
    tl_number
FROM vts_alert_history 
WHERE invoice_number = '9015941411-ZF23-1292'
"""),

    ("count of records with empty violation arrays",
     """SELECT 'vts_alert_history' AS "Data Table",
    COUNT(*) as empty_violation_count
FROM vts_alert_history 
WHERE violation_type IS NULL OR array_length(violation_type, 1) = 0
"""),

    ("what is the count of null or empty violations",
     """SELECT 'vts_alert_history' AS "Data Table",
    COUNT(CASE WHEN violation_type IS NULL OR array_length(violation_type, 1) = 0 THEN 1 END) AS empty_violation_count,
    COUNT(*) as total_records
FROM vts_alert_history
"""),

# Add these schema query examples to TRAINING_QA_PAIRS
("what are the columns in vts_tripauditmaster",
 "SELECT column_name, data_type FROM information_schema.columns WHERE table_name = 'vts_tripauditmaster' ORDER BY ordinal_position"),

("what are the columns in vts_alert_history table",
 "SELECT column_name, data_type FROM information_schema.columns WHERE table_name = 'vts_alert_history' ORDER BY ordinal_position"),

("show me the schema for vts_alert_history",
 "SELECT column_name, data_type FROM information_schema.columns WHERE table_name = 'vts_alert_history' ORDER BY ordinal_position"),
("describe table alerts",
 "SELECT column_name, data_type, column_default FROM information_schema.columns WHERE table_name = 'alerts' ORDER BY ordinal_position"),

("what is the structure of vts_truck_master",
 "SELECT column_name, data_type, character_maximum_length FROM information_schema.columns WHERE table_name = 'vts_truck_master' ORDER BY ordinal_position"),

    # ===== Schema Comparison =====
    # ===== NEW: GOLD STANDARD - Simple Master Data Lookup =====
    ("Show all trucks that are marked as blacklisted",
    """SELECT
    'vts_truck_master' AS "Data Table",
    truck_no,
    transporter_name
FROM vts_truck_master
WHERE whether_truck_blacklisted = 'Y'"""),

    ("what are the columns in tt_risk_score table and that columns not in transporter_risk_score table",
    """WITH tt_cols AS (
    SELECT column_name FROM information_schema.columns WHERE table_name = 'tt_risk_score'
),
trs_cols AS (
    SELECT column_name FROM information_schema.columns WHERE table_name = 'transporter_risk_score'
)
SELECT
    'columns_in_tt_not_in_trs' AS "Data Table",
    column_name
FROM tt_cols
WHERE column_name NOT IN (SELECT column_name FROM trs_cols
);"""),

    # ===== Transporter Specific Queries =====
    ("what are the vehicles are under transporter - PAMON ROADWAYS",
    """SELECT 
    'vts_truck_master' AS "Data Table",
    truck_no,
    transporter_name
FROM vts_truck_master
WHERE transporter_name ILIKE '%PAMON ROADWAYS%'"""),

    ("what are the vehicle are blocked in last 2 months under transporter - RASHMI AUTO SERVICE",
    """SELECT
    'alerts' AS "Data Table",
    a.vehicle_number,
    a.transporter_name,
    a.vehicle_blocked_start_date
FROM alerts a
WHERE
    a.transporter_name ILIKE '%RASHMI AUTO SERVICE%'
    AND a.alert_section = 'VTS'
    AND a.vehicle_unblocked_date IS NULL -- This is the correct way to check for currently blocked vehicles
    AND a.vehicle_blocked_start_date >= CURRENT_DATE - INTERVAL '2 months';
"""),


    # ===== CORRECTED TABLE PRESENCE QUERIES =====
    ("Check in which tables the vehicle number TN28AH7294 exists",
     """
SELECT 'vts_alert_history' AS "Data Table", COUNT(*) AS count FROM vts_alert_history WHERE tl_number ILIKE 'TN28AH7294'
UNION ALL
SELECT 'vts_ongoing_trips' AS "Data Table", COUNT(*) AS count FROM vts_ongoing_trips WHERE tt_number ILIKE 'TN28AH7294'
UNION ALL
SELECT 'alerts' AS "Data Table", COUNT(*) AS count FROM alerts WHERE (vehicle_number ILIKE 'TN28AH7294' OR tt_load_number ILIKE 'TN28AH7294') AND alert_section = 'VTS'
UNION ALL 
SELECT 'vts_truck_master' AS "Data Table", COUNT(*) AS count FROM vts_truck_master WHERE truck_no ILIKE 'TN28AH7294'
UNION ALL
SELECT 'vts_tripauditmaster' AS "Data Table", COUNT(*) AS count FROM vts_tripauditmaster WHERE trucknumber = 'TN28AH7294'
UNION ALL
SELECT 'tt_risk_score' AS "Data Table", COUNT(*) AS count FROM tt_risk_score WHERE tt_number = 'TN28AH7294'
"""),

    # ===== CORRECTED ALERT QUERIES =====
    ("recent VTS alerts with high severity",
     """SELECT 'alerts' AS "Data Table",
    vehicle_number,
    alert_category,
    severity,
    created_at,
    location_name, alert_status
FROM alerts
WHERE alert_section = 'VTS' AND severity = 'HIGH' 
ORDER BY created_at DESC 
LIMIT 20
"""),

("violations for KA13B3272 last month",
 """SELECT
    'vts_alert_history' AS "Data Table",
    tl_number AS "Vehicle Number",
    vts_end_datetime::date AS "Date",
    SUM(stoppage_violations_count) AS "Stoppage Violations", 
    SUM(route_deviation_count) AS "Route Deviation",
    SUM(speed_violation_count) AS "Speed Violations", 
    SUM(night_driving_count) AS "Night Driving",
    SUM(device_offline_count) AS "Device Offline",
    SUM(device_tamper_count) AS "Device Tamper",
    SUM(continuous_driving_count) AS "Continuous Driving"
FROM vts_alert_history
WHERE tl_number = 'KA13B3272'
    AND vts_end_datetime >= DATE_TRUNC('month', CURRENT_DATE - INTERVAL '1 month') 
    AND vts_end_datetime < DATE_TRUNC('month', CURRENT_DATE) 
GROUP BY tl_number, vts_end_datetime::date
ORDER BY vts_end_datetime::date DESC
"""),

("what are the violations for KA13AA2040 for last month",
 """SELECT 
    'vts_alert_history' AS "Data Table",
    tl_number AS "Vehicle Number",
    vts_end_datetime::date AS "Date",
    SUM(stoppage_violations_count) AS "Stoppage Violations", 
    SUM(route_deviation_count) AS "Route Deviation",
    SUM(speed_violation_count) AS "Speed Violations",
    SUM(night_driving_count) AS "Night Driving",
    SUM(device_offline_count) AS "Device Offline",
    SUM(device_tamper_count) AS "Device Tamper"
    , SUM(continuous_driving_count) AS "Continuous Driving",
    SUM(no_halt_zone_count) AS "No Halt Zone", 
    SUM(main_supply_removal_count) AS "Power Disconnection"
FROM vts_alert_history 
WHERE tl_number = 'KA13AA2040'
    AND vts_end_datetime >= DATE_TRUNC('month', CURRENT_DATE)
GROUP BY tl_number, vts_end_datetime::date
ORDER BY vts_end_datetime::date DESC
"""),

    ("VTS alert count by category",
     """SELECT 'alerts' AS "Data Table",
    alert_category,
    COUNT(*) as alert_count
FROM alerts
WHERE alert_section = 'VTS' 
GROUP BY alert_category 
ORDER BY alert_count DESC
"""),

    ("alerts for vehicle KA13B3272",
     """SELECT 'alerts' AS "Data Table",
    vehicle_number,
    alert_category,
    severity,
    alert_status,
    created_at 
FROM alerts 
WHERE alert_section = 'VTS' AND (vehicle_number = 'KA13B3272' OR tt_load_number = 'KA13B3272') 
ORDER BY created_at DESC
"""),

    ("open alerts",
     """SELECT 'alerts' AS "Data Table",
    vehicle_number,
    alert_category,
    severity,
    location_name, 
    created_at
FROM alerts 
WHERE alert_section = 'VTS' AND alert_status = 'OPEN' 
ORDER BY created_at DESC
"""),

# TEST 17: Fix for "open VTS alerts with high severity". Model used wrong table (vts_alert_history).
("show me open VTS alerts with high severity",
"""
SELECT 
    'alerts' AS "Data Table",
    vehicle_number,
    alert_category,
    severity,
    created_at
FROM alerts 
WHERE alert_section = 'VTS' AND severity = 'High' AND alert_status = 'OPEN'
ORDER BY created_at DESC
"""),


    ("show me alerts for vehicles from Mumbai region with high severity that are still open",
     """SELECT 'alerts' AS "Data Table",
    vehicle_number,
    alert_category,
    severity, 
    location_name,
    region
FROM alerts
WHERE alert_section = 'VTS'
  AND severity = 'HIGH'
  AND alert_status = 'OPEN'
  AND region ILIKE 'Mumbai'
"""),

    # ===== CORRECTED SAP_ID QUERIES =====
    ("vehicles with sap_id 1937",
     """SELECT 'vts_ongoing_trips' AS "Data Table",
    tt_number,
    sap_id
FROM vts_ongoing_trips 
WHERE sap_id = '1937'
"""),

    ("vehicles where sap_id equals 1937",
     """SELECT 'vts_ongoing_trips' AS "Data Table",
    tt_number,
    sap_id
FROM vts_ongoing_trips 
WHERE sap_id = '1937'
"""),

    # ===== CORRECTED ZONE UNION QUERIES =====
    ("get all zones from every table", 
     """SELECT 'vts_alert_history' AS "Table", zone FROM vts_alert_history WHERE zone IS NOT NULL
UNION 
SELECT 'vts_ongoing_trips' AS "Table", zone FROM vts_ongoing_trips WHERE zone IS NOT NULL
UNION 
SELECT 'alerts' AS "Table", zone FROM alerts WHERE zone IS NOT NULL AND alert_section = 'VTS'
UNION 
SELECT 'vts_truck_master' AS "Table", zone FROM vts_truck_master WHERE zone IS NOT NULL
"""),

    # ===== CORRECTED ONGOING TRIPS QUERIES =====
    ("current ongoing trips in Bathinda region",
     """SELECT 'vts_ongoing_trips' AS "Data Table",
    tt_number, 
    vehicle_location,
    created_at
FROM vts_ongoing_trips 
WHERE region = 'BATHINDA' 
ORDER BY created_at DESC
"""),

    ("vehicles currently on trip from Mumbai",
     """SELECT 'vts_ongoing_trips' AS "Data Table",
    tt_number, 
    vehicle_location,
    trip_id,
    created_at
FROM vts_ongoing_trips 
WHERE region ILIKE '%Mumbai%' 
ORDER BY created_at DESC
"""),

    ("trip details for vehicle TS06UB0125",
     """SELECT 'vts_ongoing_trips' AS "Data Table",
    tt_number, 
    trip_id,
    vehicle_location,
    scheduled_start_datetime,
    actual_trip_start_datetime,
    driver_name 
FROM vts_ongoing_trips 
WHERE tt_number = 'TS06UB0125' 
ORDER BY created_at DESC
LIMIT 5
"""),

    # ===== CORRECTED SPECIFIC VEHICLE VIOLATION QUERIES =====
    ("speed violation and night driving counts for AP05TB9465",
     """SELECT 'vts_alert_history' AS "Data Table",
    tl_number,
    SUM(speed_violation_count) as total_speed_violations,
    SUM(night_driving_count) as night_driving
FROM vts_alert_history 
WHERE tl_number = 'AP05TB9465' 
GROUP BY tl_number
"""),

    ("all violation counts for RJ19GD6553",
     """SELECT 'vts_alert_history' AS "Data Table",
    tl_number,
    SUM(stoppage_violations_count) as stoppage,
    SUM(route_deviation_count) as route_deviation,
    SUM(speed_violation_count) as speed,
    SUM(night_driving_count) as night,
    SUM(device_offline_count) as offline,
    SUM(device_tamper_count) as tamper,
    SUM(continuous_driving_count) as continuous,
    SUM(no_halt_zone_count) as no_halt, 
    SUM(main_supply_removal_count) as supply
FROM vts_alert_history 
WHERE tl_number = 'RJ19GD6553'
GROUP BY tl_number
"""),

    # ===== CORRECTED VEHICLE COMPARISON QUERIES =====
    ("violation count comparison for CG04PN3333 and KA13B3272",
     """SELECT 'vts_alert_history' AS "Data Table",
    tl_number,
    SUM(stoppage_violations_count + route_deviation_count + speed_violation_count + 
        night_driving_count + device_offline_count + device_tamper_count + 
        continuous_driving_count + no_halt_zone_count + main_supply_removal_count) as total_violations
FROM vts_alert_history
WHERE tl_number IN ('CG04PN3333', 'KA13B3272') 
GROUP BY tl_number 
ORDER BY total_violations DESC
"""),

    # ===== CORRECTED STATISTICAL QUERIES =====
    ("which transporter has maximum risk score and maximum violations",
     """SELECT
    tm.transporter_name,
    trs.risk_score,
    SUM(
        COALESCE(vah.stoppage_violations_count, 0) +
        COALESCE(vah.route_deviation_count, 0) +
        COALESCE(vah.speed_violation_count, 0) +
        COALESCE(vah.main_supply_removal_count, 0) +
        COALESCE(vah.night_driving_count, 0) +
        COALESCE(vah.no_halt_zone_count, 0) +
        COALESCE(vah.device_offline_count, 0) +
        COALESCE(vah.device_tamper_count, 0) +
        COALESCE(vah.continuous_driving_count, 0)
    ) AS total_violations
FROM vts_truck_master tm
JOIN vts_alert_history vah ON tm.truck_no = vah.tl_number
JOIN transporter_risk_score trs ON tm.transporter_code = trs.transporter_code
GROUP BY tm.transporter_name, trs.risk_score
ORDER BY total_violations DESC, trs.risk_score DESC
LIMIT 10
"""),

    ("average violations per vehicle by region",
     """SELECT 'vts_alert_history' AS "Data Table",
    region,
    COUNT(DISTINCT tl_number) as vehicle_count,
    AVG(stoppage_violations_count + route_deviation_count + speed_violation_count + night_driving_count + device_offline_count + device_tamper_count + continuous_driving_count + no_halt_zone_count + main_supply_removal_count) as avg_violations
FROM vts_alert_history 
WHERE region IS NOT NULL 
GROUP BY region 
ORDER BY avg_violations DESC
"""),


    # ===== CORRECTED BASIC AGGREGATION QUERIES =====
    ("total stoppage violations for KA13B3272",
     """SELECT 'vts_alert_history' AS "Data Table",
    SUM(stoppage_violations_count) as total_stoppage
FROM vts_alert_history 
WHERE tl_number = 'KA13B3272'
"""),

    # ===== CORRECTED VEHICLE MASTER DATA QUERIES =====
    ("get transporter name for vehicle KA13AA2040",
     """SELECT 'vts_truck_master' AS "Data Table",
    truck_no,
    transporter_name,
    location_name
FROM vts_truck_master 
WHERE truck_no = 'KA13AA2040'
"""),

    # ===== CORRECTED DAILY SUMMARY QUERIES =====
    ("daily operations summary",
     """SELECT 'daily_summary' AS "Data Table",
    'Active Trips' as metric,
    COUNT(*) as value
FROM vts_ongoing_trips
UNION ALL
SELECT 'daily_summary' AS "Data Table",
    'Violations Today' as metric,
    COALESCE(SUM(stoppage_violations_count + route_deviation_count + speed_violation_count + night_driving_count + device_offline_count + device_tamper_count + continuous_driving_count + no_halt_zone_count + main_supply_removal_count), 0) as value
FROM vts_alert_history
WHERE vts_end_datetime::date = CURRENT_DATE"""),

    # ===== CORRECTED VEHICLE PRESENCE ACROSS TABLES =====
    ("what tables have vehicle MP48ZC1958",
     """
SELECT 'vts_alert_history' AS "Data Table", COUNT(*) AS count FROM vts_alert_history WHERE tl_number ILIKE 'MP48ZC1958'
UNION ALL
SELECT 'vts_ongoing_trips' AS "Data Table", COUNT(*) AS count FROM vts_ongoing_trips WHERE tt_number ILIKE 'MP48ZC1958'
UNION ALL
SELECT 'alerts' AS "Data Table", COUNT(*) AS count FROM alerts WHERE (vehicle_number ILIKE 'MP48ZC1958' OR tt_load_number ILIKE 'MP48ZC1958') AND alert_section = 'VTS'
UNION ALL 
SELECT 'vts_truck_master' AS "Data Table", COUNT(*) AS count FROM vts_truck_master WHERE truck_no ILIKE 'MP48ZC1958'
UNION ALL
SELECT 'vts_tripauditmaster' AS "Data Table", COUNT(*) AS count FROM vts_tripauditmaster WHERE trucknumber = 'MP48ZC1958'
UNION ALL
SELECT 'tt_risk_score' AS "Data Table", COUNT(*) AS count FROM tt_risk_score WHERE tt_number = 'MP48ZC1958'
"""),

    # ===== CORRECTED SPECIFIC VEHICLE WITH DATE =====
    ("violations for vehicle JH05DH9866 on 2025-10-14",
     """SELECT 'vts_alert_history' AS "Data Table",
    tl_number,
    SUM(stoppage_violations_count + route_deviation_count + speed_violation_count + 
        night_driving_count + device_offline_count + device_tamper_count +
        continuous_driving_count + no_halt_zone_count + main_supply_removal_count) AS total_violations
FROM vts_alert_history
WHERE tl_number = 'JH05DH9866'
    AND vts_end_datetime::date = '2025-10-14'
GROUP BY tl_number
"""),

    # ===== CORRECTED RANGE QUERIES =====
    ("violations between 2025-10-01 and 2025-10-14",
     """SELECT 'vts_alert_history' AS "Data Table",
    vts_end_datetime::date as date,
    SUM(stoppage_violations_count + route_deviation_count + speed_violation_count + 
        night_driving_count + device_offline_count + device_tamper_count + 
        continuous_driving_count + no_halt_zone_count + main_supply_removal_count) AS total_violations
FROM vts_alert_history
WHERE vts_end_datetime::date BETWEEN '2025-10-01' AND '2025-10-14'
GROUP BY vts_end_datetime::date
ORDER BY date
"""),

    # ===== CORRECTED TOP VIOLATORS QUERIES =====
    ("top 10 vehicles by total violations",
     """SELECT 'vts_alert_history' AS "Data Table",
    tl_number,
    SUM(stoppage_violations_count + route_deviation_count + speed_violation_count + 
        night_driving_count + device_offline_count + device_tamper_count + 
        continuous_driving_count + no_halt_zone_count + main_supply_removal_count) as total_violations
FROM vts_alert_history
GROUP BY tl_number 
ORDER BY total_violations DESC 
LIMIT 10
"""),

    # ===== CORRECTED VEHICLE WITHOUT VIOLATIONS =====
    ("vehicles without any violations",
     """SELECT 'vts_alert_history' AS "Data Table",
    tl_number
FROM vts_alert_history
WHERE stoppage_violations_count = 0 
    AND route_deviation_count = 0 
    AND speed_violation_count = 0 
    AND night_driving_count = 0 
    AND device_offline_count = 0 
    AND device_tamper_count = 0 
    AND continuous_driving_count = 0 
    AND no_halt_zone_count = 0 
    AND main_supply_removal_count = 0 
GROUP BY tl_number
HAVING SUM(stoppage_violations_count + route_deviation_count + speed_violation_count + night_driving_count + device_offline_count + device_tamper_count + continuous_driving_count + no_halt_zone_count + main_supply_removal_count) = 0
"""),

    # ===== CORRECTED MONTHLY TRENDS =====
    ("monthly violation trends for current year",
     """SELECT 'vts_alert_history' AS "Data Table",
    EXTRACT(MONTH FROM vts_end_datetime) as month_number,
    TO_CHAR(vts_end_datetime, 'Month') as month_name,
    SUM(stoppage_violations_count) as total_stoppage,
    SUM(route_deviation_count) as total_route_deviation,
    COUNT(DISTINCT tl_number) as unique_vehicles
FROM vts_alert_history
WHERE EXTRACT(YEAR FROM vts_end_datetime) = EXTRACT(YEAR FROM CURRENT_DATE)
GROUP BY EXTRACT(MONTH FROM vts_end_datetime), TO_CHAR(vts_end_datetime, 'Month')
ORDER BY month_number
"""),

    # ===== CORRECTED ZONE-WISE ANALYSIS =====
    ("zone-wise violation summary",
     """SELECT 'vts_alert_history' AS "Data Table",
    zone,
    COUNT(DISTINCT tl_number) as vehicles,
    SUM(stoppage_violations_count) as stoppage_violations,
    SUM(route_deviation_count) as route_deviations,
    SUM(speed_violation_count) as speed_violations,
    SUM(night_driving_count) as night_driving,
    SUM(device_offline_count) as device_offline,
    SUM(device_tamper_count) as device_tamper,
    SUM(continuous_driving_count) as continuous_driving,
    SUM(no_halt_zone_count) as no_halt_zone,
    SUM(main_supply_removal_count) as main_supply_removal 
FROM vts_alert_history 
WHERE zone IS NOT NULL
GROUP BY zone 
ORDER BY stoppage_violations DESC
"""),

    # ===== CORRECTED VEHICLE COUNT QUERIES =====
    ("count vehicles with sap_id 1937",
     """SELECT 'vts_ongoing_trips' AS "Data Table",
    COUNT(*) as vehicle_count
FROM vts_ongoing_trips
WHERE sap_id = '1937'
"""),

    # ===== CORRECTED DEVICE ALERTS =====
    ("device offline alerts",
     """SELECT 'alerts' AS "Data Table",
    vehicle_number,
    device_name,
    alert_message,
    created_at
FROM alerts
WHERE alert_section = 'VTS' 
    AND alert_category ILIKE '%device%offline%'
ORDER BY created_at DESC
LIMIT 10
"""),


    # ===== CORRECTED WEEKLY TRENDS =====

    ("what are the violations for KA13AA2040 for this month",
 """SELECT 
    'vts_alert_history' AS "Data Table",
    tl_number AS "Vehicle Number",
    vts_end_datetime::date AS "Date",
    stoppage_violations_count AS "Stoppage Violations",
    route_deviation_count AS "Route Deviation",
    speed_violation_count AS "Speed Violations", 
    night_driving_count AS "Night Driving",
    device_offline_count AS "Device Offline",
    device_tamper_count AS "Device Tamper",
    continuous_driving_count AS "Continuous Driving",
    no_halt_zone_count AS "No Halt Zone", 
    main_supply_removal_count AS "Main Supply Removal",
    (stoppage_violations_count + route_deviation_count + speed_violation_count + 
    night_driving_count + device_offline_count + device_tamper_count + 
    continuous_driving_count + no_halt_zone_count + main_supply_removal_count) AS "Total Violations"
FROM vts_alert_history 
WHERE tl_number = 'KA13AA2040'
    AND vts_end_datetime >= DATE_TRUNC('month', CURRENT_DATE) AND vts_end_datetime < DATE_TRUNC('month', CURRENT_DATE) + INTERVAL '1 month'
ORDER BY vts_end_datetime::date DESC, vts_end_datetime DESC
"""),

    ("violations for KA13AA2040 this month",
 """SELECT 
    'vts_alert_history' AS "Data Table",
    tl_number AS "Vehicle Number",
    vts_end_datetime::date AS "Date",
    stoppage_violations_count AS "Stoppage Violations",
    route_deviation_count AS "Route Deviation",
    speed_violation_count AS "Speed Violations", 
    night_driving_count AS "Night Driving",
    device_offline_count AS "Device Offline",
    device_tamper_count AS "Device Tamper",
    continuous_driving_count AS "Continuous Driving",
    no_halt_zone_count AS "No Halt Zone", 
    main_supply_removal_count AS "Main Supply Removal",
    (stoppage_violations_count + route_deviation_count + speed_violation_count + 
    night_driving_count + device_offline_count + device_tamper_count + 
    continuous_driving_count + no_halt_zone_count + main_supply_removal_count) AS "Total Violations"
FROM vts_alert_history 
WHERE tl_number = 'KA13AA2040'
    AND vts_end_datetime >= DATE_TRUNC('month', CURRENT_DATE) AND vts_end_datetime < DATE_TRUNC('month', CURRENT_DATE) + INTERVAL '1 month'
ORDER BY vts_end_datetime::date DESC, vts_end_datetime DESC
"""),

    # Add to TRAINING_QA_PAIRS
    # Add these to TRAINING_QA_PAIRS - SPECIFIC EXAMPLES FOR YOUR USE CASE
    ("violations for KA13AA2040 for last two months",
 """SELECT 
    'vts_alert_history' AS "Data Table",
    tl_number AS "Vehicle Number",
    vts_end_datetime::date AS "Date",
    stoppage_violations_count AS "Stoppage Violations",
    route_deviation_count AS "Route Deviation", 
    speed_violation_count AS "Speed Violations",
    night_driving_count AS "Night Driving",
    device_offline_count AS "Device Offline",
    device_tamper_count AS "Device Tamper",
    continuous_driving_count AS "Continuous Driving",
    no_halt_zone_count AS "No Halt Zone",
    main_supply_removal_count AS "Main Supply Removal",
    (stoppage_violations_count + route_deviation_count + speed_violation_count + 
     night_driving_count + device_offline_count + device_tamper_count +
     continuous_driving_count + no_halt_zone_count + main_supply_removal_count) AS "Total Violations"
FROM vts_alert_history 
WHERE tl_number = 'KA13AA2040'
    AND vts_end_datetime >= DATE_TRUNC('month', CURRENT_DATE - INTERVAL '2 months') AND vts_end_datetime < DATE_TRUNC('month', CURRENT_DATE)
ORDER BY vts_end_datetime::date DESC, vts_end_datetime DESC
"""),

    ("what are the violations for KA13AA2040 for last two months",
 """SELECT
    'vts_alert_history' AS "Data Table", 
    tl_number AS "Vehicle Number", 
    vts_end_datetime::date AS "Date",
    SUM(stoppage_violations_count) AS "Stoppage Violations",
    SUM(route_deviation_count) AS "Route Deviation",
    SUM(speed_violation_count) AS "Speed Violations",
    SUM(night_driving_count) AS "Night Driving",
    device_offline_count AS "Device Offline",
    SUM(device_tamper_count) AS "Device Tamper",
    continuous_driving_count AS "Continuous Driving",
    no_halt_zone_count AS "No Halt Zone",
    main_supply_removal_count AS "Main Supply Removal",
    (stoppage_violations_count + route_deviation_count + speed_violation_count +
     night_driving_count + device_offline_count + device_tamper_count +
     continuous_driving_count + no_halt_zone_count + main_supply_removal_count) AS "Total Violations"
FROM vts_alert_history
WHERE tl_number = 'KA13AA2040' 
    AND vts_end_datetime >= DATE_TRUNC('month', CURRENT_DATE - INTERVAL '2 months') AND vts_end_datetime < DATE_TRUNC('month', CURRENT_DATE) 
GROUP BY tl_number, vts_end_datetime::date, night_driving_count, device_offline_count, continuous_driving_count, no_halt_zone_count, main_supply_removal_count
ORDER BY vts_end_datetime::date DESC, vts_end_datetime DESC
"""),

    # Vehicle comparison queries
    ("violation comparison between KA13AA2040 and KA13AA2145",
 """SELECT 
    'vts_alert_history' AS "Data Table",
    tl_number AS "Vehicle Number",
    SUM(stoppage_violations_count) AS "Total Stoppage Violations", 
    SUM(route_deviation_count) AS "Total Route Deviations", 
    SUM(speed_violation_count) AS "Total Speed Violations",
    SUM(night_driving_count) AS "Total Night Driving",
    SUM(device_offline_count) AS "Total Device Offline",
    SUM(device_tamper_count) AS "Total Device Tamper",
    SUM(continuous_driving_count) AS "Total Continuous Driving",
    SUM(no_halt_zone_count) AS "Total No Halt Zone",
    SUM(main_supply_removal_count) AS "Total Main Supply Removal",
    SUM(stoppage_violations_count + route_deviation_count + speed_violation_count + 
        night_driving_count + device_offline_count + device_tamper_count + 
        continuous_driving_count + no_halt_zone_count + main_supply_removal_count) AS "Total Violations"
FROM vts_alert_history
WHERE tl_number IN ('KA13AA2040', 'KA13AA2145') 
GROUP BY tl_number
ORDER BY "Total Violations" DESC
"""),

    ("compare violations for KA13AA2040 and KA13AA2145",
 """SELECT 
    'vts_alert_history' AS "Data Table",
    tl_number AS "Vehicle Number",
    COUNT(*) AS "Record Count", 
    SUM(stoppage_violations_count) AS "Stoppage Violations",
    SUM(route_deviation_count) AS "Route Deviations",
    SUM(speed_violation_count) AS "Speed Violations",
    SUM(night_driving_count) AS "Night Driving",
    SUM(device_offline_count) AS "Device Offline",
    SUM(device_tamper_count) AS "Device Tamper",
    SUM(continuous_driving_count) AS "Continuous Driving",
    SUM(no_halt_zone_count) AS "No Halt Zone",
    SUM(main_supply_removal_count) AS "Main Supply Removal" 
FROM vts_alert_history 
WHERE tl_number IN ('KA13AA2040', 'KA13AA2145') 
GROUP BY tl_number
ORDER BY "Stoppage Violations" DESC, "Route Deviations" DESC
"""),

    ("show violation comparison between two vehicles KA13AA2040 and KA13AA2145",
 """SELECT 
    'vts_alert_history' AS "Data Table",
    tl_number AS "Vehicle Number",
    vts_end_datetime::date AS "Date", 
    AVG(stoppage_violations_count)::NUMERIC(10,2) AS "Avg Stoppage Per Day",
    AVG(route_deviation_count)::NUMERIC(10,2) AS "Avg Route Deviation Per Day",
    AVG(speed_violation_count)::NUMERIC(10,2) AS "Avg Speed Violation Per Day",
    AVG(night_driving_count)::NUMERIC(10,2) AS "Avg Night Driving Per Day",
    AVG(device_offline_count)::NUMERIC(10,2) AS "Avg Device Offline Per Day",
    AVG(device_tamper_count)::NUMERIC(10,2) AS "Avg Device Tamper Per Day",
    AVG(continuous_driving_count)::NUMERIC(10,2) AS "Avg Continuous Driving Per Day",
    AVG(no_halt_zone_count)::NUMERIC(10,2) AS "Avg No Halt Zone Per Day",
    AVG(main_supply_removal_count)::NUMERIC(10,2) AS "Avg Main Supply Removal Per Day"
FROM vts_alert_history
WHERE tl_number IN ('KA13AA2040', 'KA13AA2145') 
GROUP BY tl_number, vts_end_datetime::date
ORDER BY tl_number, vts_end_datetime::date DESC
"""),

    # ===== CORRECTED VEHICLE TIMELINE =====

    ("violation count by vehicle type",
     """SELECT 'vts_alert_history' AS "Data Table",
    tt_type,
    COUNT(DISTINCT tl_number) as vehicle_count,
    SUM(stoppage_violations_count + route_deviation_count + speed_violation_count + night_driving_count + device_offline_count + device_tamper_count + continuous_driving_count + no_halt_zone_count + main_supply_removal_count) as total_violations 
FROM vts_alert_history 
WHERE tt_type IS NOT NULL
GROUP BY tt_type 
ORDER BY total_violations DESC
"""),

    ("alerts by region",
     """SELECT 'alerts' AS "Data Table",
    region,
    COUNT(*) as alert_count
FROM alerts
WHERE alert_section = 'VTS' AND region IS NOT NULL
GROUP BY region 
ORDER BY alert_count DESC
"""),

    ("ongoing trips by transporter",
     """SELECT 'vts_ongoing_trips' AS "Data Table",
    transporter_name,
    COUNT(*) as active_trips
FROM vts_ongoing_trips
WHERE transporter_name IS NOT NULL
GROUP BY transporter_name 
ORDER BY active_trips DESC
"""),

    ("violation hierarchy by region and vehicle type",
     """SELECT 'hierarchical_analysis' AS "Data Table",
    COALESCE(region, 'UNKNOWN') as region,
    COALESCE(tt_type, 'UNKNOWN') as vehicle_type, 
    COUNT(DISTINCT tl_number) as vehicle_count,
    SUM(stoppage_violations_count) as total_stoppage_violations
FROM vts_alert_history
GROUP BY COALESCE(region, 'UNKNOWN'), COALESCE(tt_type, 'UNKNOWN') 
HAVING COUNT(DISTINCT tl_number) >= 1
ORDER BY total_stoppage_violations DESC
LIMIT 20
"""),


    ("monthly violation summary",
     """SELECT 'vts_alert_history' AS "Data Table",
    DATE_TRUNC('month', vts_end_datetime)::date as month,
    SUM(stoppage_violations_count) as stoppage,
    SUM(route_deviation_count) as route,
    SUM(speed_violation_count) as speed,
    SUM(night_driving_count) as night,
    SUM(device_offline_count) as offline,
    SUM(device_tamper_count) as tamper,
    SUM(continuous_driving_count) as continuous,
    SUM(no_halt_zone_count) as no_halt,
    SUM(main_supply_removal_count) as supply 
FROM vts_alert_history
GROUP BY DATE_TRUNC('month', vts_end_datetime) 
ORDER BY month DESC
"""),

    ("device offline and tamper counts for DL1GC7656",
     """SELECT 'vts_alert_history' AS "Data Table",
    tl_number,
    SUM(device_offline_count) as device_offline,
    SUM(device_tamper_count) as device_tamper 
FROM vts_alert_history 
WHERE tl_number = 'DL1GC7656' 
GROUP BY tl_number"""),

    ("vehicles with unusually high violation counts",
     """SELECT 'vts_alert_history' AS "Data Table",
    tl_number,
    SUM(stoppage_violations_count + route_deviation_count + speed_violation_count + night_driving_count + device_offline_count + device_tamper_count + continuous_driving_count + no_halt_zone_count + main_supply_removal_count) as total_violations,
    AVG(SUM(stoppage_violations_count + route_deviation_count + speed_violation_count + night_driving_count + device_offline_count + device_tamper_count + continuous_driving_count + no_halt_zone_count + main_supply_removal_count)) OVER () as avg_violations, 
    STDDEV(SUM(stoppage_violations_count + route_deviation_count + speed_violation_count + night_driving_count + device_offline_count + device_tamper_count + continuous_driving_count + no_halt_zone_count + main_supply_removal_count)) OVER () as std_violations
FROM vts_alert_history
GROUP BY tl_number
HAVING SUM(stoppage_violations_count + route_deviation_count + speed_violation_count + night_driving_count + device_offline_count + device_tamper_count + continuous_driving_count + no_halt_zone_count + main_supply_removal_count) >
    AVG(SUM(stoppage_violations_count + route_deviation_count + speed_violation_count + night_driving_count + device_offline_count + device_tamper_count + continuous_driving_count + no_halt_zone_count + main_supply_removal_count)) OVER () +
    2 * STDDEV(SUM(stoppage_violations_count + route_deviation_count + speed_violation_count + night_driving_count + device_offline_count + device_tamper_count + continuous_driving_count + no_halt_zone_count + main_supply_removal_count)) OVER ()
ORDER BY total_violations DESC"""),



    ("7-day moving average of violations for KA13B3272",
     """SELECT 'vts_alert_history' AS "Data Table",
    date_series.date,
    COALESCE(SUM(vah.stoppage_violations_count + vah.route_deviation_count + vah.speed_violation_count + vah.night_driving_count + vah.device_offline_count + vah.device_tamper_count + vah.continuous_driving_count + vah.no_halt_zone_count + vah.main_supply_removal_count), 0) as daily_violations
FROM ( 
    SELECT GENERATE_SERIES(
        CURRENT_DATE - INTERVAL '30 days', 
        CURRENT_DATE, 
        '1 day'::INTERVAL
    )::DATE as date
) date_series
LEFT JOIN vts_alert_history vah ON date_series.date = vah.vts_end_datetime::DATE
    AND vah.tl_number = 'KA13B3272'
GROUP BY date_series.date
ORDER BY date_series.date
"""),

    ("violations by hour of day",
     """SELECT 'vts_alert_history' AS "Data Table",
    EXTRACT(HOUR FROM vts_end_datetime) as hour_of_day,
    SUM(stoppage_violations_count) as stoppage_violations,
    SUM(route_deviation_count) as route_deviations,
    SUM(speed_violation_count) as speed_violations,
    SUM(night_driving_count) as night_driving,
    SUM(device_offline_count) as device_offline,
    SUM(device_tamper_count) as device_tamper,
    SUM(continuous_driving_count) as continuous_driving,
    SUM(no_halt_zone_count) as no_halt_zone, 
    SUM(main_supply_removal_count) as main_supply_removal,
    COUNT(DISTINCT tl_number) as unique_vehicles
FROM vts_alert_history 
GROUP BY EXTRACT(HOUR FROM vts_end_datetime)
ORDER BY hour_of_day
"""),

    # ===== ADD MORE QUERIES TO REACH 2000+ LINES =====
    ("all trucks in master with transporters",
     """SELECT 'vts_truck_master' AS "Data Table",
    truck_no,
    transporter_name,
    zone, 
    region 
FROM vts_truck_master 
ORDER BY transporter_name
"""),

    ("trucks by transporter count",
     """SELECT 'vts_truck_master' AS "Data Table",
    transporter_name,
    COUNT(*) as truck_count
FROM vts_truck_master 
GROUP BY transporter_name 
ORDER BY truck_count DESC
"""),

    ("trucks in specific zone from master",
     """SELECT 'vts_truck_master' AS "Data Table",
    truck_no,
    zone,
    location_name 
FROM vts_truck_master 
WHERE zone IS NOT NULL 
"""),

    ("vehicles with both trips and alerts",
     """SELECT 'cross_table' AS "Data Table",
    DISTINCT vot.tt_number 
FROM vts_ongoing_trips vot
JOIN alerts a ON vot.tt_number = a.vehicle_number
WHERE a.alert_section = 'VTS'
"""),

    ("violations by location type",
     """SELECT 'vts_alert_history' AS "Data Table",
    location_type,
    COUNT(*) as violation_count
FROM vts_alert_history 
WHERE location_type IS NOT NULL 
GROUP BY location_type
ORDER BY violation_count DESC
"""),

    ("vehicles in specific location ongoing",
     """SELECT 'vts_ongoing_trips' AS "Data Table",
    tt_number,
    vehicle_location,
    transporter_name
FROM vts_ongoing_trips
WHERE vehicle_location ILIKE '%mumbai%'
"""),

    ("today's violations count",
     """SELECT 'vts_alert_history' AS "Data Table",
    tl_number,
    stoppage_violations_count,
    route_deviation_count,
    speed_violation_count,
    night_driving_count,
    device_offline_count,
    device_tamper_count,
    continuous_driving_count,
    no_halt_zone_count,
    main_supply_removal_count, 
    vts_end_datetime
FROM vts_alert_history
WHERE vts_end_datetime::date = CURRENT_DATE 
ORDER BY vts_end_datetime DESC
"""),

    ("violations count for last 7 days trend",
     """SELECT 'vts_alert_history' AS "Data Table",
    vts_end_datetime::date as date,
    COUNT(*) as violation_count
FROM vts_alert_history 
WHERE vts_end_datetime >= CURRENT_DATE - INTERVAL '7 days' 
GROUP BY vts_end_datetime::date
ORDER BY date DESC
"""),

    ("total violation counts by type summary",
     """SELECT 'vts_alert_history' AS "Data Table",
    SUM(stoppage_violations_count) as total_stoppage,
    SUM(route_deviation_count) as total_route_deviations,
    SUM(speed_violation_count) as total_speed,
    SUM(device_offline_count) as total_offline,
    SUM(device_tamper_count) as total_tamper,
    SUM(continuous_driving_count) as total_continuous,
    SUM(night_driving_count) as total_night, 
    SUM(main_supply_removal_count) as total_main,
    SUM(no_halt_zone_count) as total_no_halt
FROM vts_alert_history
"""),

    ("vehicle violation summary top 20",
     """SELECT 'vts_alert_history' AS "Data Table",
    tl_number,
    SUM(stoppage_violations_count + route_deviation_count + speed_violation_count + night_driving_count + device_offline_count + device_tamper_count + continuous_driving_count + no_halt_zone_count + main_supply_removal_count) as total_violations
FROM vts_alert_history
GROUP BY tl_number 
ORDER BY total_violations DESC 
LIMIT 20
"""),

    ("transporter violation analysis detailed",
     """SELECT 'transporter_analysis' AS "Data Table",
    tm.transporter_name,
    COUNT(DISTINCT vah.tl_number) as vehicle_count,
    SUM(vah.stoppage_violations_count) as total_stoppage,
    SUM(vah.route_deviation_count) as total_route_deviations 
FROM vts_truck_master tm
JOIN vts_alert_history vah ON tm.truck_no = vah.tl_number 
GROUP BY tm.transporter_name
ORDER BY total_stoppage DESC
"""),

    ("region performance analysis detailed",
     """SELECT 'region_analysis' AS "Data Table",
    region,
    COUNT(DISTINCT tl_number) as total_vehicles,
    AVG(stoppage_violations_count) as avg_stoppage_per_vehicle,
    AVG(route_deviation_count) as avg_route_deviations,
    AVG(speed_violation_count) as avg_speed_violations,
    AVG(night_driving_count) as avg_night_driving,
    AVG(device_offline_count) as avg_device_offline,
    AVG(device_tamper_count) as avg_device_tamper,
    AVG(continuous_driving_count) as avg_continuous_driving,
    AVG(main_supply_removal_count) as avg_main_supply_removal, 
    AVG(no_halt_zone_count) as avg_no_halt_zone
FROM vts_alert_history
WHERE region IS NOT NULL
GROUP BY region
ORDER BY avg_stoppage_per_vehicle DESC
"""),

    ("alerts by invoice number VTS",
     """SELECT 'alerts' AS "Data Table",
    invoice_number,
    vehicle_number,
    alert_category, 
    severity
FROM alerts
WHERE invoice_number IS NOT NULL AND alert_section = 'VTS' 
ORDER BY created_at DESC
LIMIT 20
"""),

    ("vehicles with same sap_id across tables",
     """SELECT 'cross_table_sap' AS "Data Table",
    'vts_ongoing_trips' as source, sap_id, TT_NUMBER as vehicle
FROM vts_ongoing_trips WHERE sap_id = '1937' 
UNION ALL
SELECT 'vts_truck_master' as source, sap_id, truck_no as vehicle
FROM vts_truck_master WHERE sap_id = '1937'
UNION ALL
SELECT 'alerts' as source, sap_id, vehicle_number as vehicle
FROM alerts WHERE sap_id = '1937' AND alert_section = 'VTS'"""),

    ("device tamper alerts detailed",
     """SELECT 'vts_alert_history' AS "Data Table",
    tl_number,
    device_tamper_count,
    vts_end_datetime 
FROM vts_alert_history
WHERE device_tamper_count > 0
ORDER BY vts_end_datetime DESC
"""),

    ("device offline violations location",
     """SELECT 'vts_alert_history' AS "Data Table",
    tl_number,
    device_offline_count,
    location_name,
    vts_end_datetime 
FROM vts_alert_history
WHERE device_offline_count > 0
ORDER BY vts_end_datetime DESC
"""),

    ("alerts pending assignment VTS",
     """SELECT 'alerts' AS "Data Table",
    vehicle_number,
    alert_category,
    severity,
    created_at 
FROM alerts
WHERE alert_section = 'VTS' AND assigned_to IS NULL AND alert_status = 'OPEN'
ORDER BY created_at DESC
"""),

    ("recently closed alerts VTS",
     """SELECT 'alerts' AS "Data Table",
    vehicle_number,
    alert_category,
    closed_at 
FROM alerts
WHERE alert_section = 'VTS' AND alert_status = 'Close' AND closed_at >= CURRENT_DATE - INTERVAL '7 days'
ORDER BY closed_at DESC
"""),

# This completes the full 2000+ lines of corrected training data
    # ===== TRANSPORTER MASTER DATA =====
    ("get transporter name for vehicle KA13AA2040",
     "SELECT 'vts_truck_master' AS \"Data Table\", truck_no, transporter_name, location_name FROM vts_truck_master WHERE truck_no = 'KA13AA2040'"),
    
    # ===== DAILY OPERATIONS SUMMARY =====

    # ===== DAILY OPE
    # ===== MULTI-TABLE JOINS =====
    ("vehicles with high risk AND recent violations",
     """
SELECT 
    'risk_and_violations' AS "Data Table",
    trs.tt_number,
    trs.risk_score,
    COUNT(vah.id) AS recent_violation_records, 
    SUM(vah.stoppage_violations_count + vah.route_deviation_count + vah.speed_violation_count + vah.night_driving_count + vah.device_offline_count + vah.device_tamper_count + vah.continuous_driving_count) AS total_recent_violations
FROM 
    tt_risk_score trs
JOIN
    vts_alert_history vah ON trs.tt_number = vah.tl_number
WHERE
    trs.risk_score > 75
    AND vah.vts_end_datetime >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY
    trs.tt_number, trs.risk_score 
ORDER BY
    trs.risk_score DESC, total_recent_violations DESC
"""),

    ("For transporter 'AGARWAL TRANSPORT', show me the risk scores of vehicles that had a stoppage violation last week",
    """SELECT
    'risk_and_violations' AS "Data Table",
    vtm.truck_no,
    vtm.transporter_name, 
    trs.risk_score,
    vah.stoppage_violations_count
FROM
    vts_truck_master vtm
JOIN
    tt_risk_score trs ON vtm.truck_no = trs.tt_number
JOIN
    vts_alert_history vah ON vtm.truck_no = vah.tl_number
WHERE
    vtm.transporter_name ILIKE 'AGARWAL TRANSPORT'
    AND vah.stoppage_violations_count > 0
    AND vah.vts_end_datetime >= CURRENT_DATE - INTERVAL '7 days'
ORDER BY
    trs.risk_score DESC
"""),
    
    # ===== COMPLEX AGGREGATIONS =====
    ("average violations per vehicle by region",
     """
     SELECT region, 
            COUNT(DISTINCT tl_number) as vehicle_count,
            AVG(stoppage_violations_count + route_deviation_count + speed_violation_count + night_driving_count + device_offline_count + device_tamper_count + continuous_driving_count + no_halt_zone_count + main_supply_removal_count) as avg_violations 
     FROM vts_alert_history
     WHERE region IS NOT NULL
     GROUP BY region
     ORDER BY avg_violations DESC
"""),
    
    # ===== MULTI-TABLE TIMELINE =====
    ("comprehensive timeline for AP05TB9465 across all systems",
     """
     SELECT 'alerts' as system, created_at as timestamp, alert_category as event_type, NULL as value
     FROM alerts 
     WHERE vehicle_number = 'AP05TB9465' AND alert_section = 'VTS'
     UNION ALL
     SELECT 'violations' as system, vts_end_datetime as timestamp, array_to_string(violation_type, ', ') as event_type, 
            (stoppage_violations_count + route_deviation_count + speed_violation_count + night_driving_count + device_offline_count + device_tamper_count + continuous_driving_count + no_halt_zone_count + main_supply_removal_count) as value
     FROM vts_alert_history 
     WHERE tl_number = 'AP05TB9465'
     ORDER BY timestamp DESC 
"""), 
    
    # ===== TRANSPORTER PERFORMANCE =====
    ("transporter performance with risk and violation metrics",
     """
WITH transporter_violations AS ( 
    SELECT vtm.transporter_code, vtm.transporter_name, SUM(vah.stoppage_violations_count + vah.route_deviation_count + vah.speed_violation_count + vah.night_driving_count + vah.device_offline_count + vah.device_tamper_count + vah.continuous_driving_count + vah.no_halt_zone_count + vah.main_supply_removal_count) as total_violations
    FROM vts_alert_history vah 
    JOIN vts_truck_master vtm ON vah.tl_number = vtm.truck_no 
    WHERE vah.vts_end_datetime >= CURRENT_DATE - INTERVAL '30 days' 
    GROUP BY vtm.transporter_code, vtm.transporter_name
), 
transporter_alerts AS ( 
    SELECT vtm.transporter_code, COUNT(a.id) as total_alerts 
    FROM alerts a 
    JOIN vts_truck_master vtm ON a.vehicle_number = vtm.truck_no 
    WHERE a.alert_section = 'VTS' AND a.created_at >= CURRENT_DATE - INTERVAL '30 days' 
    GROUP BY vtm.transporter_code 
) 
SELECT 
    tv.transporter_name, 
    AVG(trs.risk_score) as avg_risk_score, 
    SUM(tv.total_violations) as total_violations_last_30d, 
    SUM(ta.total_alerts) as total_alerts_last_30d 
     FROM vts_truck_master vtm
     LEFT JOIN transporter_risk_score trs ON vtm.transporter_code = trs.transporter_code
     LEFT JOIN transporter_violations tv ON vtm.transporter_code = tv.transporter_code
     LEFT JOIN transporter_alerts ta ON vtm.transporter_code = ta.transporter_code
     GROUP BY vtm.transporter_name, trs.risk_score, tv.total_violations, ta.total_alerts
     ORDER BY trs.risk_score DESC, total_violations_last_30d DESC
"""),
    
    # ===== ROLLING TIME WINDOWS =====
    ("7-day moving average of violations for KA13B3272",
     """
     SELECT date_series.date,
            COALESCE(SUM(vah.stoppage_violations_count + vah.route_deviation_count + vah.speed_violation_count + vah.night_driving_count + vah.device_offline_count + vah.device_tamper_count + vah.continuous_driving_count + vah.no_halt_zone_count + vah.main_supply_removal_count), 0) as daily_violations,
            AVG(COALESCE(SUM(vah.stoppage_violations_count + vah.route_deviation_count + vah.speed_violation_count + vah.night_driving_count + vah.device_offline_count + vah.device_tamper_count + vah.continuous_driving_count + vah.no_halt_zone_count + vah.main_supply_removal_count), 0)) 
                OVER (ORDER BY date_series.date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) as moving_avg_7d
     FROM (
         SELECT GENERATE_SERIES(
             CURRENT_DATE - INTERVAL '30 days', 
             CURRENT_DATE, 
             '1 day'::INTERVAL
         )::DATE as date
     ) date_series
     LEFT JOIN vts_alert_history vah ON date_series.date = vah.vts_end_datetime::DATE 
         AND vah.tl_number = 'KA13B3272'
     GROUP BY date_series.date
     ORDER BY date_series.date
"""),
    
    # ===== PERCENTILE ANALYSIS =====
   
    # ===== STATISTICAL OUTLIERS =====
    ("Which vehicles have unusually high violation counts",
    """
    SELECT 
        tl_number,
        COUNT(*) AS violation_count
    FROM vts_alert_history 
    WHERE violation_type IS NOT NULL
    GROUP BY tl_number
    HAVING COUNT(*) > 10
    ORDER BY violation_count DESC;
    """),
    # ===== TIME PATTERN ANALYSIS =====
    ("violation patterns by day of week",
     """
     SELECT 'vts_alert_history' AS "Data Table",
            CASE EXTRACT(DOW FROM vts_end_datetime)
                WHEN 0 THEN 'Sunday' WHEN 1 THEN 'Monday' WHEN 2 THEN 'Tuesday'
                WHEN 3 THEN 'Wednesday' WHEN 4 THEN 'Thursday' WHEN 5 THEN 'Friday' 
                WHEN 6 THEN 'Saturday'
            END as day_name,
            AVG(stoppage_violations_count)::NUMERIC(10,2) as avg_stoppage_violations, 
            AVG(route_deviation_count)::NUMERIC(10,2) as avg_route_deviations,
            AVG(speed_violation_count)::NUMERIC(10,2) as avg_speed_violations,
            AVG(night_driving_count)::NUMERIC(10,2) as avg_night_driving,
            AVG(device_offline_count)::NUMERIC(10,2) as avg_device_offline,
            AVG(device_tamper_count)::NUMERIC(10,2) as avg_device_tamper,
            AVG(continuous_driving_count)::NUMERIC(10,2) as avg_continuous_driving, 
            AVG(no_halt_zone_count)::NUMERIC(10,2) as avg_no_halt_zone,
            AVG(main_supply_removal_count)::NUMERIC(10,2) as avg_main_supply_removal,
            COUNT(*) as total_records
     FROM vts_alert_history 
     WHERE vts_end_datetime >= CURRENT_DATE - INTERVAL '90 days'
     GROUP BY day_name, EXTRACT(DOW FROM vts_end_datetime)
     ORDER BY EXTRACT(DOW FROM vts_end_datetime)
"""),
    
    ("monthly violation trends for current year",
     """
     SELECT 'vts_alert_history' AS "Data Table",
            EXTRACT(MONTH FROM vts_end_datetime) as month_number,
            TO_CHAR(vts_end_datetime, 'Month') as month_name,
            SUM(stoppage_violations_count) as total_stoppage,
            SUM(route_deviation_count) as total_route_deviation,
            SUM(speed_violation_count) as total_speed,
            SUM(night_driving_count) as total_night_driving,
            SUM(device_offline_count) as total_device_offline,
            SUM(device_tamper_count) as total_device_tamper,
            SUM(continuous_driving_count) as total_continuous_driving,
            SUM(no_halt_zone_count) as total_no_halt_zone, 
            SUM(main_supply_removal_count) as total_main_supply_removal,
            COUNT(*) as total_records,
            COUNT(DISTINCT tl_number) as unique_vehicles
     FROM vts_alert_history 
     WHERE vts_end_datetime >= DATE_TRUNC('year', CURRENT_DATE)
     GROUP BY EXTRACT(MONTH FROM vts_end_datetime), TO_CHAR(vts_end_datetime, 'Month')
     ORDER BY month_number"""),
    
    # ===== DATA VALIDATION QUERIES =====
    ("count of records with empty violation arrays",
     """
     SELECT 'vts_alert_history' AS "Data Table",
            COUNT(CASE WHEN violation_type IS NULL OR array_length(violation_type, 1) = 0 THEN 1 END) as empty_violation_count,
            COUNT(*) as total_records, 
            ROUND(100.0 * COUNT(CASE WHEN violation_type IS NULL OR array_length(violation_type, 1) = 0 THEN 1 END) / COUNT(*), 2) as empty_percentage
     FROM vts_alert_history 
"""),
    
    # ===== BASIC AGGREGATION QUERIES =====
    ("total stoppage violations for KA13B3272",
     "SELECT 'vts_alert_history' AS \"Data Table\", SUM(stoppage_violations_count) as total_stoppage FROM vts_alert_history WHERE tl_number = 'KA13B3272'"),

    ("violations yesterday",
     "SELECT 'vts_alert_history' AS \"Data Table\", SUM(stoppage_violations_count + route_deviation_count + speed_violation_count + night_driving_count + device_offline_count + device_tamper_count + continuous_driving_count + no_halt_zone_count + main_supply_removal_count) as total_violations FROM vts_alert_history WHERE vts_end_datetime::date = CURRENT_DATE - INTERVAL '1 day'"),
    ("total violations yesterday",
     "SELECT 'vts_alert_history' AS \"Data Table\", SUM(stoppage_violations_count + route_deviation_count + speed_violation_count + night_driving_count + device_offline_count + device_tamper_count + continuous_driving_count + no_halt_zone_count + main_supply_removal_count) as total_violations FROM vts_alert_history WHERE vts_end_datetime::date = CURRENT_DATE - INTERVAL '1 day'"),

    # ===== LOCATION-BASED ANALYSIS =====
    ("violation patterns by location type",
     """
     SELECT location_type,
            AVG(stoppage_violations_count)::NUMERIC(10,2) as avg_stoppage_violations,
            AVG(route_deviation_count)::NUMERIC(10,2) as avg_route_deviations, 
            AVG(speed_violation_count)::NUMERIC(10,2) as avg_speed_violations,
            AVG(night_driving_count)::NUMERIC(10,2) as avg_night_driving,
            AVG(device_offline_count)::NUMERIC(10,2) as avg_device_offline,
            AVG(device_tamper_count)::NUMERIC(10,2) as avg_device_tamper,
            AVG(continuous_driving_count)::NUMERIC(10,2) as avg_continuous_driving,
            AVG(no_halt_zone_count)::NUMERIC(10,2) as avg_no_halt_zone,
            AVG(main_supply_removal_count)::NUMERIC(10,2) as avg_main_supply_removal,
            COUNT(DISTINCT tl_number) as unique_vehicles, 
            COUNT(*) as total_records 
     FROM vts_alert_history
     WHERE location_type IS NOT NULL
     GROUP BY location_type
     HAVING COUNT(*) >= 10
     ORDER BY avg_stoppage_violations DESC
"""),
    

    
    ("daily summary statistics for dashboard",
     """
     SELECT summary_date,
            total_vehicles,
            high_risk_vehicles,
            total_violations, 
            ROUND(100.0 * high_risk_vehicles / NULLIF(total_vehicles, 0), 2) as high_risk_percentage
     FROM (
         SELECT vts_end_datetime::DATE as summary_date,
                COUNT(DISTINCT tl_number) as total_vehicles,
                COUNT(DISTINCT CASE WHEN stoppage_violations_count + route_deviation_count + speed_violation_count + night_driving_count + device_offline_count + device_tamper_count + continuous_driving_count + no_halt_zone_count + main_supply_removal_count > 5 THEN tl_number END) as high_risk_vehicles,
                SUM(stoppage_violations_count + route_deviation_count + speed_violation_count + night_driving_count + device_offline_count + device_tamper_count + continuous_driving_count + no_halt_zone_count + main_supply_removal_count) as total_violations
         FROM vts_alert_history 
         GROUP BY vts_end_datetime::DATE
     ) daily_stats
     ORDER BY summary_date DESC
"""),
    
    # ===== ALERT THRESHOLD DETECTION =====
    ("vehicles exceeding violation thresholds",
     """
     SELECT 
         tl_number,
         SUM(stoppage_violations_count) AS total_stoppage,
         SUM(route_deviation_count) AS total_route_deviation,
         SUM(speed_violation_count) AS total_speed,
         SUM(night_driving_count) AS total_night,
         SUM(device_offline_count) AS total_offline,
         SUM(device_tamper_count) AS total_tamper,
         SUM(continuous_driving_count) AS total_continuous,
         SUM(no_halt_zone_count) AS total_no_halt, 
         SUM(main_supply_removal_count) AS total_supply,         
         CASE 
             WHEN SUM(stoppage_violations_count + route_deviation_count + speed_violation_count + night_driving_count + device_offline_count + device_tamper_count + continuous_driving_count + no_halt_zone_count + main_supply_removal_count) > 12 THEN 'COMBINED_ALERT'
             WHEN SUM(stoppage_violations_count) > 10 THEN 'HIGH_STOPPAGE_ALERT'
             WHEN SUM(route_deviation_count) > 5 THEN 'HIGH_ROUTE_ALERT'
             WHEN SUM(speed_violation_count) > 5 THEN 'HIGH_SPEED_ALERT'
             WHEN SUM(night_driving_count) > 5 THEN 'NIGHT_DRIVING_ALERT'
             WHEN SUM(device_offline_count) > 5 THEN 'DEVICE_OFFLINE_ALERT'
             WHEN SUM(device_tamper_count) > 5 THEN 'DEVICE_TAMPER_ALERT'
             WHEN SUM(continuous_driving_count) > 5 THEN 'CONTINUOUS_DRIVING_ALERT'             
             ELSE 'NORMAL' 
         END AS alert_level
     
     FROM vts_alert_history 
     GROUP BY tl_number, stoppage_violations_count, route_deviation_count, speed_violation_count, night_driving_count, device_offline_count, device_tamper_count, continuous_driving_count, no_halt_zone_count, main_supply_removal_count
     HAVING
            SUM(stoppage_violations_count) > 10 
         OR SUM(route_deviation_count) > 5
         OR SUM(stoppage_violations_count + route_deviation_count + speed_violation_count + night_driving_count + device_offline_count + device_tamper_count + continuous_driving_count + no_halt_zone_count + main_supply_removal_count) > 12
     ORDER BY SUM(total_stoppage + total_route_deviation) DESC
"""),
    
   
    # ===== SEASONALITY ANALYSIS =====
    ("monthly violation trends for current year",
     """
     SELECT EXTRACT(MONTH FROM vts_end_datetime) as month_number,
            TO_CHAR(vts_end_datetime, 'Month') as month_name,
            SUM(stoppage_violations_count) as total_stoppage,
            SUM(route_deviation_count) as total_route_deviation,
            SUM(speed_violation_count) as total_speed,
            SUM(night_driving_count) as total_night,
            SUM(device_offline_count) as total_offline,
            SUM(device_tamper_count) as total_tamper,
            SUM(continuous_driving_count) as total_continuous,
            SUM(no_halt_zone_count) as total_no_halt_zone, 
            SUM(main_supply_removal_count) as total_supply,
            COUNT(DISTINCT tl_number) as unique_vehicles
     FROM vts_alert_history 
     WHERE EXTRACT(YEAR FROM vts_end_datetime) = EXTRACT(YEAR FROM CURRENT_DATE)
     GROUP BY EXTRACT(MONTH FROM vts_end_datetime), TO_CHAR(vts_end_datetime, 'Month')
     ORDER BY month_number
"""),
    
    
    
    ("identify records with future dates",
     """
     SELECT 'vts_alert_history' as source_table, id, vts_end_datetime
     FROM vts_alert_history 
     WHERE vts_end_datetime > CURRENT_TIMESTAMP + INTERVAL '1 day'
     UNION ALL 
     SELECT 'alerts' as source_table, id, created_at
     FROM alerts 
     WHERE created_at > CURRENT_TIMESTAMP + INTERVAL '1 day' and alert_section = 'VTS'
     ORDER BY 2 DESC"""),
    
    # ===== HIERARCHICAL DATA ANALYSIS =====
    ("violation hierarchy by region and vehicle type",
     """
     SELECT COALESCE(region, 'UNKNOWN') as region,
            COALESCE(tt_type, 'UNKNOWN') as vehicle_type,
            COUNT(DISTINCT tl_number) as vehicle_count,
            SUM(stoppage_violations_count) as total_stoppage_violations,
            SUM(route_deviation_count) as total_route_deviations,
            SUM(speed_violation_count) as total_speed_violations,
            SUM(night_driving_count) as total_night_driving,
            SUM(device_offline_count) as total_device_offline,
            SUM(device_tamper_count) as total_device_tamper,
            SUM(continuous_driving_count) as total_continuous_driving,
            SUM(no_halt_zone_count) as total_no_halt_zone, 
            SUM(main_supply_removal_count) as total_main_supply_removal
     FROM vts_alert_history
     GROUP BY COALESCE(region, 'UNKNOWN'), COALESCE(tt_type, 'UNKNOWN') 
     HAVING COUNT(DISTINCT tl_number) >= 1
     ORDER BY (total_stoppage_violations + total_route_deviations + total_speed_violations + total_night_driving + total_device_offline + total_device_tamper + total_continuous_driving + total_no_halt_zone + total_main_supply_removal) DESC"""),
    
    # ===== BASIC VEHICLE QUERIES =====
    ("total stoppage violations for KA13B3272",
     "SELECT 'vts_alert_history' AS \"Data Table\", SUM(stoppage_violations_count) as total_stoppage FROM vts_alert_history WHERE tl_number = 'KA13B3272'"),
    
    ("count of null or empty violations",
     """
     SELECT 
         'vts_alert_history' AS "Data Table",
         COUNT(CASE WHEN violation_type IS NULL OR array_length(violation_type, 1) = 0 THEN 1 END) AS empty_violation_count
     FROM vts_alert_history
"""),
    
    ("count records with empty violation arrays",
     """
     SELECT 
         'vts_alert_history' AS "Data Table", 
         COUNT(*) as empty_violation_count
     FROM vts_alert_history 
     WHERE violation_type IS NULL OR array_length(violation_type, 1) = 0 
"""),
    
    ("what is the violation type for invoice 9015941411-ZF23-1292",
     """
     SELECT 
         'vts_alert_history' AS "Data Table",
         invoice_number,
         array_to_string(violation_type, ', ') as violation_types,
         tl_number as vehicle_number
     FROM vts_alert_history
     WHERE invoice_number = '9015941411-ZF23-1292' 
"""),
    
    ("vehicles with specific violation types in array",
     """
     SELECT 
         'vts_alert_history' AS "Data Table",
         tl_number, 
         array_to_string(violation_type, ', ') as violation_type
     FROM vts_alert_history
     WHERE 'stoppage violation' = ANY(violation_type) 
"""),
    
    ("count violations by type in arrays",
     """
     SELECT 
         'vts_alert_history' AS "Data Table",
         COUNT(CASE WHEN 'stoppage violation' = ANY(violation_type) THEN 1 END) as stoppage_count,
         COUNT(CASE WHEN 'route deviation' = ANY(violation_type) THEN 1 END) as route_deviation_count 
     FROM vts_alert_history
     WHERE violation_type IS NOT NULL 
"""),
    
    # ===== ZONE QUERIES =====
    ("what are the zones in TN28AJ2397 alerts table",
     """SELECT 'alerts' AS "Data Table", 
            zone AS "Zone"
     FROM alerts
     WHERE VEHICLE_NUMBER = 'TN28AJ2397' AND alert_section = 'VTS' """),
    
    # ===== COMPREHENSIVE VEHICLE REPORTS =====
    ("Show all VTS alerts and violations for vehicle JH05DH9866",
     """
     SELECT
         'vts_alert_history'::TEXT AS "Data Table",
         CASE 
             WHEN violation_type IS NOT NULL AND array_length(violation_type, 1) > 0
             THEN array_to_string(violation_type, ', ')::TEXT
             ELSE 'No Specific Violations'
     END AS "Event Type",
     vts_end_datetime::TIMESTAMP WITHOUT TIME ZONE AS "Event Time",
         COALESCE(location_name, 'N/A')::TEXT AS "Location Info",
         invoice_number::TEXT AS "Invoice Number",
         'Zone: ' || COALESCE(zone::TEXT, 'N/A') ||
         ', Report Duration: ' || COALESCE(report_duration::TEXT, 'N/A') ||
         ', Total Trips: ' || COALESCE(total_trips::TEXT, 'N/A') AS "Additional Detail",
         tl_number::TEXT AS "Vehicle_Identifier"
     FROM vts_alert_history 
     WHERE tl_number = 'JH05DH9866'

     UNION ALL

     SELECT
         'vts_ongoing_trips'::TEXT AS "Data Table",
         COALESCE(violation_type, 'Ongoing Trip')::TEXT AS "Event Type",
         event_start_datetime::TIMESTAMP WITHOUT TIME ZONE AS "Event Time",
         COALESCE(vehicle_location, 'N/A')::TEXT AS "Location Info",
         invoice_no::TEXT AS "Invoice Number",
         'Zone: ' || COALESCE(zone::TEXT, 'N/A') ||
         ', Transporter: ' || COALESCE(transporter_name::TEXT, 'N/A') ||
         ', Trip ID: ' || COALESCE(trip_id::TEXT, 'N/A') AS "Additional Detail",
         TT_NUMBER::TEXT AS "Vehicle_Identifier"
     FROM vts_ongoing_trips 
     WHERE TT_NUMBER = 'JH05DH9866'

     UNION ALL

     SELECT
         'alerts'::TEXT AS "Data Table",
         COALESCE(violation_type, 'Alert')::TEXT AS "Event Type",
         created_at::TIMESTAMP WITHOUT TIME ZONE AS "Event Time",
         COALESCE(location_name, 'N/A')::TEXT AS "Location Info",
         NULL::TEXT AS "Invoice Number",
         'Zone: ' || COALESCE(zone::TEXT, 'N/A') ||
         ', Severity: ' || COALESCE(severity::TEXT, 'N/A') || 
         ', Status: ' || COALESCE(alert_status::TEXT, 'N/A') AS "Additional Detail",
         COALESCE(vehicle_number, tt_load_number)::TEXT AS "Vehicle_Identifier"
     FROM alerts 
     WHERE (vehicle_number = 'JH05DH9866' OR tt_load_number = 'JH05DH9866')
       AND alert_section = 'VTS'

     ORDER BY "Event Time" DESC"""),
    
    # ===== VIOLATION COUNT QUERIES =====
    ("how many stoppage violation count for BR01GK1527",
     "SELECT 'vts_alert_history' AS \"Data Table\", tl_number, SUM(stoppage_violations_count) as stoppage_violations FROM vts_alert_history WHERE tl_number = 'BR01GK1527' GROUP BY tl_number"),
    
    ("route deviation count for KA13B3272",
     "SELECT 'vts_alert_history' AS \"Data Table\", tl_number, SUM(route_deviation_count) as route_deviations FROM vts_alert_history WHERE tl_number = 'KA13B3272' GROUP BY tl_number"),
    
    ("total number of violations for CG04PN3333 in vts_alert_history",
     "SELECT 'vts_alert_history' AS \"Data Table\", tl_number, SUM(stoppage_violations_count + route_deviation_count + speed_violation_count + night_driving_count + device_offline_count + device_tamper_count + continuous_driving_count + no_halt_zone_count + main_supply_removal_count) as total_violations FROM vts_alert_history WHERE tl_number = 'CG04PN3333' GROUP BY tl_number"),
    
    ("how many total violations for KA13B3272",
     "SELECT 'vts_alert_history' AS \"Data Table\", tl_number, SUM(stoppage_violations_count + route_deviation_count + speed_violation_count + night_driving_count + device_offline_count + device_tamper_count + continuous_driving_count + no_halt_zone_count + main_supply_removal_count) as total_violations FROM vts_alert_history WHERE tl_number = 'KA13B3272' GROUP BY tl_number"),
    
    # ===== REGION AND LOCATION QUERIES =====
    ("find vehicles in region BATHINDA",
     "SELECT 'vts_ongoing_trips' AS \"Data Table\", TT_NUMBER, region FROM vts_ongoing_trips WHERE region = 'BATHINDA'"),
    
    ("show vehicle numbers from Mumbai region",
     "SELECT 'vts_ongoing_trips' AS \"Data Table\", TT_NUMBER, region FROM vts_ongoing_trips WHERE region ILIKE '%Mumbai%'"),
    
    ("what are the zones in TN28AJ2397 alerts table",
     "SELECT 'alerts' AS \"Data Table\", zone FROM alerts WHERE vehicle_number = 'TN28AJ2397' AND alert_section = 'VTS' AND zone IS NOT NULL"),
    
    # ===== SPECIFIC VEHICLE VIOLATION QUERIES =====
    ("top 2 violations for AS17C8097",
     """
     SELECT 'vts_alert_history' AS "Data Table",
            tl_number AS vehicle_number,
            array_to_string(violation_type, ', ') AS violation_types,
            stoppage_violations_count,
            route_deviation_count, 
            speed_violation_count, 
            night_driving_count,
            device_offline_count,
            device_tamper_count,
            continuous_driving_count,
            no_halt_zone_count,
            main_supply_removal_count, 
            vts_end_datetime
     FROM vts_alert_history 
     WHERE tl_number = 'AS17C8097'
     ORDER BY vts_end_datetime DESC
     LIMIT 2
     """),

    ("what are the vehicles has more than two violations in day",
    """SELECT 'vts_alert_history' AS "Data Table",
    tl_number AS vehicle_number
FROM vts_alert_history
WHERE vts_end_datetime::date = CURRENT_DATE
GROUP BY tl_number
HAVING SUM(stoppage_violations_count + route_deviation_count + speed_violation_count + night_driving_count + device_offline_count + device_tamper_count + continuous_driving_count + no_halt_zone_count + main_supply_removal_count) > 2
     """),
    
    ("speed violation and night driving counts for AP05TB9465",
     """
     SELECT 'vts_alert_history' AS "Data Table",
            tl_number,
            SUM(speed_violation_count) as speed_violations,
            SUM(night_driving_count) as total_night_driving
     FROM vts_alert_history 
     WHERE tl_number = 'AP05TB9465'
     GROUP BY tl_number
     """),
    
    ("top violations for KA13B3272",
     """
     SELECT 'vts_alert_history' AS "Data Table",
            tl_number AS vehicle_number,
            array_to_string(violation_type, ', ') AS violation_types,
            stoppage_violations_count,
            route_deviation_count,
            speed_violation_count,
            night_driving_count,
            device_offline_count,
            device_tamper_count,
            continuous_driving_count,
            no_halt_zone_count, 
            main_supply_removal_count
     FROM vts_alert_history 
     WHERE tl_number = 'KA13B3272'
     ORDER BY (stoppage_violations_count + route_deviation_count + speed_violation_count + device_offline_count + device_tamper_count + continuous_driving_count + no_halt_zone_count + main_supply_removal_count) DESC
     LIMIT 5
     """),
    
    ("speed violation and night driving counts for TN28BL5897",
     "SELECT 'vts_alert_history' AS \"Data Table\", tl_number, SUM(speed_violation_count) as speed_violations, SUM(night_driving_count) as night_driving FROM vts_alert_history WHERE tl_number = 'TN28BL5897' GROUP BY tl_number"),

    # ===== ALERT QUERIES =====
    ("recent VTS alerts with high severity", 
     "SELECT 'alerts' AS \"Data Table\", vehicle_number, alert_category, severity, created_at, location_name FROM alerts WHERE alert_section = 'VTS' AND severity = 'HIGH' ORDER BY created_at DESC LIMIT 20"), 
    
    ("VTS alert count by category",
     "SELECT 'alerts' AS \"Data Table\", alert_category, COUNT(*) as alert_count FROM alerts WHERE alert_section = 'VTS' GROUP BY alert_category ORDER BY alert_count DESC"),
    
    # ===== ONGOING TRIPS =====
    ("current ongoing trips in Bathinda region", 
     "SELECT 'vts_ongoing_trips' AS \"Data Table\", tt_number, vehicle_location, created_at FROM vts_ongoing_trips WHERE region = 'BATHINDA' ORDER BY created_at DESC"),
    
    ("vehicles currently on trip from Mumbai",
     "SELECT 'vts_ongoing_trips' AS \"Data Table\", tt_number, vehicle_location, trip_id, created_at FROM vts_ongoing_trips WHERE region ILIKE '%Mumbai%' ORDER BY created_at DESC"),
    
    #  "SELECT 'vts_alert_history' AS \"Data Table\", SUM(stoppage_violations_count + route_deviation_count + speed_violation_count) as total_violations FROM vts_alert_history WHERE created_at::date = CURRENT_DATE - INTERVAL '1 day'"),
     ("what vehicles are in vts_ongoing_trips where sap_id is 1937",
 """
 SELECT 'vts_ongoing_trips' AS "Data Table",
        TT_NUMBER,
        vehicle_location,
        transporter_name, 
        sap_id,
        created_at
 FROM vts_ongoing_trips 
 WHERE sap_id = '1937'
"""),
 
    # ===== SAP_ID QUERIES WITH PROPER STRING FORMATTING =====
    ("what vehicles are in vts_ongoing_trips of sap_id is 1937",
     "SELECT 'vts_ongoing_trips' AS \"Data Table\", tt_number, vehicle_location, transporter_name FROM vts_ongoing_trips WHERE sap_id = '1937'"),
    
    ("vehicles with sap_id 1937 in ongoing trips", 
     "SELECT 'vts_ongoing_trips' AS \"Data Table\", tt_number, trip_id, vehicle_location FROM vts_ongoing_trips WHERE sap_id = '1937'"),
    
    ("show me vehicles where sap_id equals 1937",
     "SELECT 'vts_ongoing_trips' AS \"Data Table\", tt_number, vehicle_location FROM vts_ongoing_trips WHERE sap_id = '1937'"),
    
    ("list all vehicles for sap id 1937",
     "SELECT 'vts_ongoing_trips' AS \"Data Table\", tt_number as vehicle_number FROM vts_ongoing_trips WHERE sap_id = '1937'"),
    
    ("count vehicles with sap_id 1937",
     "SELECT 'vts_ongoing_trips' AS \"Data Table\", COUNT(*) as vehicle_count FROM vts_ongoing_trips WHERE sap_id = '1937'"),
    
    # ===== OTHER SAP_ID QUERIES ACROSS TABLES =====
    ("trucks in truck master with sap_id 1937", 
     "SELECT 'vts_truck_master' AS \"Data Table\", truck_no, location_name, transporter_name FROM vts_truck_master WHERE sap_id = '1937'"),
    
    ("alerts for sap_id 1937",
     "SELECT 'alerts' AS \"Data Table\", vehicle_number, alert_category, severity FROM alerts WHERE sap_id = '1937' AND alert_section = 'VTS'"),
    
    # ===== VTS_ONGOING_TRIPS BASIC QUERIES =====
    ("all ongoing trips",
     "SELECT 'vts_ongoing_trips' AS \"Data Table\", tt_number, transporter_name, vehicle_location FROM vts_ongoing_trips ORDER BY created_at DESC"),
    
    ("current trips with transporter details",
     "SELECT 'vts_ongoing_trips' AS \"Data Table\", tt_number, transporter_name, trip_id, vehicle_location FROM vts_ongoing_trips WHERE created_at >= CURRENT_DATE - INTERVAL '1 day'"),
    
    ("vehicles in mumbai location",
     "SELECT 'vts_ongoing_trips' AS \"Data Table\", tt_number, vehicle_location FROM vts_ongoing_trips WHERE vehicle_location ILIKE '%mumbai%'"),
    
    # ===== VTS_ALERT_HISTORY QUERIES =====
    ("violations for vehicle KA13B3272",
     "SELECT 'vts_alert_history' AS \"Data Table\", tl_number, stoppage_violations_count, route_deviation_count, speed_violation_count, main_supply_removal_count, night_driving_count, no_halt_zone_count, device_offline_count, device_tamper_count, continuous_driving_count, (stoppage_violations_count + route_deviation_count + speed_violation_count + main_supply_removal_count + night_driving_count + no_halt_zone_count + device_offline_count + device_tamper_count + continuous_driving_count) as total_violations FROM vts_alert_history WHERE tl_number = 'KA13B3272' ORDER BY vts_end_datetime DESC"),
  
    ("route deviation alerts",
     "SELECT 'vts_alert_history' AS \"Data Table\", tl_number, route_deviation_count, location_name FROM vts_alert_history WHERE route_deviation_count > 0 ORDER BY vts_end_datetime DESC LIMIT 20"), 
     
  # ===== ALERTS TABLE QUERIES =====
    ("open VTS alerts",
     "SELECT 'alerts' AS \"Data Table\", vehicle_number, alert_category, severity FROM alerts WHERE alert_section = 'VTS' AND alert_status = 'OPEN' ORDER BY created_at DESC"),
    
    ("high severity alerts",
     "SELECT 'alerts' AS \"Data Table\", vehicle_number, alert_category, location_name FROM alerts WHERE alert_section = 'VTS' AND severity = 'HIGH' ORDER BY created_at DESC"),
    
    # ===== VTS_TRUCK_MASTER QUERIES =====
    ("all trucks in master",
     "SELECT 'vts_truck_master' AS \"Data Table\", truck_no, transporter_name, zone FROM vts_truck_master ORDER BY transporter_name"),
    
    ("trucks by transporter",
     "SELECT 'vts_truck_master' AS \"Data Table\", transporter_name, COUNT(*) as truck_count FROM vts_truck_master GROUP BY transporter_name ORDER BY truck_count DESC"),
    
    # ===== CROSS-TABLE QUERIES =====

    ("vehicles with both trips and alerts",
     "SELECT DISTINCT vot.tt_number FROM vts_ongoing_trips vot JOIN alerts a ON vot.tt_number = a.vehicle_number WHERE a.alert_section = 'VTS'"), 
    
    # ===== ZONE AND REGION QUERIES =====
    ("vehicles in specific zone", "SELECT 'vts_ongoing_trips' AS \"Data Table\", tt_number, zone, vehicle_location FROM vts_ongoing_trips WHERE zone = 'North'"),
    
    # ===== TIME-BASED FILTERING =====
    ("today's ongoing trips",
     "SELECT 'vts_ongoing_trips' AS \"Data Table\", TT_NUMBER, transporter_name FROM vts_ongoing_trips WHERE created_at::date = CURRENT_DATE"),
    
    ("violations last week",
     "SELECT 'vts_alert_history' AS \"Data Table\", tl_number, (stoppage_violations_count + route_deviation_count + speed_violation_count + night_driving_count + device_offline_count + device_tamper_count + continuous_driving_count + no_halt_zone_count + main_supply_removal_count) as total_violations, created_at FROM vts_alert_history WHERE created_at >= CURRENT_DATE - INTERVAL '7 days' ORDER BY created_at DESC"), 
     
    # ===== COUNT AND SUMMARY QUERIES =====
    ("total ongoing trips count",
     "SELECT 'vts_ongoing_trips' AS \"Data Table\", COUNT(*) as total_trips FROM vts_ongoing_trips"),
    
    ("vehicle count by status",
     "SELECT 'vts_ongoing_trips' AS \"Data Table\", COUNT(DISTINCT tt_number) as active_vehicles FROM vts_ongoing_trips"),
    
    # ===== SPECIFIC VEHICLE LOOKUP =====
    ("all information for vehicle AP05TB9465 in vts_ongoing_trips",
     "SELECT 'vts_ongoing_trips' AS \"Data Table\", tt_number, transporter_name, vehicle_location FROM vts_ongoing_trips WHERE tt_number = 'AP05TB9465'"),
  
    ("count vehicles by sap_id in ongoing trips", 
     "SELECT 'vts_ongoing_trips' AS \"Data Table\", sap_id, COUNT(*) as vehicle_count FROM vts_ongoing_trips GROUP BY sap_id ORDER BY vehicle_count DESC"), 
    
    # ===== VTS_ONGOING_TRIPS QUERIES =====
    ("current ongoing trips with transporter details", "SELECT 'vts_ongoing_trips' AS \"Data Table\", TT_NUMBER, transporter_name, vehicle_location, trip_id FROM vts_ongoing_trips ORDER BY created_at DESC"),
    
    ("trips by transporter name",
     "SELECT 'vts_ongoing_trips' AS \"Data Table\", transporter_name, COUNT(*) as trip_count FROM vts_ongoing_trips GROUP BY transporter_name ORDER BY trip_count DESC"),
    
    ("vehicles in specific zone",
     "SELECT 'vts_ongoing_trips' AS \"Data Table\", tt_number, zone, vehicle_location FROM vts_ongoing_trips WHERE zone IS NOT NULL"),
    
    ("ongoing trips by region",
     "SELECT 'vts_ongoing_trips' AS \"Data Table\", region, COUNT(*) as trip_count FROM vts_ongoing_trips WHERE region IS NOT NULL GROUP BY region ORDER BY trip_count DESC"),
    
    # ===== VTS_ALERT_HISTORY QUERIES =====
    ("violation counts for vehicle KA13B3272",
     "SELECT 'vts_alert_history' AS \"Data Table\", tl_number, SUM(stoppage_violations_count) as stoppage, SUM(route_deviation_count) as route_deviations FROM vts_alert_history WHERE tl_number = 'KA13B3272' GROUP BY tl_number"),
    
    ("recent violations with location details",
     "SELECT 'vts_alert_history' AS \"Data Table\", tl_number, location_name, (stoppage_violations_count + route_deviation_count + speed_violation_count + night_driving_count + device_offline_count + device_tamper_count + continuous_driving_count + no_halt_zone_count + main_supply_removal_count) as total_violations, vts_end_datetime FROM vts_alert_history WHERE vts_end_datetime >= CURRENT_DATE - INTERVAL '7 days' ORDER BY vts_end_datetime DESC"),
    
    ("violations by region",
     "SELECT 'vts_alert_history' AS \"Data Table\", region, SUM(stoppage_violations_count + route_deviation_count + speed_violation_count + night_driving_count + device_offline_count + device_tamper_count + continuous_driving_count + no_halt_zone_count + main_supply_removal_count) as total_violations FROM vts_alert_history WHERE region IS NOT NULL GROUP BY region ORDER BY total_violations DESC"),
    
    ("violations by zone",
     "SELECT 'vts_alert_history' AS \"Data Table\", zone, COUNT(*) as violation_count FROM vts_alert_history WHERE zone IS NOT NULL GROUP BY zone ORDER BY violation_count DESC"), 
      
    # ===== ALERTS TABLE QUERIES =====
    ("open VTS alerts",
     "SELECT 'alerts' AS \"Data Table\", vehicle_number, alert_category, severity, location_name, created_at FROM alerts WHERE alert_section = 'VTS' AND alert_status = 'OPEN' ORDER BY created_at DESC"),
    
    ("alert count by severity",
     "SELECT 'alerts' AS \"Data Table\", severity, COUNT(*) as alert_count FROM alerts WHERE alert_section = 'VTS' GROUP BY severity ORDER BY alert_count DESC"),
    
    ("alerts by zone",
     "SELECT 'alerts' AS \"Data Table\", zone, COUNT(*) as alert_count FROM alerts WHERE alert_section = 'VTS' AND zone IS NOT NULL GROUP BY zone ORDER BY alert_count DESC"),
    
    ("recent high severity alerts",
     "SELECT 'alerts' AS \"Data Table\", vehicle_number, alert_category, location_name, created_at FROM alerts WHERE alert_section = 'VTS' AND severity = 'HIGH' AND created_at >= CURRENT_DATE - INTERVAL '1 day' ORDER BY created_at DESC"),
    
    # ===== VTS_TRUCK_MASTER QUERIES =====
    ("truck master list with transporters",
     "SELECT 'vts_truck_master' AS \"Data Table\", truck_no, transporter_name, zone, region FROM vts_truck_master ORDER BY transporter_name"),
    
    ("trucks in specific zone",
     "SELECT 'vts_truck_master' AS \"Data Table\", truck_no, zone, location_name FROM vts_truck_master WHERE zone IS NOT NULL"),
    
    
    # ===== LOCATION-BASED QUERIES =====
    ("violations by location type",
     "SELECT 'vts_alert_history' AS \"Data Table\", location_type, COUNT(*) as violation_count FROM vts_alert_history WHERE location_type IS NOT NULL GROUP BY location_type ORDER BY violation_count DESC"), 
    
    ("vehicles in specific location",
     "SELECT 'vts_ongoing_trips' AS \"Data Table\", TT_NUMBER, vehicle_location, transporter_name FROM vts_ongoing_trips WHERE vehicle_location ILIKE '%mumbai%'"), 
    
    # ===== TIME-BASED QUERIES =====
    ("today's violations",
     "SELECT 'vts_alert_history' AS \"Data Table\", tl_number, stoppage_violations_count, route_deviation_count, vts_end_datetime FROM vts_alert_history WHERE vts_end_datetime::date = CURRENT_DATE ORDER BY vts_end_datetime DESC"),
    
    ("violations last 7 days",
     "SELECT 'vts_alert_history' AS \"Data Table\", vts_end_datetime::date as date, COUNT(*) as violation_count FROM vts_alert_history WHERE vts_end_datetime >= CURRENT_DATE - INTERVAL '7 days' GROUP BY vts_end_datetime::date ORDER BY date DESC"),
    
    # ===== AGGREGATION QUERIES =====
    ("total violation counts by type",
     """SELECT 'vts_alert_history' AS "Data Table", SUM(stoppage_violations_count) as total_stoppage, SUM(route_deviation_count) as total_route_deviations, SUM(speed_violation_count) as total_speed, SUM(device_offline_count) as total_offline, SUM(device_tamper_count) as total_tamper, SUM(continuous_driving_count) as total_continuous, SUM(night_driving_count) as total_night, SUM(main_supply_removal_count) as total_main, SUM(no_halt_zone_count) as total_no_halt FROM vts_alert_history WHERE vts_end_datetime >= CURRENT_DATE - INTERVAL '30 days'
     """), 
    
    ("vehicle violation summary",
     """SELECT 'vts_alert_history' AS "Data Table", tl_number, SUM(stoppage_violations_count + route_deviation_count + speed_violation_count + night_driving_count + device_offline_count + device_tamper_count + continuous_driving_count + no_halt_zone_count + main_supply_removal_count) as total_violations FROM vts_alert_history WHERE vts_end_datetime >= CURRENT_DATE - INTERVAL '30 days' GROUP BY tl_number ORDER BY total_violations DESC LIMIT 20"""),
    
    # ===== SPECIFIC VEHICLE QUERIES =====
    ("all data for vehicle AP05TB9465", 
     """SELECT 'vts_alert_history' AS "Data Table", 
    tl_number, stoppage_violations_count, route_deviation_count,speed_violation_count,main_supply_removal_count,no_halt_zone_count,device_offline_count,device_tamper_count,continuous_driving_count,night_driving_count,vts_end_datetime
FROM vts_alert_history WHERE tl_number = 'AP05TB9465'
ORDER BY created_at DESC"""),
    
    # ===== TRANSPORTER PERFORMANCE =====
    ("transporter violation analysis",
     """
     SELECT tm.transporter_name,
            COUNT(DISTINCT vah.tl_number) as vehicle_count,
            SUM(vah.stoppage_violations_count) as total_stoppage, 
            SUM(vah.route_deviation_count) as total_route_deviations
     FROM vts_truck_master tm
     JOIN vts_alert_history vah ON tm.truck_no = vah.tl_number 
     WHERE vah.vts_end_datetime >= CURRENT_DATE - INTERVAL '30 days'
     GROUP BY tm.transporter_name
     ORDER BY total_stoppage DESC
"""),
    
    # ===== ZONE/REGION ANALYSIS =====
    ("zone-wise violation summary",
     """
     SELECT COALESCE(vah.zone, 'Unknown') as zone,
            COUNT(DISTINCT vah.tl_number) as vehicles,
            SUM(vah.stoppage_violations_count) as stoppage_violations,
            SUM(vah.route_deviation_count) as route_deviations 
     FROM vts_alert_history vah
     WHERE vah.vts_end_datetime >= CURRENT_DATE - INTERVAL '30 days'
     GROUP BY COALESCE(vah.zone, 'Unknown')
     ORDER BY stoppage_violations DESC"""),
    
    ("region performance analysis",
     """
     SELECT region,
            COUNT(DISTINCT tl_number) as total_vehicles,
            AVG(stoppage_violations_count) as avg_stoppage_per_vehicle,
            AVG(route_deviation_count) as avg_route_deviations 
     FROM vts_alert_history
     WHERE region IS NOT NULL
       AND vts_end_datetime >= CURRENT_DATE - INTERVAL '30 days'
     GROUP BY region
     ORDER BY avg_stoppage_per_vehicle DESC
"""),
    
    # ===== SAP_ID CROSS TABLE QUERIES =====
    ("vehicles with same sap_id across tables",
     """
     SELECT 'vts_ongoing_trips' as source, sap_id, TT_NUMBER as vehicle
     FROM vts_ongoing_trips WHERE sap_id = '1937' 
     UNION ALL
     SELECT 'vts_truck_master' as source, sap_id, truck_no as vehicle
     FROM vts_truck_master WHERE sap_id = '1937'
     UNION ALL
     SELECT 'alerts' as source, sap_id, vehicle_number as vehicle 
     FROM alerts WHERE sap_id = '1937' AND alert_section = 'VTS'
"""),
    
    # ===== DEVICE & EQUIPMENT QUERIES =====
    ("device tamper alerts",
     "SELECT 'vts_alert_history' AS \"Data Table\", tl_number, device_tamper_count, vts_end_datetime FROM vts_alert_history WHERE device_tamper_count > 0 ORDER BY vts_end_datetime DESC"),
     
    ("device offline violations",
     "SELECT 'vts_alert_history' AS \"Data Table\", tl_number, device_offline_count, location_name, vts_end_datetime FROM vts_alert_history WHERE device_offline_count > 0 ORDER BY vts_end_datetime DESC"),
     
    # ===== WORKFLOW QUERIES =====
    ("alerts pending assignment",
     "SELECT 'alerts' AS \"Data Table\", vehicle_number, alert_category, severity, created_at FROM alerts WHERE alert_section = 'VTS' AND assigned_to IS NULL AND alert_status = 'OPEN' ORDER BY created_at DESC"),
    
    ("recently closed alerts",
     "SELECT 'alerts' AS \"Data Table\", vehicle_number, alert_category, closed_at FROM alerts WHERE alert_section = 'VTS' AND alert_status = 'Close' AND closed_at >= CURRENT_DATE - INTERVAL '7 days' ORDER BY closed_at DESC"), 

    # ===== ZONE/REGION ANALYSIS QUERIES =====
    ("what are the unique zones in all tables", # This is a duplicate of "get all zones from every table"
     """SELECT 'vts_alert_history' AS "Data Table", zone 
     FROM vts_alert_history WHERE zone IS NOT NULL 
     UNION 
     SELECT 'vts_ongoing_trips' AS "Data Table", zone 
     FROM vts_ongoing_trips WHERE zone IS NOT NULL
     UNION 
     SELECT 'vts_truck_master' AS "Data Table", zone 
     FROM vts_truck_master WHERE zone IS NOT NULL
     """),
    
    ("list all distinct zones across tables",
     """
     SELECT DISTINCT zone, 'vts_alert_history' AS source_table 
     FROM vts_alert_history WHERE zone IS NOT NULL 
     UNION ALL
     SELECT DISTINCT zone, 'vts_ongoing_trips' AS source_table 
     FROM vts_ongoing_trips WHERE zone IS NOT NULL 
     UNION ALL
     SELECT DISTINCT zone, 'alerts' AS source_table
     FROM alerts WHERE zone IS NOT NULL AND alert_section = 'VTS' 
     UNION ALL
     SELECT DISTINCT zone, 'vts_truck_master' AS source_table 
     FROM vts_truck_master WHERE zone IS NOT NULL
     ORDER BY zone"""),
     
    ("show me all zones from every table",
     """
     SELECT 'vts_alert_history' AS table_name, zone, COUNT(*) as record_count
     FROM vts_alert_history WHERE zone IS NOT NULL GROUP BY zone 
     UNION ALL
     SELECT 'vts_ongoing_trips' AS table_name, zone, COUNT(*) as record_count
     FROM vts_ongoing_trips WHERE zone IS NOT NULL GROUP BY zone 
     UNION ALL
     SELECT 'alerts' AS table_name, zone, COUNT(*) as record_count
     FROM alerts WHERE zone IS NOT NULL AND alert_section = 'VTS' GROUP BY zone 
     UNION ALL
     SELECT 'vts_truck_master' AS table_name, zone, COUNT(*) as record_count
     FROM vts_truck_master WHERE zone IS NOT NULL GROUP BY zone
     ORDER BY table_name, zone"""),
    
    # ===== SPECIFIC TABLE ZONE QUERIES =====
    ("what zones are in vts_alert_history",
     "SELECT 'vts_alert_history' AS \"Data Table\", DISTINCT zone FROM vts_alert_history WHERE zone IS NOT NULL ORDER BY zone"),
    
    ("unique zones in ongoing trips",
     "SELECT 'vts_ongoing_trips' AS \"Data Table\", DISTINCT zone FROM vts_ongoing_trips WHERE zone IS NOT NULL ORDER BY zone"),
    
    ("zones in alerts table",
     "SELECT 'alerts' AS \"Data Table\", DISTINCT zone FROM alerts WHERE zone IS NOT NULL AND alert_section = 'VTS' ORDER BY zone"),
    
    ("zones from truck master",
     "SELECT 'vts_truck_master' AS \"Data Table\", DISTINCT zone FROM vts_truck_master WHERE zone IS NOT NULL ORDER BY zone"),
    
    # ===== REGION QUERIES =====
    ("list all regions across tables",
     """
     SELECT DISTINCT region, 'vts_alert_history' AS source_table
     FROM vts_alert_history WHERE region IS NOT NULL
     UNION ALL
     SELECT DISTINCT region, 'vts_ongoing_trips' AS source_table
     FROM vts_ongoing_trips WHERE region IS NOT NULL
     UNION ALL
     SELECT DISTINCT region, 'alerts' AS source_table 
     FROM alerts WHERE region IS NOT NULL AND alert_section = 'VTS'
     UNION ALL
     SELECT DISTINCT region, 'vts_truck_master' AS source_table 
     FROM vts_truck_master WHERE region IS NOT NULL
     ORDER BY region"""),
     
    # ===== ZONE COUNT QUERIES =====
    ("count of records by zone in each table",
     """
     SELECT 'vts_alert_history' AS table_name, zone, COUNT(*) as record_count
     FROM vts_alert_history WHERE zone IS NOT NULL GROUP BY zone 
     UNION ALL
     SELECT 'vts_ongoing_trips' AS table_name, zone, COUNT(*) as record_count
     FROM vts_ongoing_trips WHERE zone IS NOT NULL GROUP BY zone 
     UNION ALL
     SELECT 'alerts' AS table_name, zone, COUNT(*) as record_count
     FROM alerts WHERE zone IS NOT NULL AND alert_section = 'VTS' GROUP BY zone 
     UNION ALL
     SELECT 'vts_truck_master' AS table_name, zone, COUNT(*) as record_count
     FROM vts_truck_master WHERE zone IS NOT NULL GROUP BY zone
     ORDER BY table_name, record_count DESC"""),
    
    ("which zones have the most vehicles",
     """
     SELECT zone, COUNT(DISTINCT truck_no) as vehicle_count
     FROM vts_truck_master
     WHERE zone IS NOT NULL
     GROUP BY zone 
     ORDER BY vehicle_count DESC"""),
    
    # ===== ZONE AND REGION COMBINED =====
    ("zones and regions together",
     """
     SELECT 'zone' as type, zone as value, 'vts_alert_history' as table_name
     FROM vts_alert_history WHERE zone IS NOT NULL AND zone <> '' 
     UNION ALL
     SELECT 'region' as type, region as value, 'vts_alert_history' as table_name
     FROM vts_alert_history WHERE region IS NOT NULL AND region <> '' 
     UNION ALL
     SELECT 'zone' as type, zone as value, 'vts_ongoing_trips' as table_name
     FROM vts_ongoing_trips WHERE zone IS NOT NULL AND zone <> '' 
     UNION ALL
     SELECT 'region' as type, region as value, 'vts_ongoing_trips' as table_name
     FROM vts_ongoing_trips WHERE region IS NOT NULL
     ORDER BY type, value"""),
    
    # ===== LOCATION-BASED ZONE ANALYSIS =====
    ("zones with their locations",
     """
     SELECT zone, location_name, COUNT(*) as location_count
     FROM vts_truck_master
     WHERE zone IS NOT NULL AND location_name IS NOT NULL 
     GROUP BY zone, location_name
     ORDER BY zone, location_count DESC"""),
    
    ("active zones in ongoing trips",
     """
     SELECT zone, COUNT(DISTINCT TT_NUMBER) as active_vehicles
     FROM vts_ongoing_trips
     WHERE zone IS NOT NULL 
     GROUP BY zone
     ORDER BY active_vehicles DESC"""),
    # Comprehensive vehicle queries
    ("Show all VTS alerts and violations for vehicle JH05DH9866",
     """SELECT
    'vts_alert_history'::TEXT AS "Data Table",
    CASE 
        WHEN violation_type IS NOT NULL AND array_length(violation_type, 1) > 0
        THEN array_to_string(violation_type, ', ')::TEXT 
        ELSE 'No Specific Violations'
    END AS "Event Type",
    vts_end_datetime::TIMESTAMP WITHOUT TIME ZONE AS "Event Time",
    COALESCE(location_name, 'N/A')::TEXT AS "Location Info", 
    invoice_number::TEXT AS "Invoice Number", 
    tl_number::TEXT AS "Vehicle_Identifier"
FROM vts_alert_history
WHERE tl_number = 'JH05DH9866'
UNION ALL
SELECT 'vts_ongoing_trips'::TEXT AS "Data Table",
    COALESCE(violation_type, 'Ongoing Trip')::TEXT AS "Event Type",
    created_at::TIMESTAMP WITHOUT TIME ZONE AS "Event Time",
    COALESCE(vehicle_location, 'N/A')::TEXT AS "Location Info", 
    invoice_no::TEXT AS "Invoice Number", 
    tt_number::TEXT AS "Vehicle_Identifier"
FROM vts_ongoing_trips
WHERE tt_number ILIKE 'JH05DH9866'
UNION ALL
SELECT 'alerts'::TEXT AS "Data Table",
    COALESCE(violation_type, 'Alert')::TEXT AS "Event Type",
    created_at::TIMESTAMP WITHOUT TIME ZONE AS "Event Time",
    COALESCE(location_name, 'N/A')::TEXT AS "Location Info",
    NULL::TEXT AS "Invoice Number", 
    COALESCE(vehicle_number, tt_load_number)::TEXT AS "Vehicle_Identifier"
FROM alerts 
WHERE (vehicle_number = 'JH05DH9866' OR tt_load_number = 'JH05DH9866') 
  AND alert_section = 'VTS'
ORDER BY "Event Time" DESC
LIMIT 100"""),
    
    # =============================================================================================
    # ===== NEW COMPREHENSIVE TRAINING DATA (MAJOR EXPANSION) =====
    # =============================================================================================

    # ===== ADVANCED SEQUENTIAL & TIME-GAP ANALYSIS =====
    ("For vehicles with more than 5 `device_tamper_count` events, what is the average time until the next `device_offline_count` event?",
    """WITH tamper_events AS (
    SELECT
        vah.tl_number,
        vah.vts_end_datetime
    FROM vts_alert_history vah
    WHERE vah.device_tamper_count > 0
      AND vah.tl_number IN (
        -- Subquery to find vehicles with more than 5 total tamper events
        SELECT tl_number
        FROM vts_alert_history
        WHERE device_tamper_count > 0
        GROUP BY tl_number
        HAVING SUM(device_tamper_count) > 5
      )
),
event_stream AS (
    SELECT
        te.tl_number,
        te.vts_end_datetime AS tamper_time,
        -- Use a correlated subquery to find the first offline event after the tamper event. This is more compatible than IGNORE NULLS.
        (SELECT MIN(vah_inner.vts_end_datetime)
         FROM vts_alert_history vah_inner
         WHERE vah_inner.tl_number = te.tl_number
           AND vah_inner.device_offline_count > 0
           AND vah_inner.vts_end_datetime > te.vts_end_datetime
        ) AS next_offline_time
    FROM tamper_events te
)
SELECT
    'sequential_analysis' AS "Data Table",
    AVG(next_offline_time - tamper_time) AS average_time_to_next_offline
FROM event_stream
WHERE next_offline_time IS NOT NULL;
"""),

    ("Which violation type has the highest probability of being followed by a 'device offline' event within 3 hours?",
    """WITH event_pairs AS (
    SELECT
        vt.violation_type_element as current_violation,
        LEAD(vah.device_offline_count > 0) OVER (PARTITION BY vah.tl_number ORDER BY vah.vts_end_datetime) as next_is_offline,
        LEAD(vah.vts_end_datetime) OVER (PARTITION BY vah.tl_number ORDER BY vah.vts_end_datetime) as next_event_time,
        vah.vts_end_datetime as current_event_time
    FROM vts_alert_history vah,
    LATERAL UNNEST(vah.violation_type) AS vt(violation_type_element)
    WHERE vah.violation_type IS NOT NULL AND array_length(vah.violation_type, 1) > 0
),
violation_stats AS (
    SELECT
        current_violation,
        COUNT(*) as total_occurrences,
        COUNT(*) FILTER (WHERE next_is_offline = true AND (next_event_time - current_event_time) <= INTERVAL '3 hours') as followed_by_offline_count
    FROM event_pairs
    WHERE current_violation != 'device_offline_count > 0' -- Exclude offline as the starting event
    GROUP BY current_violation
)
SELECT
    'violation_probability' AS "Data Table",
    current_violation,
    total_occurrences,
    followed_by_offline_count,
    ROUND(100.0 * followed_by_offline_count / NULLIF(total_occurrences, 0), 2) as probability_percent
FROM violation_stats
WHERE total_occurrences > 50 -- Only consider violations that occur frequently enough to be statistically relevant
ORDER BY probability_percent DESC;
"""),

    # ===== ANOMALY & CONTRADICTION DETECTION =====
    ("Are there any blacklisted vehicles that are currently on an ongoing trip?",
    """SELECT
    'anomaly_blacklisted_active' AS "Data Table",
    vot.tt_number,
    vot.transporter_name,
    vot.vehicle_location,
    vot.created_at AS trip_start_time 
FROM vts_ongoing_trips vot
JOIN vts_truck_master vtm ON vot.tt_number = vtm.truck_no
WHERE vtm.whether_truck_blacklisted = 'Y';
"""),

    # ===== CORRELATION & STATISTICAL ANALYSIS =====
    ("What is the correlation between `stoppage_violation` score in `tt_risk_score` and the actual `stoppage_violations_count` in `vts_alert_history` for the top 10 riskiest vehicles?",
    """WITH top_vehicles AS (
    SELECT tt_number
    FROM tt_risk_score
    ORDER BY risk_score DESC
    LIMIT 10
),
risk_data AS (
    SELECT
        tt_number,
        stoppage_violation as risk_stoppage_score
    FROM tt_risk_score
    WHERE tt_number IN (SELECT tt_number FROM top_vehicles)
),
violation_data AS (
    SELECT
        tl_number,
        SUM(stoppage_violations_count) as actual_stoppage_count
    FROM vts_alert_history
    WHERE tl_number IN (SELECT tt_number FROM top_vehicles)
    GROUP BY tl_number
)
SELECT
    'correlation_analysis' AS "Data Table",
    rd.tt_number,
    rd.risk_stoppage_score,
    vd.actual_stoppage_count,
    CORR(rd.risk_stoppage_score, vd.actual_stoppage_count) OVER () as correlation_coefficient
FROM risk_data rd
JOIN violation_data vd ON rd.tt_number = vd.tl_number;
"""),

    # ===== DATA QUALITY & AUDITING =====
    ("Which vehicles appear in `vts_alert_history` but are missing from the `vts_truck_master` table?",
    """SELECT DISTINCT
    'data_quality_orphan_vehicles' AS "Data Table",
    vah.tl_number
FROM vts_alert_history vah
LEFT JOIN vts_truck_master vtm ON vah.tl_number = vtm.truck_no
WHERE vtm.truck_no IS NULL;
"""),

    # ===== CLIENT-SIDE / BUSINESS-FOCUSED QUESTIONS =====
    ("Which transporter has the most efficient routes, measured by lowest number of violations per trip?",
    """SELECT
    'transporter_efficiency' AS "Data Table",
    vtm.transporter_name,
    COUNT(DISTINCT vah.invoice_number) AS total_trips,
    SUM(vah.route_deviation_count + vah.stoppage_violations_count) AS total_violations,
    (SUM(vah.route_deviation_count + vah.stoppage_violations_count) * 1.0) / NULLIF(COUNT(DISTINCT vah.invoice_number), 0) AS avg_violations_per_trip
FROM vts_alert_history vah
JOIN vts_truck_master vtm ON vah.tl_number = vtm.truck_no
WHERE vah.invoice_number IS NOT NULL AND vtm.transporter_name IS NOT NULL
GROUP BY vtm.transporter_name
HAVING COUNT(DISTINCT vah.invoice_number) > 10 -- Only consider transporters with a meaningful number of trips
ORDER BY avg_violations_per_trip ASC
LIMIT 10;
"""),

    ("What is the financial impact of violations this month, assuming each violation costs $50?",
    """SELECT
    'financial_impact' AS "Data Table",
    SUM(stoppage_violations_count + route_deviation_count + speed_violation_count + night_driving_count + device_offline_count + device_tamper_count + continuous_driving_count + no_halt_zone_count + main_supply_removal_count) as total_violations,
    SUM(stoppage_violations_count + route_deviation_count + speed_violation_count + night_driving_count + device_offline_count + device_tamper_count + continuous_driving_count + no_halt_zone_count + main_supply_removal_count) * 50.00 as estimated_cost
FROM vts_alert_history
WHERE vts_end_datetime >= DATE_TRUNC('month', CURRENT_DATE);
"""),

    ("Give me a dashboard summary for today's operations",
    """SELECT 'dashboard' AS "Data Table", 'Active Trips' as metric, COUNT(*) as value FROM vts_ongoing_trips
UNION ALL
SELECT 'dashboard' AS "Data Table", 'Open High-Severity Alerts' as metric, COUNT(*) as value FROM alerts WHERE alert_status = 'OPEN' AND severity = 'HIGH' AND alert_section = 'VTS'
UNION ALL
SELECT 'dashboard' AS "Data Table", 'Total Violations Today' as metric, SUM(stoppage_violations_count + route_deviation_count + speed_violation_count + night_driving_count + device_offline_count + device_tamper_count + continuous_driving_count + no_halt_zone_count + main_supply_removal_count) as value FROM vts_alert_history WHERE vts_end_datetime::date = CURRENT_DATE;
"""),

    ("violations for date 2025-10-14",
     """SELECT 'vts_alert_history' AS "Data Table",
    SUM(stoppage_violations_count + route_deviation_count + speed_violation_count + 
        night_driving_count + device_offline_count + device_tamper_count + 
        continuous_driving_count + no_halt_zone_count + main_supply_removal_count) AS total_violations
FROM vts_alert_history 
WHERE vts_end_datetime::date = '2025-10-14'"""),
    
    # With breakdown
    ("violation count on 2025-10-14",
     """SELECT 'vts_alert_history' AS "Data Table",
    SUM(stoppage_violations_count + route_deviation_count + speed_violation_count + 
        night_driving_count + device_offline_count + device_tamper_count + 
        continuous_driving_count + no_halt_zone_count + main_supply_removal_count) AS total_violations
FROM vts_alert_history 
WHERE vts_end_datetime::date = '2025-10-14'"""),
    
    # Specific vehicle on date
    ("violations for vehicle JH05DH9866 on 2025-10-14",
     """SELECT 'vts_alert_history' AS "Data Table",
    tl_number,
    SUM(stoppage_violations_count + route_deviation_count + speed_violation_count + 
        night_driving_count + device_offline_count + device_tamper_count + 
        continuous_driving_count + no_halt_zone_count + main_supply_removal_count) AS total_violations
FROM vts_alert_history 
WHERE tl_number = 'JH05DH9866'
    AND vts_end_datetime::date = '2025-10-14'
GROUP BY tl_number"""),
    
    ("count of all violations on 2025-05-23",
     """SELECT 'vts_alert_history' AS "Data Table",
    SUM(stoppage_violations_count + route_deviation_count + speed_violation_count + 
        night_driving_count + device_offline_count + device_tamper_count + 
        continuous_driving_count + no_halt_zone_count + main_supply_removal_count) AS total_violations
FROM vts_alert_history 
WHERE vts_end_datetime::date = '2025-05-23'"""),
    
    # ===== PRIORITY 2: COMPREHENSIVE VEHICLE QUERIES =====
    ("Show all VTS alerts and violations for vehicle JH05DH9866",
     get_comprehensive_query_for_training(query_type="vehicle_timeline", vehicle_id="JH05DH9866")),
    
    ("violations on 2025-10-14",
     """SELECT 'vts_alert_history' AS "Data Table",
    SUM(stoppage_violations_count + route_deviation_count + speed_violation_count + 
        night_driving_count + device_offline_count + device_tamper_count + 
        continuous_driving_count + no_halt_zone_count + main_supply_removal_count) AS total_violations
FROM vts_alert_history 
WHERE vts_end_datetime::date = '2025-10-14'"""),
    
    ("count of all violations on 2025-05-23",
     """SELECT 'vts_alert_history' AS "Data Table",
    SUM(stoppage_violations_count + route_deviation_count + speed_violation_count + 
        night_driving_count + device_offline_count + device_tamper_count + 
        continuous_driving_count + no_halt_zone_count + main_supply_removal_count) AS total_violations
FROM vts_alert_history 
WHERE vts_end_datetime::date = '2025-05-23'"""),
    
    # ===== PRIORITY 2: COMPREHENSIVE VEHICLE QUERIES =====
    ("Show all VTS alerts and violations for vehicle JH05DH9866",
     get_comprehensive_query_for_training("JH05DH9866", SQL_RULES)),

    # ===== VTS Trip Audit Master Queries =====
    ("how many emlock trips were there today",
     """SELECT 'vts_tripauditmaster' AS "Data Table",
    COUNT(*) as emlock_trip_count
FROM vts_tripauditmaster 
WHERE isemlocktrip = 'Y' AND createdat::date = CURRENT_DATE"""),

    ("show trip audit details for truck KA13AA2040",
     """SELECT 'vts_tripauditmaster' AS "Data Table",
    trucknumber,
    invoicenumber,
    isemlocktrip,
    swipeinl1,
    swipeoutl1,
    createdat 
FROM vts_tripauditmaster
WHERE trucknumber = 'KA13AA2040'
ORDER BY createdat DESC
LIMIT 10"""),

    ("find trip audit for invoice number 9015941411-ZF23-1292",
     """SELECT 'vts_tripauditmaster' AS "Data Table",
    trucknumber,
    invoicenumber,
    isemlocktrip,
    swipeinl1,
    swipeoutl1, 
    createdat
FROM vts_tripauditmaster
WHERE invoicenumber = '9015941411-ZF23-1292'"""),

    ("list trips with missing swipe out data",
     """SELECT 'vts_tripauditmaster' AS "Data Table",
    trucknumber,
    invoicenumber,
    createdat 
FROM vts_tripauditmaster
WHERE swipeoutl1 IS NULL OR swipeoutl2 IS NULL
ORDER BY createdat DESC
LIMIT 20"""),

    ("show me trips from 'Mumbai' location that are missing swipe out data",
     """SELECT 'vts_tripauditmaster' AS "Data Table",
    trucknumber,
    invoicenumber,
    createdat, 
    swipeinl1
FROM vts_tripauditmaster
WHERE location_name ILIKE 'Mumbai'
  AND (swipeoutl1 IS NULL OR swipeoutl2 IS NULL)
ORDER BY createdat DESC
LIMIT 20"""),

    ("what is the count of emlock and non-emlock trips for each BU",
     """SELECT 'vts_tripauditmaster' AS "Data Table",
    bu,
    isemlocktrip,
    COUNT(*) as trip_count 
FROM vts_tripauditmaster
WHERE bu IS NOT NULL GROUP BY bu, isemlocktrip
ORDER BY bu, isemlocktrip"""),

    ("find the latest trip audit for lock ID 'L12345'",
     """SELECT 'vts_tripauditmaster' AS "Data Table",
    trucknumber,
    invoicenumber,
    createdat,
    lockid1, 
    lockid2
FROM vts_tripauditmaster
WHERE lockid1 = 'L12345' OR lockid2 = 'L12345'
ORDER BY createdat DESC
LIMIT 1"""),

    ("find trip audit for invoice number 9015941411-ZF23-1292",
     """SELECT 'vts_tripauditmaster' AS "Data Table",
    trucknumber,
    invoicenumber,
    isemlocktrip,
    swipeinl1,
    swipeoutl1, 
    createdat
FROM vts_tripauditmaster
WHERE invoicenumber = '9015941411-ZF23-1292'"""),

    ("list trips with missing swipe out data",
     """SELECT 'vts_tripauditmaster' AS "Data Table",
    trucknumber,
    invoicenumber,
    createdat 
FROM vts_tripauditmaster
WHERE swipeoutl1 IS NULL OR swipeoutl2 IS NULL
ORDER BY createdat DESC
LIMIT 20"""),

    # ===== TT Risk Score & Transporter Risk Score =====
    ("what is the tt risk score for transporter_code of 28121929",
     """SELECT 'tt_risk_score' AS "Data Table",
    transporter_code, risk_score
FROM tt_risk_score 
WHERE transporter_code = '28121929'"""), 

    ("what is the transporter risk score for transporter_code of 28121929",
     """SELECT 'transporter_risk_score' AS "Data Table", 
    transporter_code, risk_score
FROM transporter_risk_score 
WHERE transporter_code = '28121929'"""),

    ("show me the transporter risk for 28114169",
     """SELECT 'transporter_risk_score' AS "Data Table",risk_score
FROM transporter_risk_score
WHERE transporter_code = '28114169'"""),

    ("top 10 transporters by risk score",
     """SELECT 'transporter_risk_score' AS "Data Table",
    transporter_code,
    risk_score 
FROM transporter_risk_score
WHERE risk_score IS NOT NULL AND transporter_code IS NOT NULL ORDER BY risk_score DESC
LIMIT 10"""),

    ("Are there any transporters with a perfect risk score of 0?",
     """SELECT 'transporter_risk_score' AS "Data Table",
    transporter_code, 
    risk_score
FROM transporter_risk_score
WHERE risk_score = 0 AND transporter_code IS NOT NULL
ORDER BY transporter_code"""),

    # ===== NEW: CRITICAL 3-TABLE JOIN (Alerts -> Master -> Transporter Risk) =====
    ("List open, high-severity alerts for vehicles belonging to transporters with a risk score above 70",
    """SELECT
    'alerts' AS "Data Table",
    a.vehicle_number,
    vtm.transporter_name,
    a.alert_category,
    a.severity,
    trs.risk_score AS "Transporter Risk Score"
FROM alerts a
JOIN vts_truck_master vtm ON a.vehicle_number = vtm.truck_no
JOIN transporter_risk_score trs ON vtm.transporter_code = trs.transporter_code
WHERE
    a.alert_section = 'VTS'
    AND a.alert_status = 'OPEN'
    AND a.severity IN ('HIGH', 'CRITICAL')
    AND trs.risk_score > 70
ORDER BY trs.risk_score DESC, a.created_at DESC
LIMIT 50"""),

    # ===== NEW: LOCATION-BASED ANALYSIS (MAJOR EXPANSION) =====
    ("Which vehicles often violate in the same locations?",
    """SELECT
    'vts_alert_history' AS "Data Table",
    tl_number,
    location_name,
    COUNT(*) AS violation_count 
FROM vts_alert_history
WHERE location_name IS NOT NULL
  AND (stoppage_violations_count > 0 OR route_deviation_count > 0 OR speed_violation_count > 0,
       night_driving_count > 0 OR device_offline_count > 0 OR device_tamper_count > 0 OR continuous_driving_count > 0 OR no_halt_zone_count > 0 OR main_supply_removal_count > 0)
GROUP BY tl_number, location_name
HAVING COUNT(*) > 5
ORDER BY violation_count DESC, tl_number;
"""),

    ("Which locations are violation hotspots? Show me the top 20.",
    """SELECT
    'violation_hotspots' AS "Data Table",
    location_name,
    COUNT(*) as total_violation_events,
    SUM(stoppage_violations_count + route_deviation_count + speed_violation_count + night_driving_count + device_offline_count + device_tamper_count + continuous_driving_count + no_halt_zone_count + main_supply_removal_count) as total_violations 
FROM vts_alert_history
WHERE location_name IS NOT NULL
GROUP BY location_name
ORDER BY total_violation_events DESC
LIMIT 20;
"""),

    ("For the 'Mumbai' location, which transporters have the most violations?",
    """SELECT
    vtm.transporter_name,
    vah.location_name,
    SUM(vah.stoppage_violations_count + vah.route_deviation_count + vah.speed_violation_count + vah.main_supply_removal_count + vah.night_driving_count + vah.device_offline_count + vah.device_tamper_count + vah.continuous_driving_count + vah.no_halt_zone_count) as total_violations 
FROM vts_alert_history vah
JOIN vts_truck_master vtm ON vah.tl_number = vtm.truck_no
WHERE vah.location_name ILIKE '%Mumbai%'
GROUP BY vtm.transporter_name, vah.location_name
ORDER BY total_violations DESC;
"""),

    ("Show me vehicles that are creating violations across many different locations.",
    """SELECT
    'multi_location_violators' AS "Data Table",
    tl_number,
    COUNT(DISTINCT location_name) as distinct_location_count 
FROM vts_alert_history
WHERE location_name IS NOT NULL
  AND (stoppage_violations_count > 0 OR route_deviation_count > 0)
GROUP BY tl_number
HAVING COUNT(DISTINCT location_name) > 10 -- Violated in more than 10 different places
ORDER BY distinct_location_count DESC;
"""),

    ("For each transporter, categorize their vehicles into 'High Risk' (score > 75), 'Medium Risk' (40-75), and 'Low Risk' (score < 40) and show the count for each category",
    """SELECT
    vtm.transporter_name,
    SUM(CASE WHEN trs.risk_score > 75 THEN 1 ELSE 0 END) AS "High Risk Vehicles",
    SUM(CASE WHEN trs.risk_score BETWEEN 40 AND 75 THEN 1 ELSE 0 END) AS "Medium Risk Vehicles",
    SUM(CASE WHEN trs.risk_score < 40 THEN 1 ELSE 0 END) AS "Low Risk Vehicles"
FROM vts_truck_master vtm
JOIN tt_risk_score trs ON vtm.truck_no = trs.tt_number
WHERE vtm.transporter_name IS NOT NULL AND trs.risk_score IS NOT NULL
GROUP BY vtm.transporter_name
ORDER BY "High Risk Vehicles" DESC, "Medium Risk Vehicles" DESC"""),

    ("Which trips started more than 2 hours late?",
    """
SELECT
    'vts_ongoing_trips' AS "Data Table",
    tt_number,
    trip_id,
    (actual_trip_start_datetime - scheduled_start_datetime) AS delay 
FROM vts_ongoing_trips
WHERE (actual_trip_start_datetime - scheduled_start_datetime) > INTERVAL '2 hours'
ORDER BY delay DESC"""),
    

    ("List all vehicles and their transporter names",
    """
SELECT
    'vts_truck_master' AS "Data Table",
    truck_no,
    transporter_name
FROM vts_truck_master;
"""),

    ("What is the average time difference between a trip's scheduled start and actual start for trips in the last month",
    """SELECT
    'vts_ongoing_trips' AS "Data Table",
    AVG(actual_trip_start_datetime - scheduled_start_datetime) AS "Average Start Delay"
FROM vts_ongoing_trips
WHERE
    actual_trip_start_datetime IS NOT NULL
    AND scheduled_start_datetime IS NOT NULL
    AND actual_trip_start_datetime >= CURRENT_DATE - INTERVAL '1 month'"""),

    ("Rank transporters by the total number of unique vehicles that had a violation in the past 90 days",
    """SELECT
    vtm.transporter_name,
    COUNT(DISTINCT vah.tl_number) AS unique_violating_vehicles 
FROM vts_alert_history vah
JOIN vts_truck_master vtm ON vah.tl_number = vtm.truck_no
WHERE vah.vts_end_datetime >= CURRENT_DATE - INTERVAL '90 days'
GROUP BY vtm.transporter_name 
HAVING vtm.transporter_name IS NOT NULL ORDER BY unique_violating_vehicles DESC"""),



    ("List vehicles from the truck master table that have no recorded violations in the vts_alert_history table",
    """SELECT
    vtm.truck_no
FROM vts_truck_master vtm 
LEFT JOIN vts_alert_history vah ON vtm.truck_no = vah.tl_number
WHERE vah.id IS NULL AND vtm.truck_no IS NOT NULL"""),

    ("Which carrier has the most power disconnect events",
    """SELECT
    vtm.transporter_name,
    SUM(vah.main_supply_removal_count) as total_power_disconnects 
FROM vts_alert_history vah
JOIN vts_truck_master vtm ON vah.tl_number = vtm.truck_no
WHERE vah.main_supply_removal_count > 0
GROUP BY vtm.transporter_name 
HAVING vtm.transporter_name IS NOT NULL ORDER BY total_power_disconnects DESC
LIMIT 1"""),

    ("For each transporter, find their top 3 vehicles with the highest number of total violations in the last 60 days",
    """WITH ranked_vehicles AS (
    SELECT
        vtm.transporter_name,
        vah.tl_number,
        SUM(vah.stoppage_violations_count + vah.route_deviation_count + vah.speed_violation_count + vah.main_supply_removal_count + vah.night_driving_count + vah.no_halt_zone_count + vah.device_offline_count + vah.device_tamper_count + vah.continuous_driving_count) AS total_violations, 
        ROW_NUMBER() OVER(PARTITION BY vtm.transporter_name ORDER BY SUM(vah.stoppage_violations_count + vah.route_deviation_count + vah.speed_violation_count + vah.main_supply_removal_count + vah.night_driving_count + vah.no_halt_zone_count + vah.device_offline_count + vah.device_tamper_count + vah.continuous_driving_count) DESC) as rn
    FROM vts_alert_history vah
    JOIN vts_truck_master vtm ON vah.tl_number = vtm.truck_no
    WHERE vah.vts_end_datetime >= CURRENT_DATE - INTERVAL '60 days' 
    AND vtm.transporter_name IS NOT NULL
    GROUP BY vtm.transporter_name, vah.tl_number
)
SELECT
    'top_violators_by_transporter' AS "Data Table",
    transporter_name,
    tl_number, 
    total_violations
FROM ranked_vehicles
WHERE rn <= 3
ORDER BY transporter_name, total_violations DESC"""),

    ("For each vehicle, what was the time difference in days between its last two stoppage violations?",
    """WITH ranked_violations AS (
    SELECT
        tl_number,
        vts_end_datetime, 
        LAG(vts_end_datetime, 1) OVER (PARTITION BY tl_number ORDER BY vts_end_datetime) as previous_violation_time
    FROM vts_alert_history
    WHERE stoppage_violations_count > 0
),
latest_violations AS (
    SELECT
        tl_number, 
        vts_end_datetime,
        previous_violation_time,
        ROW_NUMBER() OVER (PARTITION BY tl_number ORDER BY vts_end_datetime DESC) as rn
    FROM ranked_violations
    WHERE previous_violation_time IS NOT NULL
)
SELECT
    'stoppage_violation_gap' AS "Data Table",
    tl_number,
    EXTRACT(DAY FROM (vts_end_datetime - previous_violation_time)) as days_between_violations 
FROM latest_violations
WHERE rn = 1"""),

    ("For each transporter, what percentage of their open alerts are 'HIGH' or 'CRITICAL' severity?",
    """SELECT
    vtm.transporter_name,
    COUNT(*) AS total_open_alerts,
    COUNT(*) FILTER (WHERE a.severity IN ('HIGH', 'CRITICAL')) AS high_critical_alerts,
    ROUND( 
        100.0 * COUNT(*) FILTER (WHERE a.severity IN ('HIGH', 'CRITICAL')) / NULLIF(COUNT(*), 0), 2
    ) AS percentage_high_critical
FROM alerts a
JOIN vts_truck_master vtm ON a.vehicle_number = vtm.truck_no
WHERE a.alert_status = 'OPEN' AND a.alert_section = 'VTS' AND vtm.transporter_name IS NOT NULL
GROUP BY vtm.transporter_name
HAVING COUNT(*) > 0
ORDER BY percentage_high_critical DESC, total_open_alerts DESC"""),

    ("Identify vehicles that had a device_tamper_count event and then a device_offline_count event within the next 24 hours.",
    """WITH event_sequences AS (
    SELECT
        tl_number,
        vts_end_datetime, 
        device_tamper_count,
        LEAD(vts_end_datetime) OVER (PARTITION BY tl_number ORDER BY vts_end_datetime) as next_event_time, 
        LEAD(device_offline_count) OVER (PARTITION BY tl_number ORDER BY vts_end_datetime) as next_event_is_offline
    FROM vts_alert_history
    WHERE device_tamper_count > 0 OR device_offline_count > 0
)
SELECT
    'tamper_followed_by_offline' AS "Data Table",
    tl_number,
    vts_end_datetime AS tamper_event_time, 
    next_event_time AS offline_event_time
FROM event_sequences
WHERE
    device_tamper_count > 0
    AND next_event_is_offline > 0
    AND next_event_time <= vts_end_datetime + INTERVAL '24 hours'
ORDER BY tl_number, tamper_event_time"""),

    ("List all vehicles that have a risk score above the average risk score for their specific transporter.",
    """WITH transporter_avg_risk AS (
    SELECT
        vtm.transporter_code,
        AVG(trs.risk_score) as avg_transporter_risk 
    FROM tt_risk_score trs
    JOIN vts_truck_master vtm ON trs.tt_number = vtm.truck_no
    WHERE vtm.transporter_code IS NOT NULL
    GROUP BY vtm.transporter_code
)
SELECT
    'above_average_risk_vehicles' AS "Data Table",
    trs.tt_number,
    vtm.transporter_name, 
    trs.risk_score,
    tar.avg_transporter_risk::NUMERIC(10, 2) as avg_transporter_risk
FROM tt_risk_score trs
JOIN vts_truck_master vtm ON trs.tt_number = vtm.truck_no
JOIN transporter_avg_risk tar ON vtm.transporter_code = tar.transporter_code
WHERE trs.risk_score > tar.avg_transporter_risk
AND vtm.transporter_name IS NOT NULL
ORDER BY vtm.transporter_name, trs.risk_score DESC"""),

    ("Which vehicles had a stoppage_violation but did not have a speed_violation in the last 30 days?",
    """SELECT
    'stoppage_no_speed' AS "Data Table",
    stoppage_vehicles.tl_number
FROM ( 
    SELECT DISTINCT tl_number FROM vts_alert_history
    WHERE stoppage_violations_count > 0 AND vts_end_datetime >= CURRENT_DATE - INTERVAL '30 days'
) AS stoppage_vehicles
LEFT JOIN ( 
    SELECT DISTINCT tl_number FROM vts_alert_history
    WHERE speed_violation_count > 0 AND vts_end_datetime >= CURRENT_DATE - INTERVAL '30 days'
) AS speed_vehicles ON stoppage_vehicles.tl_number = speed_vehicles.tl_number
WHERE speed_vehicles.tl_number IS NULL"""), 

    ("List the top 10 transporters with the highest number of blacklisted vehicles.",
    """SELECT
    'vts_truck_master' AS "Data Table",
    transporter_name,
    COUNT(*) AS blacklisted_vehicle_count 
FROM vts_truck_master
WHERE whether_truck_blacklisted = 'Y' AND transporter_name IS NOT NULL
GROUP BY transporter_name
ORDER BY blacklisted_vehicle_count DESC
LIMIT 10"""),

    ("What is the average number of compartments for vehicles belonging to the transporter 'AGARWAL TRANSPORT'?",
    """SELECT
    'vts_truck_master' AS "Data Table",
    AVG(no_of_compartments)::NUMERIC(10, 2) AS average_compartments 
FROM vts_truck_master
WHERE transporter_name ILIKE 'AGARWAL TRANSPORT'"""), 

    ("Show me the percentage of trips that are emlock trips, grouped by Business Unit (BU).",
    """SELECT
    'vts_tripauditmaster' AS "Data Table",
    bu,
    COUNT(*) AS total_trips,
    COUNT(*) FILTER (WHERE isemlocktrip = 'Y') AS emlock_trips, 
    ROUND(100.0 * COUNT(*) FILTER (WHERE isemlocktrip = 'Y') / NULLIF(COUNT(*), 0), 2) AS percentage_emlock
FROM vts_tripauditmaster 
WHERE bu IS NOT NULL
GROUP BY bu
ORDER BY bu"""),

    ("How many swipe out failures have been recorded for each zone?",
    """SELECT
    'vts_tripauditmaster' AS "Data Table",
    zone,
    COUNT(*) AS swipe_out_failures 
FROM vts_tripauditmaster
WHERE (swipeoutl1 = 'False' OR swipeoutl2 = 'False') AND zone IS NOT NULL 
GROUP BY zone
ORDER BY swipe_out_failures DESC"""),

    ("Which trucks have the highest rate of swipe-in failures?",
    """SELECT
    'vts_tripauditmaster' AS "Data Table",
    trucknumber,
    COUNT(*) as total_audits,
    SUM(CASE WHEN swipeinl1 = 'False' OR swipeinl2 = 'False' THEN 1 ELSE 0 END) as failed_swipes, 
    ROUND(100.0 * SUM(CASE WHEN swipeinl1 = 'False' OR swipeinl2 = 'False' THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0), 2) as failure_rate_percent
FROM vts_tripauditmaster 
WHERE trucknumber IS NOT NULL GROUP BY trucknumber
HAVING COUNT(*) > 10 -- Only consider trucks with a significant number of audits
ORDER BY failure_rate_percent DESC
LIMIT 10"""),

    ("What is the average risk score for blacklisted vehicles versus non-blacklisted vehicles?",
    """SELECT
    'risk_comparison' AS "Data Table",
    vtm.whether_truck_blacklisted,
    AVG(trs.risk_score)::NUMERIC(10, 2) as average_risk_score, 
    COUNT(DISTINCT vtm.truck_no) as vehicle_count
FROM vts_truck_master vtm
JOIN tt_risk_score trs ON vtm.truck_no = trs.tt_number 
WHERE vtm.whether_truck_blacklisted IN ('Y', 'N')
GROUP BY vtm.whether_truck_blacklisted"""),

    ("List transporters who have at least one vehicle with more than 10 compartments.",
    """SELECT DISTINCT
    'vts_truck_master' AS "Data Table",
    transporter_name
FROM vts_truck_master
WHERE no_of_compartments > 10 AND transporter_name IS NOT NULL
ORDER BY transporter_name"""),

    ("How many swipe out failures were recorded in the 'Mumbai' region in the last 30 days?",
    """SELECT 'vts_tripauditmaster' AS "Data Table", COUNT(*) as swipe_out_failures FROM vts_tripauditmaster WHERE (swipeoutl1 = 'False' OR swipeoutl2 = 'False') AND region ILIKE 'Mumbai' AND createdat >= CURRENT_DATE - INTERVAL '30 days'"""),

    ("what is the transporter risk score for transporter_code of 28121929",
     """SELECT 'transporter_risk_score' AS "Data Table",
    transporter_code, risk_score
FROM transporter_risk_score
WHERE transporter_code = '28121929'"""),

    ("show me the transporter risk for 0028114169",
     """SELECT 'transporter_risk_score' AS "Data Table", transporter_code, risk_score
FROM transporter_risk_score
WHERE transporter_code = '0028114169'"""),

    ("What is the distribution of vehicles by ownership type?",
    """SELECT
    'vts_truck_master' AS "Data Table",
    ownership_type,
    COUNT(*) AS vehicle_count 
FROM vts_truck_master
WHERE ownership_type IS NOT NULL
GROUP BY ownership_type 
ORDER BY vehicle_count DESC"""),

    ("Which user has the most open 'HIGH' severity alerts assigned to them?",
    """SELECT
    'alerts' AS "Data Table",
    assigned_to,
    COUNT(*) as open_high_severity_alerts 
FROM alerts
WHERE alert_status = 'OPEN'
  AND severity = 'HIGH'
  AND assigned_to IS NOT NULL 
GROUP BY assigned_to
ORDER BY open_high_severity_alerts DESC
LIMIT 10"""),

    ("What is the average time to close an alert, grouped by severity?",
    """SELECT
    'alerts' AS "Data Table",
    severity,
    AVG(closed_at - created_at) as average_closure_time 
FROM alerts
WHERE alert_status = 'Close'
  AND closed_at IS NOT NULL 
  AND created_at IS NOT NULL
  AND severity IS NOT NULL
  AND created_at >= CURRENT_DATE - INTERVAL '90 days' -- Apply a time window to make the query more relevant
GROUP BY severity
ORDER BY average_closure_time DESC"""),

    ("Show a daily count of new alerts created over the last 7 days, broken down by severity.",
    """SELECT
    'alerts' AS "Data Table",
    created_at::date as creation_date,
    severity, 
    COUNT(*) as alert_count
FROM alerts
WHERE created_at >= CURRENT_DATE - INTERVAL '7 days' 
  AND severity IS NOT NULL
GROUP BY creation_date, severity
ORDER BY creation_date DESC, severity"""),

    ("List all emlock trips that also had a swipe-in failure.",
    """SELECT
    'vts_tripauditmaster' AS "Data Table",
    trucknumber,
    invoicenumber, 
    createdat
FROM vts_tripauditmaster
WHERE isemlocktrip = 'Y' 
  AND (swipeinl1 = 'False' OR swipeinl2 = 'False')
ORDER BY createdat DESC"""),

    ("what is the vehicle risk score for KA13AA2040",
     """SELECT 'tt_risk_score' AS "Data Table",
    tt_number,
    risk_score,
    total_trips 
FROM tt_risk_score
WHERE tt_number = 'KA13AA2040'"""),

    ("show me the top 10 riskiest vehicles",
     """SELECT 'tt_risk_score' AS "Data Table",
    tt_number,
    risk_score,
    transporter_name 
FROM tt_risk_score
WHERE risk_score IS NOT NULL AND tt_number IS NOT NULL ORDER BY risk_score DESC
LIMIT 10"""),

    ("list all vehicles with a risk score greater than 90",
     """SELECT 'tt_risk_score' AS "Data Table",
    tt_number,
    risk_score,
    total_trips 
FROM tt_risk_score
WHERE risk_score > 90 AND tt_number IS NOT NULL
ORDER BY risk_score DESC"""),

    ("which transporters have the highest power disconnect score",
     """SELECT 'transporter_risk_score' AS "Data Table",
    transporter_code,
    power_disconnect,
    risk_score 
FROM transporter_risk_score
WHERE power_disconnect IS NOT NULL AND transporter_code IS NOT NULL ORDER BY power_disconnect DESC
LIMIT 10"""),

    ("what is the average risk score for all transporters",
     """SELECT 'transporter_risk_score' AS "Data Table",
    AVG(risk_score)::NUMERIC(10, 2) as average_risk_score
FROM transporter_risk_score"""),

    ("show me vehicles with high route deviation and low total trips",
     """SELECT 'tt_risk_score' AS "Data Table",
    tt_number,
    risk_score,
    route_deviation, 
    total_trips
FROM tt_risk_score
WHERE route_deviation > 80 AND total_trips < 5 AND tt_number IS NOT NULL
ORDER BY risk_score DESC"""),


    ("get transporter names for the top 5 riskiest vehicles",
     """SELECT 'tt_risk_score' AS "Data Table",
    trs.tt_number,
    trs.risk_score,
    vtm.transporter_name 
FROM tt_risk_score trs
JOIN vts_truck_master vtm ON trs.tt_number = vtm.truck_no 
WHERE trs.risk_score IS NOT NULL ORDER BY trs.risk_score DESC
LIMIT 5"""),

    ("get transporter names for all vehicles",
     """SELECT 'tt_risk_score' AS "Data Table",
    trs.tt_number,
    trs.risk_score,
    vtm.transporter_name 
FROM tt_risk_score trs
JOIN vts_truck_master vtm ON trs.tt_number = vtm.truck_no 
WHERE trs.risk_score IS NOT NULL ORDER BY vtm.transporter_name"""),

    ("find transporters with risk score between 70 and 80",
     """SELECT 'transporter_risk_score' AS "Data Table",
    transporter_code,
    risk_score
FROM transporter_risk_score 
WHERE risk_score BETWEEN 70 AND 80 AND transporter_code IS NOT NULL
ORDER BY risk_score DESC"""),

    ("give me a risk report for all vehicles in Pune region",
     """SELECT
    'risk_report' AS "Data Table",
    trs.tt_number,
    trs.risk_score,
    vtm.transporter_name, 
    vtm.region
FROM
    tt_risk_score trs
JOIN
    vts_truck_master vtm ON trs.tt_number = vtm.truck_no 
WHERE
    vtm.region ILIKE 'Pune' AND trs.risk_score IS NOT NULL
ORDER BY
    trs.risk_score DESC"""),

    ("compare violation patterns between transporter 28114169 and 28114170 in transporter risk score",
     """SELECT 'transporter_risk_score' AS "Data Table",
    transporter_code,
    risk_score,
    route_deviation, 
    stoppage_violation,
    power_disconnect
FROM transporter_risk_score
WHERE transporter_code IN ('28114169', '28114170')"""),

    ("Show me the top 5 vehicles with the most stoppage violations belonging to the transporter 'AGARWAL TRANSPORT' in the last 90 days.",
     """SELECT
    vah.tl_number,
    SUM(vah.stoppage_violations_count) AS total_stoppage_violations 
FROM
    vts_alert_history vah
JOIN 
    vts_truck_master vtm ON vah.tl_number = vtm.truck_no
WHERE
    vtm.transporter_name ILIKE 'AGARWAL TRANSPORT'
    AND vah.vts_end_datetime >= CURRENT_DATE - INTERVAL '90 days' AND vtm.transporter_name IS NOT NULL
GROUP BY
    vah.tl_number
ORDER BY
    total_stoppage_violations DESC
LIMIT 5"""),

  
    # ===== PRIORITY: Top N Offenders Query =====
    ("Who are the top 5 worst offenders for speeding this quarter",
    """
SELECT
    'vts_alert_history' AS "Data Table",
    tl_number,
    SUM(speed_violation_count) as total_speed_violations
FROM vts_alert_history 
WHERE
    EXTRACT(QUARTER FROM vts_end_datetime) = EXTRACT(QUARTER FROM CURRENT_DATE)
    AND EXTRACT(YEAR FROM vts_end_datetime) = EXTRACT(YEAR FROM CURRENT_DATE)
GROUP BY tl_number
ORDER BY total_speed_violations DESC
LIMIT 5;
"""),
    
    # ===== PRIORITY: Time-based Pattern Analysis =====
    ("Are stoppage violations more common in the morning or in the evening",
    """
SELECT
    CASE
        WHEN EXTRACT(HOUR FROM vts_end_datetime) BETWEEN 6 AND 11 THEN 'Morning (6-12)'
        WHEN EXTRACT(HOUR FROM vts_end_datetime) BETWEEN 12 AND 17 THEN 'Afternoon (12-18)'
        WHEN EXTRACT(HOUR FROM vts_end_datetime) BETWEEN 18 AND 22 THEN 'Evening (18-23)'
        ELSE 'Night (23-6)'
    END as time_of_day,
    SUM(stoppage_violations_count) as total_stoppage_violations
FROM vts_alert_history 
WHERE stoppage_violations_count > 0
  AND vts_end_datetime >= CURRENT_DATE - INTERVAL '90 days'
GROUP BY time_of_day
ORDER BY total_stoppage_violations DESC;
"""),

    # ===== NEW: Generic Lookup Training Examples =====
    ("what is the invoice number for vehicle HR46F0066",
    """SELECT 'vts_alert_history' AS "Source Table", "invoice_number" FROM vts_alert_history WHERE "tl_number" ILIKE 'HR46F0066'
UNION ALL
SELECT 'vts_ongoing_trips' AS "Source Table", "invoice_no" FROM vts_ongoing_trips WHERE "tt_number" ILIKE 'HR46F0066'
UNION ALL
SELECT 'vts_tripauditmaster' AS "Source Table", "invoicenumber" FROM vts_tripauditmaster WHERE "trucknumber" ILIKE 'HR46F0066'
UNION ALL
SELECT 'alerts' AS "Source Table", "invoice_number" FROM alerts WHERE "vehicle_number" ILIKE 'HR46F0066'"""),

    # ===== NEW: Advanced Queries using vts_truck_master for joins/context ONLY =====

    ("Show me high-risk vehicles and their transporters",
    """SELECT
    'tt_risk_score' AS "Data Table",
    trs.tt_number,
    trs.risk_score,
    vtm.transporter_name
FROM tt_risk_score trs
JOIN vts_truck_master vtm ON trs.tt_number = vtm.truck_no
WHERE trs.risk_score > 80
ORDER BY trs.risk_score DESC
LIMIT 20;"""),

    ("Which transporters have the most violations in the North zone?",
    """SELECT
    vtm.transporter_name,
    SUM(vah.stoppage_violations_count + vah.route_deviation_count + vah.speed_violation_count + vah.night_driving_count + vah.device_offline_count + vah.device_tamper_count + vah.continuous_driving_count + vah.no_halt_zone_count + vah.main_supply_removal_count) as total_violations
FROM vts_alert_history vah
JOIN vts_truck_master vtm ON vah.tl_number = vtm.truck_no
WHERE vtm.zone = 'North'
GROUP BY vtm.transporter_name
ORDER BY total_violations DESC
LIMIT 10;"""),

    ("List open critical alerts and the transporter they belong to",
    """SELECT
    'alerts' AS "Data Table",
    a.vehicle_number,
    vtm.transporter_name,
    a.alert_category,
    a.created_at
FROM alerts a
JOIN vts_truck_master vtm ON a.vehicle_number = vtm.truck_no
WHERE a.alert_status = 'OPEN' AND a.severity = 'CRITICAL' AND a.alert_section = 'VTS'
ORDER BY a.created_at DESC;"""),

    ("What is the average vehicle risk score for each transporter?",
    """SELECT
    vtm.transporter_name,
    AVG(trs.risk_score)::NUMERIC(10, 2) as average_vehicle_risk
FROM tt_risk_score trs
JOIN vts_truck_master vtm ON trs.tt_number = vtm.truck_no
GROUP BY vtm.transporter_name
HAVING COUNT(trs.tt_number) > 5 -- Only for transporters with more than 5 vehicles
ORDER BY average_vehicle_risk DESC;"""),

    ("Find vehicles that belong to transporters with a risk score over 60",
    """SELECT
    vtm.truck_no,
    vtm.transporter_name,
    trs.risk_score AS transporter_risk_score
FROM vts_truck_master vtm
JOIN transporter_risk_score trs ON vtm.transporter_code = trs.transporter_code
WHERE trs.risk_score > 60
ORDER BY trs.risk_score DESC;"""),

    ("Show me swipe out failures for vehicles managed by 'AGARWAL TRANSPORT'",
    """SELECT
    tam.trucknumber,
    tam.invoicenumber,
    tam.createdat
FROM vts_tripauditmaster tam
JOIN vts_truck_master vtm ON tam.trucknumber = vtm.truck_no
WHERE vtm.transporter_name ILIKE 'AGARWAL TRANSPORT'
  AND (tam.swipeoutl1 = 'False' OR tam.swipeoutl2 = 'False')
ORDER BY tam.createdat DESC;"""),

    ("Which vehicles in the 'Mumbai' region have the highest number of stoppage violations?",
    """SELECT
    vah.tl_number,
    vtm.transporter_name,
    SUM(vah.stoppage_violations_count) as total_stoppage_violations
FROM vts_alert_history vah
JOIN vts_truck_master vtm ON vah.tl_number = vtm.truck_no
WHERE vtm.region ILIKE 'Mumbai'
GROUP BY vah.tl_number, vtm.transporter_name
ORDER BY total_stoppage_violations DESC
LIMIT 10;"""),

    ("Get the transporter name for the vehicle with the highest risk score",
    """WITH highest_risk_vehicle AS (
    SELECT tt_number
    FROM tt_risk_score
    ORDER BY risk_score DESC
    LIMIT 1
)
SELECT
    vtm.transporter_name,
    hrv.tt_number,
    trs.risk_score
FROM vts_truck_master vtm
JOIN highest_risk_vehicle hrv ON vtm.truck_no = hrv.tt_number
JOIN tt_risk_score trs ON vtm.truck_no = trs.tt_number;"""),

    ("find the vehicle and transporter for invoice 9018439740-ZF23-1777",
    """SELECT
    'vts_alert_history' AS "Source",
    vah.tl_number AS vehicle,
    vtm.transporter_name
FROM vts_alert_history vah
JOIN vts_truck_master vtm ON vah.tl_number = vtm.truck_no
WHERE vah.invoice_number ILIKE '9018439740-ZF23-1777'
UNION ALL
SELECT
    'vts_ongoing_trips' AS "Source",
    vot.tt_number AS vehicle,
    vot.transporter_name
FROM vts_ongoing_trips vot
WHERE vot.invoice_no ILIKE '9018439740-ZF23-1777'"""),

    ("show me the transporter name for invoice 9015995160-ZF23-1435",
    """SELECT 'vts_alert_history' AS "Source Table", vtm.transporter_name
FROM vts_alert_history vah JOIN vts_truck_master vtm ON vah.tl_number = vtm.truck_no
WHERE vah.invoice_number = '9015995160-ZF23-1435'
UNION
SELECT 'vts_ongoing_trips' AS "Source Table", vot.transporter_name
FROM vts_ongoing_trips vot
WHERE vot.invoice_no = '9015995160-ZF23-1435'
UNION ALL
SELECT 'vts_tripauditmaster' AS "Source Table", vtm.transporter_name
FROM vts_tripauditmaster tam JOIN vts_truck_master vtm ON tam.trucknumber = vtm.truck_no
WHERE tam.invoicenumber = '9015995160-ZF23-1435'"""),

    ("find the vehicle and transporter for invoice 9018439740-ZF23-1777",
    """SELECT 'vts_alert_history' AS "Source Table", vtm.truck_no, vtm.transporter_name
FROM vts_alert_history vah JOIN vts_truck_master vtm ON vah.tl_number = vtm.truck_no
WHERE vah.invoice_number = '9018439740-ZF23-1777'
UNION
SELECT 'vts_ongoing_trips' AS "Source Table", vtm.truck_no, vtm.transporter_name
FROM vts_ongoing_trips vot JOIN vts_truck_master vtm ON vot.tt_number = vtm.truck_no
WHERE vot.invoice_no = '9018439740-ZF23-1777'
UNION
SELECT 'vts_tripauditmaster' AS "Source Table", vtm.truck_no, vtm.transporter_name
FROM vts_tripauditmaster tam JOIN vts_truck_master vtm ON tam.trucknumber = vtm.truck_no
WHERE tam.invoicenumber = '9018439740-ZF23-1777'
"""),

    ("how many types of violation are there",
     """SELECT 'vts_alert_history' AS "Source Table", vt AS violation_type, COUNT(*) as count
FROM vts_alert_history, LATERAL unnest(violation_type) as vt
WHERE violation_type IS NOT NULL AND array_length(violation_type, 1) > 0
GROUP BY vt
UNION ALL
SELECT 'vts_ongoing_trips' AS "Source Table", violation_type, COUNT(*) as count
FROM vts_ongoing_trips
WHERE violation_type IS NOT NULL
GROUP BY violation_type
UNION ALL
SELECT 'alerts' AS "Source Table", violation_type, COUNT(*) as count
FROM alerts
WHERE violation_type IS NOT NULL AND alert_section = 'VTS'
GROUP BY violation_type
ORDER BY count DESC"""),

    ("what are the different violation types",
     """SELECT DISTINCT vt AS violation_type FROM vts_alert_history, LATERAL unnest(violation_type) AS vt WHERE vt IS NOT NULL
UNION
SELECT DISTINCT violation_type FROM vts_ongoing_trips WHERE violation_type IS NOT NULL
UNION
SELECT DISTINCT violation_type FROM alerts WHERE violation_type IS NOT NULL AND alert_section = 'VTS'"""),

    # ===== NEW: Zero Violations Query (Specific Vehicle) =====
    ("Did HR46F0066 have zero violations last month",
    """
SELECT
    'vts_truck_master' AS "Data Table",
    vtm.truck_no AS "Vehicle With Zero Violations"
FROM vts_truck_master vtm
LEFT JOIN vts_alert_history vah ON vtm.truck_no = vah.tl_number AND vah.vts_end_datetime >= DATE_TRUNC('month', CURRENT_DATE - INTERVAL '1 month') AND vah.vts_end_datetime < DATE_TRUNC('month', CURRENT_DATE)
WHERE vtm.truck_no ILIKE 'HR46F0066' AND vah.id IS NULL;
"""),

    # ===== NEW: Zero Violations Query =====
    ("Which of my vehicles had zero violations last month",
    """
SELECT
    'vts_truck_master' AS "Data Table",
    vtm.truck_no AS "Vehicle With Zero Violations"
FROM vts_truck_master vtm
LEFT JOIN vts_alert_history vah ON vtm.truck_no = vah.tl_number AND vah.vts_end_datetime >= DATE_TRUNC('month', CURRENT_DATE - INTERVAL '1 month') AND vah.vts_end_datetime < DATE_TRUNC('month', CURRENT_DATE)
WHERE vah.id IS NULL;
"""),

    # ===== NEW: Specific Violation Count Query =====
    ("Which of my vehicles had one violations last month",
    """
SELECT
    'vts_alert_history' AS "Data Table",
    tl_number AS "Vehicle With One Violation"
FROM vts_alert_history
WHERE
    (stoppage_violations_count + route_deviation_count + speed_violation_count + night_driving_count + device_offline_count + device_tamper_count + continuous_driving_count + no_halt_zone_count + main_supply_removal_count) = 1
    AND vts_end_datetime >= DATE_TRUNC('month', CURRENT_DATE - INTERVAL '1 month')
    AND vts_end_datetime < DATE_TRUNC('month', CURRENT_DATE)
GROUP BY tl_number;
"""),

    # ===== NEW: Vehicles with Specific Violation Type and Time Period =====
    ("Which of my vehicles had stoppage violations last month",
    """
SELECT
    'vts_alert_history' AS "Data Table",
    tl_number AS "Vehicle Number",
    SUM(stoppage_violations_count) AS "Total Stoppage Violations"
FROM vts_alert_history
WHERE stoppage_violations_count > 0
    AND vts_end_datetime >= DATE_TRUNC('month', CURRENT_DATE - INTERVAL '1 month')
    AND vts_end_datetime < DATE_TRUNC('month', CURRENT_DATE)
GROUP BY tl_number
ORDER BY "Total Stoppage Violations" DESC;
"""),
    # ===== NEW: Cross-table lookup (Vehicle for Invoice) =====
    ("what are the vehicles under invoice 9015941411-ZF23-1292",
    """SELECT 'vts_alert_history' AS "Source Table", tl_number AS "Vehicle" FROM vts_alert_history WHERE invoice_number ILIKE '9015941411-ZF23-1292'
UNION ALL
SELECT 'vts_ongoing_trips' AS "Source Table", tt_number AS "Vehicle" FROM vts_ongoing_trips WHERE invoice_no ILIKE '9015941411-ZF23-1292'
UNION ALL
SELECT 'vts_tripauditmaster' AS "Source Table", trucknumber AS "Vehicle" FROM vts_tripauditmaster WHERE invoicenumber ILIKE '9015941411-ZF23-1292'"""),

    # ===== PRIORITY 1: Complex Join for High Risk & Recent Violations =====
    ("show me vehicles from a specific transporter that have both high risk scores and recent violations",
    """
SELECT
    'risk_and_violations' AS "Data Table",
    vtm.truck_no,
    vtm.transporter_name,
    trs.risk_score,
    SUM(vah.stoppage_violations_count + vah.route_deviation_count + vah.speed_violation_count + vah.main_supply_removal_count + vah.night_driving_count + vah.no_halt_zone_count + vah.device_offline_count + vah.device_tamper_count + vah.continuous_driving_count) AS total_recent_violations
FROM vts_truck_master vtm
JOIN
    tt_risk_score trs ON vtm.truck_no = trs.tt_number
JOIN
    vts_alert_history vah ON vtm.truck_no = vah.tl_number
WHERE
    vtm.transporter_code = '28114169'
    AND trs.risk_score > 75 -- This is a hardcoded example value
    AND vah.vts_end_datetime >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY 
    vtm.truck_no, vtm.transporter_name, trs.risk_score
ORDER BY
    trs.risk_score DESC, total_recent_violations DESC
"""
    ),

    # ===== PRIORITY 8: Transporter Risk + Violations =====
    ("For transporter 'AGARWAL TRANSPORT', show me the risk scores of vehicles that had a stoppage violation last week",
    """
SELECT
    vtm.truck_no,
    vtm.transporter_name,
    trs.risk_score,
    vah.stoppage_violations_count
FROM
    vts_truck_master vtm
JOIN
    tt_risk_score trs ON vtm.truck_no = trs.tt_number
JOIN
    vts_alert_history vah ON vtm.truck_no = vah.tl_number
WHERE
    vtm.transporter_name ILIKE 'AGARWAL TRANSPORT'
    AND vah.stoppage_violations_count > 0
    AND vah.vts_end_datetime >= CURRENT_DATE - INTERVAL '7 days'
ORDER BY trs.risk_score DESC"""
    ),

    # ===== PRIORITY 3: Fix for Night Time Query =====
    ("identify vehicles with violations consistently occurring between 10 PM and 6 AM",
    """
SELECT
    'vts_alert_history' AS "Data Table",
    tl_number,
    COUNT(*) AS night_violation_count
FROM vts_alert_history
WHERE
    (EXTRACT(HOUR FROM vts_end_datetime) >= 22 OR EXTRACT(HOUR FROM vts_end_datetime) < 6)
    AND (stoppage_violations_count > 0 OR route_deviation_count > 0 OR speed_violation_count > 0 OR night_driving_count > 0 OR device_offline_count > 0 OR device_tamper_count > 0 OR continuous_driving_count > 0 OR no_halt_zone_count > 0 OR main_supply_removal_count > 0)
GROUP BY
    tl_number
HAVING
    COUNT(DISTINCT vts_end_datetime::date) > 3 -- Occurred on more than 3 different days
ORDER BY
    night_violation_count DESC
LIMIT 20
"""
    ),

    ("Which zone has the highest violation rate per vehicle",
    """
SELECT
    'vts_alert_history' AS "Data Table",
    zone,
    SUM(stoppage_violations_count + route_deviation_count + speed_violation_count + night_driving_count + device_offline_count + device_tamper_count + continuous_driving_count + no_halt_zone_count + main_supply_removal_count) AS total_violations,
    COUNT(DISTINCT tl_number) AS unique_vehicles,
    -- Calculate rate per vehicle, avoiding division by zero
    SUM(stoppage_violations_count + route_deviation_count + speed_violation_count + night_driving_count + device_offline_count + device_tamper_count + continuous_driving_count + no_halt_zone_count + main_supply_removal_count)
    /
    NULLIF(COUNT(DISTINCT tl_number), 0) AS violation_rate_per_vehicle
FROM
    vts_alert_history 
WHERE zone IS NOT NULL
GROUP BY
    zone
ORDER BY violation_rate_per_vehicle DESC
"""
    ),

    # ===== NEW: PRIORITY FIX for Day-of-Week Aggregation =====
    ("Which day of the week has the most route deviations?",
    """
SELECT
    'vts_alert_history' AS "Data Table",
    TO_CHAR(vts_end_datetime, 'Day') AS weekday, 
    SUM(route_deviation_count) AS total_route_deviations
FROM
    vts_alert_history
WHERE
    route_deviation_count > 0
GROUP BY
    EXTRACT(DOW FROM vts_end_datetime), weekday
ORDER BY
    total_route_deviations DESC;
"""),

    ("Which day of the week has the most route deviations",
    """
SELECT
    'vts_alert_history' AS "Data Table",
    TO_CHAR(vts_end_datetime, 'Day') as weekday, 
    SUM(route_deviation_count) as total_route_deviations
FROM
    vts_alert_history
GROUP BY
    EXTRACT(DOW FROM vts_end_datetime), TO_CHAR(vts_end_datetime, 'Day')
ORDER BY
    total_route_deviations DESC
"""
    ),

    ("Which locations have the most stoppage violations",
    """
SELECT
    'vts_alert_history' AS "Data Table",
    location_name, 
    SUM(stoppage_violations_count) as total_stoppage_violations
FROM
    vts_alert_history
WHERE
    stoppage_violations_count > 0 AND location_name IS NOT NULL
GROUP BY
    location_name
ORDER BY
    total_stoppage_violations DESC
LIMIT 10
"""
    ),

    ("Show me transporters improving their performance over last quarter",
    """
WITH current_quarter AS (
    SELECT
        vtm.transporter_name, 
        SUM(vah.stoppage_violations_count + vah.route_deviation_count + vah.speed_violation_count + vah.night_driving_count + vah.device_offline_count + vah.device_tamper_count + vah.continuous_driving_count + vah.no_halt_zone_count + vah.main_supply_removal_count) AS total_violations
    FROM vts_alert_history vah
    JOIN vts_truck_master vtm ON vah.tl_number = vtm.truck_no
    WHERE vah.vts_end_datetime >= DATE_TRUNC('quarter', CURRENT_DATE)
    GROUP BY vtm.transporter_name
),
previous_quarter AS ( 
    SELECT
        vtm.transporter_name,
        SUM(vah.stoppage_violations_count + vah.route_deviation_count + vah.speed_violation_count + vah.night_driving_count + vah.device_offline_count + vah.device_tamper_count + vah.continuous_driving_count + vah.no_halt_zone_count + vah.main_supply_removal_count) AS total_violations 
    FROM vts_alert_history vah
    JOIN vts_truck_master vtm ON vah.tl_number = vtm.truck_no
    WHERE vah.vts_end_datetime >= DATE_TRUNC('quarter', CURRENT_DATE) - INTERVAL '3 months'
      AND vah.vts_end_datetime < DATE_TRUNC('quarter', CURRENT_DATE)
    GROUP BY vtm.transporter_name
)
SELECT
    'performance_improvement' AS "Data Table",
    pq.transporter_name, 
    pq.total_violations AS previous_quarter_violations,
    COALESCE(cq.total_violations, 0) AS current_quarter_violations,
    (pq.total_violations - COALESCE(cq.total_violations, 0)) AS improvement
FROM previous_quarter pq
LEFT JOIN current_quarter cq ON pq.transporter_name = cq.transporter_name
WHERE (pq.total_violations - COALESCE(cq.total_violations, 0)) > 0 -- Show only those who improved
ORDER BY improvement DESC
LIMIT 20;
"""
    ),

    # ===== NEW: Geographic Alert Queries =====
    ("alerts in Mumbai zone in last month",
    """SELECT
    'alerts' AS "Data Table",
    a.vehicle_number,
    vtm.transporter_name,
    a.alert_category,
    a.severity,
    a.created_at
FROM alerts a
LEFT JOIN vts_truck_master vtm ON a.vehicle_number = vtm.truck_no
WHERE
    a.alert_section = 'VTS'
    AND a.zone ILIKE 'Mumbai'
    AND a.created_at >= DATE_TRUNC('month', CURRENT_DATE - INTERVAL '1 month') AND a.created_at < DATE_TRUNC('month', CURRENT_DATE)
ORDER BY a.created_at DESC"""),

    ("List transporters operating in multiple zones",
    """
SELECT
    'vts_truck_master' AS "Data Table",
    transporter_name, 
    transporter_code,
    COUNT(DISTINCT zone) as distinct_zone_count,
    array_agg(DISTINCT zone) as zones
FROM
    vts_truck_master
WHERE
    transporter_name IS NOT NULL AND zone IS NOT NULL
GROUP BY
    transporter_name, transporter_code
HAVING
    COUNT(DISTINCT zone) > 1
ORDER BY
    distinct_zone_count DESC;
"""),

    # ===== NEW: PRIORITY - "A but not B" Exclusion Logic =====
    ("Which vehicles had a power disconnect event but have not had a swipe-out failure in the last month?",
    """
SELECT DISTINCT
    'vts_alert_history' AS "Data Table",
    vah.tl_number
FROM vts_alert_history vah
WHERE
    vah.main_supply_removal_count > 0
    AND vah.vts_end_datetime >= CURRENT_DATE - INTERVAL '1 month'
    AND NOT EXISTS (
        SELECT 1
        FROM vts_tripauditmaster tam
        WHERE tam.trucknumber = vah.tl_number
          AND (tam.swipeoutl1 = 'False' OR tam.swipeoutl2 = 'False')
          AND tam.createdat >= CURRENT_DATE - INTERVAL '1 month'
    );"""),

    # ===== NEW: PRIORITY - "Not Blacklisted" Anomaly Detection =====
    ("Show me vehicles that have not reported any data in the last 7 days but are not blacklisted.",
    """
SELECT
    'anomaly_no_data_not_blacklisted' AS "Data Table",
    vtm.truck_no,
    vtm.transporter_name
FROM vts_truck_master vtm
WHERE
    (vtm.whether_truck_blacklisted IS NULL OR vtm.whether_truck_blacklisted != 'Y')
    AND NOT EXISTS (
        SELECT 1 FROM vts_alert_history vah
        WHERE vah.tl_number = vtm.truck_no AND vah.vts_end_datetime >= CURRENT_DATE - INTERVAL '7 days'
    );"""),

    ("Which region has the longest average trip duration",
    """
SELECT
    'vts_alert_history' AS "Data Table",
    region,
    AVG(vts_end_datetime - vts_start_datetime) as average_trip_duration
FROM
    vts_alert_history
WHERE
    region IS NOT NULL
    AND vts_end_datetime IS NOT NULL
    AND vts_start_datetime IS NOT NULL
    AND vts_end_datetime > vts_start_datetime -- Ensure duration is positive
GROUP BY
    region
ORDER BY average_trip_duration DESC;
"""),

    ("Show me regions with increasing violation trends",
    """
WITH recent_violations AS (
    SELECT
        region,
        SUM(stoppage_violations_count + route_deviation_count + speed_violation_count + night_driving_count + device_offline_count + device_tamper_count + continuous_driving_count + no_halt_zone_count + main_supply_removal_count) AS total_violations
    FROM vts_alert_history
    WHERE vts_end_datetime >= CURRENT_DATE - INTERVAL '30 days' AND region IS NOT NULL
    GROUP BY region
),
previous_violations AS (
    SELECT
        region,
        SUM(stoppage_violations_count + route_deviation_count + speed_violation_count + night_driving_count + device_offline_count + device_tamper_count + continuous_driving_count + no_halt_zone_count + main_supply_removal_count) AS total_violations
    FROM vts_alert_history
    WHERE vts_end_datetime >= CURRENT_DATE - INTERVAL '60 days'
      AND vts_end_datetime < CURRENT_DATE - INTERVAL '30 days' AND region IS NOT NULL
    GROUP BY region
)
SELECT
    'violation_trends' AS "Data Table",
    rv.region,
    COALESCE(pv.total_violations, 0) AS previous_30_days_violations,
    rv.total_violations AS recent_30_days_violations,
    (rv.total_violations - COALESCE(pv.total_violations, 0)) AS trend
FROM recent_violations rv
JOIN previous_violations pv ON rv.region = pv.region
WHERE rv.total_violations > COALESCE(pv.total_violations, 0) -- Filter for increasing trends
ORDER BY trend DESC;
"""),

    ("Which vehicles are operating across multiple zones",
    """
SELECT
    'vts_alert_history' AS "Data Table",
    tl_number,
    COUNT(DISTINCT zone) as distinct_zone_count,
    array_agg(DISTINCT zone) as zones_operated_in
FROM
    vts_alert_history
WHERE
    zone IS NOT NULL
GROUP BY
    tl_number
HAVING
    COUNT(DISTINCT zone) > 1
ORDER BY
    distinct_zone_count DESC;
"""
    ),

    ("Which routes have the most violations",
    """
SELECT
    'route_violations' AS "Data Table",
    vot.route_no,
    SUM(
        vah.stoppage_violations_count + vah.route_deviation_count + vah.speed_violation_count +
        vah.night_driving_count + vah.device_offline_count + vah.device_tamper_count +
        vah.continuous_driving_count + vah.no_halt_zone_count + vah.main_supply_removal_count
    ) AS total_violations
FROM
    vts_alert_history vah
JOIN
    vts_ongoing_trips vot ON vah.invoice_number = vot.invoice_no
WHERE
    vot.route_no IS NOT NULL
GROUP BY
    vot.route_no
ORDER BY total_violations DESC
LIMIT 10;
"""),

    # ===== PRIORITY 4: Fix for Ambiguous Column & Joins =====
    ("which trucks from transporter 28114169 are causing the most trouble",
    """
SELECT
    vtm.truck_no,
    vtm.transporter_name,
    trs.risk_score,
    SUM(vah.stoppage_violations_count + vah.route_deviation_count + vah.speed_violation_count + vah.main_supply_removal_count + vah.night_driving_count + vah.no_halt_zone_count + vah.device_offline_count + vah.device_tamper_count + vah.continuous_driving_count) AS total_violations
FROM vts_truck_master vtm
JOIN tt_risk_score trs ON vtm.truck_no = trs.tt_number
JOIN vts_alert_history vah ON vtm.truck_no = vah.tl_number
WHERE vtm.transporter_code = '28114169'
GROUP BY vtm.truck_no, vtm.transporter_name, trs.risk_score
ORDER BY trs.risk_score DESC, total_violations DESC
LIMIT 10
"""
    ),

    # ===== PRIORITY 5: Fix for Grouping Error =====
    ("which vehicles need immediate maintenance attention based on device tamper alerts",
    """
SELECT
    vah.tl_number,
    SUM(vah.device_tamper_count) as total_tamper_events
FROM vts_alert_history vah
WHERE vah.device_tamper_count > 0
GROUP BY vah.tl_number
ORDER BY total_tamper_events DESC
LIMIT 10
"""
    ),

    ("Transporters with good risk scores but poor actual performance",
    """
WITH transporter_performance AS (
    SELECT
        vtm.transporter_code,
        SUM(vah.stoppage_violations_count + vah.route_deviation_count + vah.speed_violation_count + vah.night_driving_count + vah.device_offline_count + vah.device_tamper_count + vah.continuous_driving_count + vah.no_halt_zone_count + vah.main_supply_removal_count) as total_violations
    FROM vts_alert_history vah
    JOIN vts_truck_master vtm ON vah.tl_number = vtm.truck_no
    WHERE vtm.transporter_code IS NOT NULL
    GROUP BY vtm.transporter_code
)
SELECT
    'performance_vs_risk' AS "Data Table",
    trs.transporter_code,
    trs.risk_score,
    tp.total_violations_all
FROM transporter_risk_score trs
JOIN transporter_performance tp ON trs.transporter_code = tp.transporter_code
WHERE trs.risk_score < 50 AND tp.total_violations > 20 -- Define 'good risk' as < 50 and 'poor performance' as > 20 violations
ORDER BY trs.risk_score ASC, tp.total_violations DESC;
"""),

    # ===== PRIORITY 6: Fix for "worst performing" query =====
    ("show me the worst performing trucks this month",
    """
SELECT
    trs.tt_number,
    trs.risk_score,
    vtm.transporter_name,
    SUM(vah.stoppage_violations_count + vah.route_deviation_count + vah.speed_violation_count + vah.main_supply_removal_count + vah.night_driving_count + vah.no_halt_zone_count + vah.device_offline_count + vah.device_tamper_count + vah.continuous_driving_count) as total_violations 
FROM tt_risk_score trs
JOIN vts_alert_history vah ON trs.tt_number = vah.tl_number
JOIN vts_truck_master vtm ON trs.tt_number = vtm.truck_no
WHERE vah.vts_end_datetime >= DATE_TRUNC('month', CURRENT_DATE)
GROUP BY trs.tt_number, trs.risk_score, vtm.transporter_name
ORDER BY 
    trs.risk_score DESC, total_violations DESC
LIMIT 20
"""
    ),
    
    # ===== PRIORITY: Sequential Time Difference Analysis =====
    ("What is the average time between a device_tamper_count event and the subsequent main_supply_removal_count event for the same vehicle?",
    """
WITH EventPairs AS (
    -- Find all tamper events and look for the next event for that vehicle
    SELECT
        tl_number,
        vts_end_datetime,
        -- Find the timestamp of the next event
        LEAD(vts_end_datetime) OVER (PARTITION BY tl_number ORDER BY vts_end_datetime) AS next_event_time,
        -- Check if the next event is a main supply removal
        LEAD(CASE WHEN main_supply_removal_count > 0 THEN 1 ELSE 0 END) OVER (PARTITION BY tl_number ORDER BY vts_end_datetime) AS is_next_event_supply_removal 
    FROM vts_alert_history
    WHERE device_tamper_count > 0 OR main_supply_removal_count > 0
)
-- Calculate the average time difference only for the sequences that match
SELECT AVG(next_event_time - vts_end_datetime) AS average_time_difference FROM EventPairs WHERE is_next_event_supply_removal = 1;"""),

    # =============================================================================================
    # ===== NEW: SCHEMA COVERAGE EXPANSION (BASED ON MINIMAL_SCHEMA ANALYSIS) =====
    # =============================================================================================

    # ===== `alerts` Table Expansion =====
    ("Show me alerts with their assigned user and role",
    """SELECT
    'alerts' AS "Data Table",
    unique_id,
    alert_category,
    assigned_to,
    assigned_to_role,
    created_at
FROM alerts
WHERE assigned_to IS NOT NULL AND alert_section = 'VTS'
ORDER BY created_at DESC
LIMIT 20;
"""),

    ("What was the root cause analysis (RCA) for high severity alerts last week?",
    """SELECT
    'alerts' AS "Data Table",
    unique_id,
    rca,
    rca_type,
    severity,
    closed_at
FROM alerts
WHERE rca IS NOT NULL
  AND severity = 'HIGH'
  AND alert_section = 'VTS'
  AND closed_at >= CURRENT_DATE - INTERVAL '7 days'
ORDER BY closed_at DESC;
"""),

    ("List alerts related to a specific SOP ID",
    """SELECT
    'alerts' AS "Data Table",
    sop_id,
    alert_message,
    vehicle_number,
    created_at
FROM alerts
WHERE sop_id = 'SOP123' AND alert_section = 'VTS'
LIMIT 50;
"""),

    # ===== `vts_alert_history` Table Expansion =====
    ("Which trips were approved by 'system_auto_unblock'?",
    """SELECT
    'vts_alert_history' AS "Data Table",
    tl_number,
    invoice_number,
    approved_by,
    vts_end_datetime
FROM vts_alert_history
WHERE approved_by ILIKE 'system_auto_unblock'
ORDER BY vts_end_datetime DESC
LIMIT 50;
"""),

    ("Show me the report duration for trips with more than 5 total violations",
    """SELECT
    'vts_alert_history' AS "Data Table",
    tl_number,
    report_duration,
    (stoppage_violations_count + route_deviation_count + speed_violation_count + night_driving_count + device_offline_count + device_tamper_count + continuous_driving_count + no_halt_zone_count + main_supply_removal_count) as total_violations
FROM vts_alert_history
WHERE (stoppage_violations_count + route_deviation_count + speed_violation_count + night_driving_count + device_offline_count + device_tamper_count + continuous_driving_count + no_halt_zone_count + main_supply_removal_count) > 5
ORDER BY total_violations DESC
LIMIT 50;
"""),

    # ===== `vts_ongoing_trips` Table Expansion =====
    ("List ongoing trips with their scheduled vs actual start times",
    """SELECT
    'vts_ongoing_trips' AS "Data Table",
    tt_number,
    trip_id,
    scheduled_start_datetime,
    actual_trip_start_datetime,
    (actual_trip_start_datetime - scheduled_start_datetime) as start_delay
FROM vts_ongoing_trips
WHERE actual_trip_start_datetime IS NOT NULL AND scheduled_start_datetime IS NOT NULL
ORDER BY start_delay DESC
LIMIT 20;
"""),

    ("Which drivers are currently on a trip?",
    """SELECT DISTINCT
    'vts_ongoing_trips' AS "Data Table",
    driver_name,
    tt_number,
    transporter_name
FROM vts_ongoing_trips
WHERE driver_name IS NOT NULL;
"""),

    # ===== `vts_truck_master` Table Expansion =====
    ("What is the capacity and ownership type for blacklisted vehicles?",
    """SELECT
    'vts_truck_master' AS "Data Table",
    truck_no,
    transporter_name,
    capacity_of_the_truck,
    ownership_type,
    whether_truck_blacklisted
FROM vts_truck_master
WHERE whether_truck_blacklisted = 'Y';
"""),


    # ===== `vts_tripauditmaster` Table Expansion =====
    ("Show me trip audit records for a specific lock ID",
    """SELECT
    'vts_tripauditmaster' AS "Data Table",
    trucknumber,
    invoicenumber,
    lockid1,
    lockid2,
    createdat
FROM vts_tripauditmaster
WHERE lockid1 = 'LOCK_XYZ' OR lockid2 = 'LOCK_XYZ'
ORDER BY createdat DESC
LIMIT 20;
"""),

    ("What percentage of trips are emlock trips?",
    """SELECT
    'vts_tripauditmaster' AS "Data Table",
    COUNT(*) AS total_trips,
    COUNT(*) FILTER (WHERE isemlocktrip = 'Y') AS emlock_trips,
    ROUND(100.0 * COUNT(*) FILTER (WHERE isemlocktrip = 'Y') / COUNT(*), 2) AS percentage_emlock
FROM vts_tripauditmaster;
"""),

    # ===== `tt_risk_score` & `transporter_risk_score` Expansion =====
    ("Which vehicles have a high device removed score but a low overall risk score?",
    """SELECT
    'tt_risk_score' AS "Data Table",
    tt_number,
    risk_score,
    device_removed
FROM tt_risk_score
WHERE device_removed > 60 AND risk_score < 40
ORDER BY device_removed DESC;
"""),

    ("Compare the average power disconnect scores for the top 5 transporters by risk",
    """WITH top_transporters AS (
    SELECT transporter_code
    FROM transporter_risk_score
    ORDER BY risk_score DESC
    LIMIT 5
)
SELECT
    'transporter_risk_score' AS "Data Table",
    transporter_code,
    risk_score,
    power_disconnect
FROM transporter_risk_score
WHERE transporter_code IN (SELECT transporter_code FROM top_transporters);
"""),

    # ===== ADVANCED MULTI-TABLE JOIN EXAMPLES =====

    ("For blacklisted vehicles, what were their last open alerts before being blacklisted?",
    """WITH ranked_alerts AS (
    SELECT
        a.vehicle_number,
        a.alert_category,
        a.severity,
        a.created_at,
        vtm.last_changed_date AS blacklisted_date,
        ROW_NUMBER() OVER(PARTITION BY a.vehicle_number ORDER BY a.created_at DESC) as rn
    FROM alerts a
    JOIN vts_truck_master vtm ON a.vehicle_number = vtm.truck_no
    WHERE vtm.whether_truck_blacklisted = 'Y'
      AND a.alert_status = 'OPEN'
      AND a.created_at < vtm.last_changed_date
)
SELECT
    'blacklisted_vehicle_alerts' AS "Data Table",
    vehicle_number,
    alert_category,
    severity,
    created_at
FROM ranked_alerts
WHERE rn = 1;
"""),

    ("Show me the drivers of vehicles that had a swipe out failure on an emlock trip",
    """SELECT DISTINCT
    'driver_swipe_failure' AS "Data Table",
    vot.driver_name,
    tam.trucknumber,
    tam.invoicenumber
FROM vts_tripauditmaster tam
JOIN vts_ongoing_trips vot ON tam.invoicenumber = vot.invoice_no
WHERE tam.isemlocktrip = 'Y'
  AND (tam.swipeoutl1 = 'False' OR tam.swipeoutl2 = 'False')
  AND vot.driver_name IS NOT NULL;
"""),

    ("What is the average vehicle risk score for each vehicle ownership type?",
    """SELECT
    'risk_by_ownership' AS "Data Table",
    vtm.ownership_type,
    AVG(trs.risk_score)::NUMERIC(10, 2) as average_risk_score,
    COUNT(vtm.truck_no) as vehicle_count
FROM vts_truck_master vtm
JOIN tt_risk_score trs ON vtm.truck_no = trs.tt_number
WHERE vtm.ownership_type IS NOT NULL
GROUP BY vtm.ownership_type
ORDER BY average_risk_score DESC;
"""),

    # ===== DATA AUDIT & QUALITY QUERIES =====

    ("Are there any vehicles in the risk table that are not in the master truck table?",
    """SELECT
    'data_audit_orphan_risk' AS "Data Table",
    trs.tt_number
FROM tt_risk_score trs
LEFT JOIN vts_truck_master vtm ON trs.tt_number = vtm.truck_no
WHERE vtm.truck_no IS NULL;
"""),

    ("Find trips where the actual start time is before the scheduled start time",
    """SELECT
    'data_audit_early_start' AS "Data Table",
    tt_number,
    trip_id,
    scheduled_start_datetime,
    actual_trip_start_datetime
FROM vts_ongoing_trips
WHERE actual_trip_start_datetime < scheduled_start_datetime;
"""),

    # ===== PRIORITY 7: Fix for location_type queries =====
    ("what are the violations for location_type LPG",
    """
(SELECT
    'vts_alert_history' AS "Data Table",
    tl_number AS "Vehicle Number",
    location_type,
    array_to_string(violation_type, ', ') AS "Violations"
FROM vts_alert_history
WHERE location_type = 'LPG' AND violation_type IS NOT NULL AND array_length(violation_type, 1) > 0)
UNION ALL
(SELECT
    'vts_ongoing_trips' AS "Data Table",
    tt_number AS "Vehicle Number",
    location_type,
    violation_type AS "Violations"
FROM vts_ongoing_trips
WHERE location_type = 'LPG' AND violation_type IS NOT NULL)
"""),

    # ===== PRIORITY 9: Fix for "what tables have..." queries =====
    ("what tables have location_type is LPG",
    """
(SELECT 'vts_alert_history' AS "Data Table", tl_number, location_type
FROM vts_alert_history
WHERE location_type = 'LPG'
LIMIT 5)
UNION ALL
(SELECT 'vts_ongoing_trips' AS "Data Table", tt_number, location_type
FROM vts_ongoing_trips
WHERE location_type = 'LPG'
LIMIT 5)
"""
    ),

# TEST 12: Fix for "currently in region" + "high risk". Model used vts_truck_master instead of vts_ongoing_trips.
    ("show me vehicles currently in Mumbai region with high risk scores",
    """
SELECT
    'ongoing_high_risk' AS "Data Table",
    vot.tt_number,
    vot.vehicle_location,
    trs.risk_score
FROM vts_ongoing_trips vot
JOIN tt_risk_score trs ON vot.tt_number = trs.tt_number
WHERE vot.region ILIKE 'Mumbai' AND trs.risk_score > 70
ORDER BY trs.risk_score DESC
"""
    ),

    # TEST 13: Fix for "missing transporter information". Model made it too complex.
    ("how many vehicles have missing transporter information",
    """
SELECT
    'vts_truck_master' AS "Data Table",
    COUNT(*) as missing_transporter_info_count
FROM vts_truck_master
WHERE transporter_name IS NULL OR transporter_name = ''
"""
    ),

    # TEST 14: Fix for "exist in A but not in B". Model used EXCEPT incorrectly. LEFT JOIN is the correct pattern.
    ("vehicles that exist in vts_alert_history but not in tt_risk_score",
    """
SELECT DISTINCT
    vah.tl_number
FROM vts_alert_history vah
LEFT JOIN tt_risk_score trs ON vah.tl_number = trs.tt_number
WHERE trs.tt_number IS NULL
"""
    ),

    # TEST 15: Fix for "different severity levels". Model incorrectly tried to unnest a non-array column.
    ("what are the different severity levels used in alerts table",
    """
SELECT DISTINCT
    severity
FROM alerts
WHERE severity IS NOT NULL AND alert_section = 'VTS'
"""
    ),

    # TEST 16: Fix for "active trips with pending alerts". Model completely misinterpreted the query.
    ("active trips with pending alerts that are over 2 hours old",
    """
SELECT
    vot.tt_number,
    vot.trip_id,
    a.alert_category,
    a.created_at as alert_time
FROM vts_ongoing_trips vot
JOIN alerts a ON vot.tt_number = a.vehicle_number
WHERE a.alert_status = 'OPEN' AND a.alert_section = 'VTS'
  AND a.created_at < CURRENT_TIMESTAMP - INTERVAL '2 hours'
ORDER BY a.created_at ASC
"""
    ),

    # ===== NEW: Active Trips Query =====
    ("How many trips are currently active in the Mumbai region",
    "SELECT COUNT(*) FROM vts_ongoing_trips WHERE region ILIKE '%Mumbai%' AND actual_trip_start_datetime IS NOT NULL AND event_end_datetime IS NULL"
    ),

    ("show me open VTS alerts with high severity",
    """
SELECT 
    'alerts' AS "Data Table",
    vehicle_number,
    alert_category,
    severity,
    created_at 
FROM alerts 
WHERE alert_section = 'VTS' AND severity = 'High' AND alert_status = 'OPEN'
ORDER BY created_at DESC
"""),

    # ===== PRIORITY 1: High Severity Alerts by Zone =====
    ("List the top 5 zones with the most high-severity alerts?",
    """
SELECT 'alerts' AS "Data Table", zone, COUNT(*) as high_severity_alert_count
FROM alerts
WHERE severity = 'HIGH' AND alert_section = 'VTS' AND zone IS NOT NULL
GROUP BY zone
ORDER BY high_severity_alert_count DESC
LIMIT 5
"""),

    # ===== PRIORITY 2: Complex Aggregation with Arrays =====
    ("find vehicles that had more than 3 different types of violations in a single day",
    """
SELECT
    'vts_alert_history' AS "Data Table",
    tl_number,
    vts_end_datetime::date as violation_date,
    COUNT(DISTINCT vt) as distinct_violation_type
FROM vts_alert_history, LATERAL unnest(violation_type) as vt
WHERE violation_type IS NOT NULL AND array_length(violation_type, 1) > 0
GROUP BY tl_number, vts_end_datetime::date
HAVING COUNT(DISTINCT vt) > 3
ORDER BY distinct_violation_type DESC
"""
    ),

    # ===== VTS Trip Audit Master - Swipe Out Failures =====
    ("List open, high-severity alerts for transporters who have a risk score above 70",
    """SELECT
    'alerts' AS "Data Table",
    a.vehicle_number,
    a.transporter_name,
    a.alert_category,
    a.severity,
    trs.risk_score
FROM alerts a
JOIN vts_truck_master vtm ON a.vehicle_number = vtm.truck_no
JOIN transporter_risk_score trs ON vtm.transporter_code = trs.transporter_code
WHERE
    a.alert_section = 'VTS'
    AND a.alert_status = 'OPEN'
    AND a.severity = 'HIGH'
    AND trs.risk_score > 70 AND vtm.transporter_code IS NOT NULL AND a.transporter_name IS NOT NULL
ORDER BY trs.risk_score DESC
LIMIT 50"""),

    # ===== NEW: CRITICAL 3-TABLE JOIN (Alerts -> Master -> Transporter Risk) =====
    ("List open, high-severity alerts for vehicles belonging to transporters with a risk score above 70",
    """SELECT
    'alerts' AS "Data Table",
    a.vehicle_number,
    vtm.transporter_name,
    a.alert_category,
    a.severity, 
    trs.risk_score AS "Transporter Risk Score"
FROM alerts a
JOIN vts_truck_master vtm ON a.vehicle_number = vtm.truck_no
JOIN transporter_risk_score trs ON vtm.transporter_code = trs.transporter_code
WHERE
    a.alert_section = 'VTS'
    AND a.alert_status = 'OPEN'
    AND a.severity IN ('HIGH', 'CRITICAL')
    AND trs.risk_score > 70
ORDER BY trs.risk_score DESC, a.created_at DESC
LIMIT 50"""),

    ("For each transporter, categorize their vehicles into 'High Risk' (score > 75), 'Medium Risk' (40-75), and 'Low Risk' (score < 40) and show the count for each category",
    """SELECT
    vtm.transporter_name,
    SUM(CASE WHEN trs.risk_score > 75 THEN 1 ELSE 0 END) AS "High Risk Vehicles",
    SUM(CASE WHEN trs.risk_score BETWEEN 40 AND 75 THEN 1 ELSE 0 END) AS "Medium Risk Vehicles",
    SUM(CASE WHEN trs.risk_score < 40 THEN 1 ELSE 0 END) AS "Low Risk Vehicles"
FROM vts_truck_master vtm
JOIN tt_risk_score trs ON vtm.truck_no = trs.tt_number
WHERE vtm.transporter_name IS NOT NULL AND trs.risk_score IS NOT NULL
GROUP BY vtm.transporter_name
ORDER BY "High Risk Vehicles" DESC, "Medium Risk Vehicles" DESC"""),

    ("Which vehicles had a swipe out failure and also a high severity alert on the same day",
    """SELECT
    'cross_table_failure_alert' AS "Data Table",
    t1.trucknumber,
    t1.event_date
FROM (
    SELECT trucknumber, createdat::date AS event_date
    FROM vts_tripauditmaster
    WHERE swipeoutl1 = 'False' OR swipeoutl2 = 'False'
) AS t1
JOIN (
    SELECT vehicle_number, created_at::date AS event_date
    FROM alerts
    WHERE severity = 'HIGH' AND alert_section = 'VTS'
) AS t2 ON t1.trucknumber = t2.vehicle_number AND t1.event_date = t2.event_date
GROUP BY t1.trucknumber, t1.event_date
ORDER BY t1.event_date DESC"""),

    ("How many swipe out failures have occurred?",
    """SELECT 'vts_tripauditmaster' AS "Data Table",
    COUNT(*) as swipe_out_failures
FROM vts_tripauditmaster
WHERE swipeoutl1 = 'False' OR swipeoutl2 = 'False'"""),

    ("Which vehicles had swipe out failures?",
    """SELECT 'vts_tripauditmaster' AS "Data Table",
    trucknumber,
    createdat,
    CASE WHEN swipeoutl1 = 'False' THEN 'L1 Failure' ELSE '' END as failure_l1,
    CASE WHEN swipeoutl2 = 'False' THEN 'L2 Failure' ELSE '' END as failure_l2
FROM vts_tripauditmaster
WHERE swipeoutl1 = 'False' OR swipeoutl2 = 'False'
ORDER BY createdat DESC
LIMIT 50"""),

    ("List vehicles with swipe out alerts",
    """SELECT DISTINCT 'vts_tripauditmaster' AS "Data Table",
    trucknumber
FROM vts_tripauditmaster
WHERE swipeoutl1 = 'False' OR swipeoutl2 = 'False'"""),

    ("Show me the top 10 vehicles with the most swipe out failures",
    """SELECT 'vts_tripauditmaster' AS "Data Table",
    trucknumber,
    COUNT(*) as failure_count
FROM vts_tripauditmaster
WHERE swipeoutl1 = 'False' OR swipeoutl2 = 'False'
GROUP BY trucknumber
ORDER BY failure_count DESC
LIMIT 10"""),

    ("Did vehicle KA13AA2040 have any swipe out failures?",
    """SELECT 'vts_tripauditmaster' AS "Data Table",
    trucknumber,
    createdat,
    invoicenumber
FROM vts_tripauditmaster
WHERE (swipeoutl1 = 'False' OR swipeoutl2 = 'False')
  AND trucknumber = 'KA13AA2040'
ORDER BY createdat DESC"""),

    ("Which vehicles had a swipe out failure and also a high severity alert on the same day",
    """SELECT
    'cross_table_failure_alert' AS "Data Table",
    t1.trucknumber,
    t1.event_date
FROM (
    SELECT trucknumber, createdat::date AS event_date
    FROM vts_tripauditmaster
    WHERE swipeoutl1 = 'False' OR swipeoutl2 = 'False'
) AS t1
JOIN (
    SELECT vehicle_number, created_at::date AS event_date
    FROM alerts
    WHERE severity = 'HIGH' AND alert_section = 'VTS'
) AS t2 ON t1.trucknumber = t2.vehicle_number AND t1.event_date = t2.event_date
GROUP BY t1.trucknumber, t1.event_date
ORDER BY t1.event_date DESC"""),

    ("How many swipe out failures were there on 2024-07-15?",
    """SELECT 'vts_tripauditmaster' AS "Data Table",
    COUNT(*) as swipe_out_failures
FROM vts_tripauditmaster
WHERE (swipeoutl1 = 'False' OR swipeoutl2 = 'False')
  AND createdat::date = '2024-07-15'"""),

    ("Show swipe out failures in the Mumbai region",
    """SELECT 'vts_tripauditmaster' AS "Data Table",
    trucknumber,
    region,
    createdat
FROM vts_tripauditmaster
WHERE (swipeoutl1 = 'False' OR swipeoutl2 = 'False')
  AND region ILIKE 'Mumbai'
ORDER BY createdat DESC"""),

    ("Count swipe out failures by zone",
    """SELECT 'vts_tripauditmaster' AS "Data Table",
    zone,
    COUNT(*) as failure_count
FROM vts_tripauditmaster
WHERE (swipeoutl1 = 'False' OR swipeoutl2 = 'False') AND zone IS NOT NULL
GROUP BY zone
ORDER BY failure_count DESC"""),

    ("Which business unit (BU) has the most swipe out alerts?",
    """SELECT 'vts_tripauditmaster' AS "Data Table",
    bu,
    COUNT(*) as failure_count
FROM vts_tripauditmaster
WHERE (swipeoutl1 = 'False' OR swipeoutl2 = 'False') AND bu IS NOT NULL
GROUP BY bu
ORDER BY failure_count DESC
LIMIT 1"""),

    ("Show a daily trend of swipe out failures for the last 90 days",
    """SELECT 'vts_tripauditmaster' AS "Data Table",
    createdat::date as failure_date,
    COUNT(*) as daily_failures
FROM vts_tripauditmaster
WHERE (swipeoutl1 = 'False' OR swipeoutl2 = 'False')
  AND createdat >= CURRENT_DATE - INTERVAL '90 days'
GROUP BY failure_date
ORDER BY failure_date ASC"""),

# ===== ADD THESE TO TRAINING_QA_PAIRS =====

# 1. Handle "current status" queries better
("what is the current status of vehicle MP07ZL7020",
 """SELECT 'vts_ongoing_trips' AS "Data Table",
    tt_number,
    vehicle_location,
    violation_type,
    created_at
FROM vts_ongoing_trips 
WHERE tt_number ILIKE 'MP07ZL7020'
ORDER BY created_at DESC
LIMIT 1"""),

# 2. Handle ambiguous "all violations" queries
("show all violations",
 """SELECT 'vts_alert_history' AS "Data Table",
    tl_number,
    vts_end_datetime,
    array_to_string(violation_type, ', ') as violations,
    SUM(stoppage_violations_count + route_deviation_count + speed_violation_count + 
        night_driving_count + device_offline_count + device_tamper_count + 
        continuous_driving_count + no_halt_zone_count + main_supply_removal_count) as total_count
FROM vts_alert_history
WHERE violation_type IS NOT NULL AND array_length(violation_type, 1) > 0
GROUP BY tl_number, vts_end_datetime, violation_type
ORDER BY vts_end_datetime DESC
LIMIT 100"""),

# 3. Handle "between dates" with proper syntax
("violations between 2024-01-01 and 2024-12-31",
 """SELECT 'vts_alert_history' AS "Data Table",
    vts_end_datetime::date as date,
    COUNT(DISTINCT tl_number) as unique_vehicles,
    SUM(stoppage_violations_count + route_deviation_count + speed_violation_count + 
        night_driving_count + device_offline_count + device_tamper_count + 
        continuous_driving_count + no_halt_zone_count + main_supply_removal_count) as total_violations
FROM vts_alert_history
WHERE vts_end_datetime::date BETWEEN '2024-01-01' AND '2024-12-31'
GROUP BY vts_end_datetime::date
ORDER BY date DESC"""),

# 4. Handle "risk score of transporter" vs "risk score of vehicle"
("what is the risk score of transporter 28114169",
 """SELECT 'transporter_risk_score' AS "Data Table",
    transporter_name,
    risk_score
FROM transporter_risk_score
WHERE transporter_code = '28114169'"""),

# 5. Handle "most recent" queries
("most recent violations for KA13AA2040",
 """SELECT 'vts_alert_history' AS "Data Table",
    tl_number,
    vts_end_datetime,
    array_to_string(violation_type, ', ') as violations,
    stoppage_violations_count,
    route_deviation_count,
    speed_violation_count
FROM vts_alert_history
WHERE tl_number = 'KA13AA2040'
ORDER BY vts_end_datetime DESC
LIMIT 10"""),

# 6. Handle "percentage" calculations
("what percentage of vehicles have violations",
 """SELECT 'vts_alert_history' AS "Data Table",
    COUNT(DISTINCT CASE WHEN stoppage_violations_count + route_deviation_count + speed_violation_count + 
        night_driving_count + device_offline_count + device_tamper_count + 
        continuous_driving_count + no_halt_zone_count + main_supply_removal_count > 0 
        THEN tl_number END) as vehicles_with_violations,
    COUNT(DISTINCT tl_number) as total_vehicles,
    ROUND(100.0 * COUNT(DISTINCT CASE WHEN stoppage_violations_count + route_deviation_count + speed_violation_count + 
        night_driving_count + device_offline_count + device_tamper_count + 
        continuous_driving_count + no_halt_zone_count + main_supply_removal_count > 0 
        THEN tl_number END) / NULLIF(COUNT(DISTINCT tl_number), 0), 2) as percentage
FROM vts_alert_history"""),

# 7. Handle "average per" queries
("average violations per vehicle",
 """SELECT 'vts_alert_history' AS "Data Table",
    AVG(stoppage_violations_count + route_deviation_count + speed_violation_count + 
        night_driving_count + device_offline_count + device_tamper_count + 
        continuous_driving_count + no_halt_zone_count + main_supply_removal_count)::NUMERIC(10,2) as avg_violations_per_record
FROM vts_alert_history"""),

# 8. Handle "where is" queries (current location)
("where is vehicle MP07ZL7020",
 """SELECT 'vts_ongoing_trips' AS "Data Table",
    tt_number,
    vehicle_location,
    vehicle_latitude,
    vehicle_longitude,
    created_at
FROM vts_ongoing_trips
WHERE tt_number ILIKE 'MP07ZL7020'
ORDER BY created_at DESC
LIMIT 1"""),

# 9. Handle "how many times" queries
("how many times did KA13AA2040 have stoppage violations",
 """SELECT 'vts_alert_history' AS "Data Table",
    tl_number,
    COUNT(*) as occurrence_count,
    SUM(stoppage_violations_count) as total_stoppage_count
FROM vts_alert_history
WHERE tl_number = 'KA13AA2040' AND stoppage_violations_count > 0
GROUP BY tl_number"""),

# 10. Handle "trend over time" queries
("show violation trend for last 6 months",
 """SELECT 'vts_alert_history' AS "Data Table",
    DATE_TRUNC('month', vts_end_datetime) as month,
    SUM(stoppage_violations_count + route_deviation_count + speed_violation_count + 
        night_driving_count + device_offline_count + device_tamper_count + 
        continuous_driving_count + no_halt_zone_count + main_supply_removal_count) as total_violations
FROM vts_alert_history
WHERE vts_end_datetime >= DATE_TRUNC('month', CURRENT_DATE - INTERVAL '6 months')
GROUP BY DATE_TRUNC('month', vts_end_datetime)
ORDER BY month DESC"""),

    # ===== NEW: Night Time / Overnight Queries =====
    ("What violations occurred between 2 AM and 6 AM last night",
    """SELECT 'vts_alert_history' AS "Data Table",
    tl_number,
    vts_end_datetime,
    array_to_string(violation_type, ', ') as violations
FROM vts_alert_history
WHERE (EXTRACT(HOUR FROM vts_end_datetime) BETWEEN 2 AND 5)
  AND vts_end_datetime::date = CURRENT_DATE - INTERVAL '1 day'
ORDER BY vts_end_datetime DESC"""),

    # ===== NEW: Day-of-Week / Weekend Analysis =====
    ("Rank the top 3 most common violation types that occur on weekends (Saturday/Sunday)",
    """SELECT
    'vts_alert_history' AS "Data Table",
    vt.violation_type_element,
    COUNT(*) AS violation_count
FROM
    vts_alert_history vah,
    LATERAL UNNEST(vah.violation_type) AS vt(violation_type_element)
WHERE
    EXTRACT(ISODOW FROM vah.vts_end_datetime) IN (6, 7)
GROUP BY
    vt.violation_type_element
ORDER BY
    violation_count DESC
LIMIT 3;
"""),

    ("Which day of the week has the most route deviations?",
    """SELECT
    'vts_alert_history' AS "Data Table",
    TO_CHAR(vts_end_datetime, 'Day') AS weekday,
    SUM(route_deviation_count) AS total_route_deviations
FROM
    vts_alert_history
WHERE
    route_deviation_count > 0
GROUP BY
    EXTRACT(DOW FROM vts_end_datetime), weekday
ORDER BY
    total_route_deviations DESC;
"""),

    # =============================================================================================
    # ===== NEW: SCHEMA COVERAGE EXPANSION (BASED ON MINIMAL_SCHEMA ANALYSIS) =====
    # =============================================================================================

    # ===== `alerts` Table Expansion =====
    ("Show me alerts with their assigned user and role",
    """SELECT
    'alerts' AS "Data Table",
    unique_id,
    alert_category,
    assigned_to,
    assigned_to_role,
    created_at
FROM alerts
WHERE assigned_to IS NOT NULL AND alert_section = 'VTS'
ORDER BY created_at DESC
LIMIT 20;
"""),

    ("What was the root cause analysis (RCA) for high severity alerts last week?",
    """SELECT
    'alerts' AS "Data Table",
    unique_id,
    rca,
    rca_type,
    severity,
    closed_at
FROM alerts
WHERE rca IS NOT NULL
  AND severity = 'HIGH'
  AND alert_section = 'VTS'
  AND closed_at >= CURRENT_DATE - INTERVAL '7 days'
ORDER BY closed_at DESC;
"""),

    ("List alerts related to a specific SOP ID",
    """SELECT
    'alerts' AS "Data Table",
    sop_id,
    alert_message,
    vehicle_number,
    created_at
FROM alerts
WHERE sop_id = 'SOP123' AND alert_section = 'VTS'
LIMIT 50;
"""),

    # ===== `vts_alert_history` Table Expansion =====
    ("Which trips were approved by 'system_auto_unblock'?",
    """SELECT
    'vts_alert_history' AS "Data Table",
    tl_number,
    invoice_number,
    approved_by,
    vts_end_datetime
FROM vts_alert_history
WHERE approved_by ILIKE 'system_auto_unblock'
ORDER BY vts_end_datetime DESC
LIMIT 50;
"""),

    ("Show me the report duration for trips with more than 5 total violations",
    """SELECT
    'vts_alert_history' AS "Data Table",
    tl_number,
    report_duration,
    (stoppage_violations_count + route_deviation_count + speed_violation_count + night_driving_count + device_offline_count + device_tamper_count + continuous_driving_count + no_halt_zone_count + main_supply_removal_count) as total_violations
FROM vts_alert_history
WHERE (stoppage_violations_count + route_deviation_count + speed_violation_count + night_driving_count + device_offline_count + device_tamper_count + continuous_driving_count + no_halt_zone_count + main_supply_removal_count) > 5
ORDER BY total_violations DESC
LIMIT 50;
"""),

    # ===== `vts_ongoing_trips` Table Expansion =====
    ("List ongoing trips with their scheduled vs actual start times",
    """SELECT
    'vts_ongoing_trips' AS "Data Table",
    tt_number,
    trip_id,
    scheduled_start_datetime,
    actual_trip_start_datetime,
    (actual_trip_start_datetime - scheduled_start_datetime) as start_delay
FROM vts_ongoing_trips
WHERE actual_trip_start_datetime IS NOT NULL AND scheduled_start_datetime IS NOT NULL
ORDER BY start_delay DESC
LIMIT 20;
"""),

    ("Which drivers are currently on a trip?",
    """SELECT DISTINCT
    'vts_ongoing_trips' AS "Data Table",
    driver_name,
    tt_number,
    transporter_name
FROM vts_ongoing_trips
WHERE driver_name IS NOT NULL;
"""),

    # ===== `vts_truck_master` Table Expansion =====
    ("What is the capacity and ownership type for blacklisted vehicles?",
    """SELECT
    'vts_truck_master' AS "Data Table",
    truck_no,
    transporter_name,
    capacity_of_the_truck,
    ownership_type,
    whether_truck_blacklisted
FROM vts_truck_master
WHERE whether_truck_blacklisted = 'Y';
"""),

    ("How many compartments do vehicles of type '20 KL' typically have?",
    """SELECT
    'vts_truck_master' AS "Data Table",
    vehicle_type_desc,
    AVG(no_of_compartments)::NUMERIC(10,2) as avg_compartments,
    MIN(no_of_compartments) as min_compartments,
    MAX(no_of_compartments) as max_compartments
FROM vts_truck_master
WHERE vehicle_type_desc = '20 KL'
GROUP BY vehicle_type_desc;
"""),

    # ===== `vts_tripauditmaster` Table Expansion =====
    ("Show me trip audit records for a specific lock ID",
    """SELECT
    'vts_tripauditmaster' AS "Data Table",
    trucknumber,
    invoicenumber,
    lockid1,
    lockid2,
    createdat
FROM vts_tripauditmaster
WHERE lockid1 = 'LOCK_XYZ' OR lockid2 = 'LOCK_XYZ'
ORDER BY createdat DESC
LIMIT 20;
"""),

    ("What percentage of trips are emlock trips?",
    """SELECT
    'vts_tripauditmaster' AS "Data Table",
    COUNT(*) AS total_trips,
    COUNT(*) FILTER (WHERE isemlocktrip = 'Y') AS emlock_trips,
    ROUND(100.0 * COUNT(*) FILTER (WHERE isemlocktrip = 'Y') / NULLIF(COUNT(*), 0), 2) AS percentage_emlock
FROM vts_tripauditmaster;
"""),

    # ===== `tt_risk_score` & `transporter_risk_score` Expansion =====
    ("Which vehicles have a high device removed score but a low overall risk score?",
    """SELECT
    'tt_risk_score' AS "Data Table",
    tt_number,
    risk_score,
    device_removed
FROM tt_risk_score
WHERE device_removed > 60 AND risk_score < 40
ORDER BY device_removed DESC;
"""),

    ("Compare the average power disconnect scores for the top 5 transporters by risk",
    """WITH top_transporters AS (
    SELECT transporter_code
    FROM transporter_risk_score
    ORDER BY risk_score DESC
    LIMIT 5
)
SELECT
    'transporter_risk_score' AS "Data Table",
    transporter_code,
    risk_score,
    power_disconnect
FROM transporter_risk_score
WHERE transporter_code IN (SELECT transporter_code FROM top_transporters);
"""),

    # ===== ADVANCED MULTI-TABLE JOIN EXAMPLES =====

    ("For blacklisted vehicles, what were their last open alerts before being blacklisted?",
    """WITH ranked_alerts AS (
    SELECT
        a.vehicle_number,
        a.alert_category,
        a.severity,
        a.created_at,
        vtm.last_changed_date AS blacklisted_date,
        ROW_NUMBER() OVER(PARTITION BY a.vehicle_number ORDER BY a.created_at DESC) as rn
    FROM alerts a
    JOIN vts_truck_master vtm ON a.vehicle_number = vtm.truck_no
    WHERE vtm.whether_truck_blacklisted = 'Y'
      AND a.alert_status = 'OPEN'
      AND a.created_at < vtm.last_changed_date
)
SELECT
    'blacklisted_vehicle_alerts' AS "Data Table",
    vehicle_number,
    alert_category,
    severity,
    created_at
FROM ranked_alerts
WHERE rn = 1;
"""),

    ("Show me the drivers of vehicles that had a swipe out failure on an emlock trip",
    """SELECT DISTINCT
    'driver_swipe_failure' AS "Data Table",
    vot.driver_name,
    tam.trucknumber,
    tam.invoicenumber
FROM vts_tripauditmaster tam
JOIN vts_ongoing_trips vot ON tam.invoicenumber = vot.invoice_no
WHERE tam.isemlocktrip = 'Y'
  AND (tam.swipeoutl1 = 'False' OR tam.swipeoutl2 = 'False')
  AND vot.driver_name IS NOT NULL;
"""),

    ("What is the average vehicle risk score for each vehicle ownership type?",
    """SELECT
    'risk_by_ownership' AS "Data Table",
    vtm.ownership_type,
    AVG(trs.risk_score)::NUMERIC(10, 2) as average_risk_score,
    COUNT(vtm.truck_no) as vehicle_count
FROM vts_truck_master vtm
JOIN tt_risk_score trs ON vtm.truck_no = trs.tt_number
WHERE vtm.ownership_type IS NOT NULL
GROUP BY vtm.ownership_type
ORDER BY average_risk_score DESC;
"""),

    ("Find trips where the actual start time is before the scheduled start time",
    """SELECT
    'data_audit_early_start' AS "Data Table",
    tt_number,
    trip_id,
    scheduled_start_datetime,
    actual_trip_start_datetime
FROM vts_ongoing_trips
WHERE actual_trip_start_datetime < scheduled_start_datetime;
"""),

    # ===== NEW: Anomaly Detection (A exists but B is zero) =====
    ("List vehicles that have a risk_score in tt_risk_score but their transporter has zero recorded trips in transporter_risk_score",
    """SELECT 
    'tt_risk_score' AS "Data Table",
    trs.tt_number,
    trs.risk_score AS vehicle_risk_score,
    trs.transporter_code,
    trs2.total_trips AS transporter_total_trips 
FROM
    tt_risk_score trs
JOIN
    transporter_risk_score trs2 ON trs.transporter_code = trs2.transporter_code
WHERE
    trs2.total_trips = 0;
"""),
    
    ("List vehicles that have a risk_score in tt_risk_score and show how many trips their transporter has in transporter_risk_score",
    """SELECT 
    'tt_risk_score' AS "Data Table",
    trs.tt_number AS vehicle_number,
    trs.risk_score AS vehicle_risk_score,
    trs.total_trips AS vehicle_total_trips,
    trs2.transporter_code,
    trs2.total_trips AS transporter_total_trips,
    trs2.risk_score AS transporter_risk_score
FROM tt_risk_score trs 
JOIN vts_truck_master vtm
    ON trs.tt_number = vtm.truck_no
JOIN transporter_risk_score trs2 
    ON vtm.transporter_code = trs2.transporter_code
ORDER BY
    trs.risk_score DESC;
"""),

    # =============================================================================================
    # ===== NEW: ADVANCED TRAINING DATA (ROUND 2) =====
    # =============================================================================================

    # ===== JSONB Column Queries (alerts.raw_data) =====
    ("Find alerts where the raw_data contains a 'battery_voltage' below 3.5",
    """SELECT
    'alerts' AS "Data Table",
    vehicle_number,
    alert_category,
    raw_data ->> 'battery_voltage' AS battery_voltage,
    created_at
FROM alerts
WHERE alert_section = 'VTS'
  AND (raw_data ->> 'battery_voltage')::numeric < 3.5
ORDER BY created_at DESC
LIMIT 20;
"""),

    ("Count alerts by the 'event_code' found in the raw_data json",
    """SELECT
    'alerts' AS "Data Table",
    raw_data ->> 'event_code' AS event_code,
    COUNT(*) as alert_count
FROM alerts
WHERE alert_section = 'VTS'
  AND raw_data ? 'event_code' -- Check if the key exists
GROUP BY raw_data ->> 'event_code'
ORDER BY alert_count DESC;
"""),

    # ===== Advanced Window Functions (RANK, DENSE_RANK) =====
    ("For each zone, rank the top 3 vehicles by their total number of violations",
    """WITH vehicle_violations AS (
    SELECT
        zone,
        tl_number,
        SUM(stoppage_violations_count + route_deviation_count + speed_violation_count + night_driving_count + device_offline_count + device_tamper_count + continuous_driving_count + no_halt_zone_count + main_supply_removal_count) as total_violations
    FROM vts_alert_history
    WHERE zone IS NOT NULL
    GROUP BY zone, tl_number
),
ranked_vehicles AS (
    SELECT
        zone,
        tl_number,
        total_violations,
        RANK() OVER (PARTITION BY zone ORDER BY total_violations DESC) as rank_in_zone
    FROM vehicle_violations
)
SELECT
    'ranked_violators_by_zone' AS "Data Table",
    zone,
    tl_number,
    total_violations,
    rank_in_zone
FROM ranked_vehicles
WHERE rank_in_zone <= 3;
"""),

    # ===== NEW: PRIORITY - Complex Percentage/Utilization Logic =====
    ("Which transporters are over-utilizing their high-risk vehicles? Show transporters where more than 50% of their active trips involve vehicles with a risk score over 75.",
    """
WITH TransporterActivity AS (
    -- Step 1: Aggregate trip counts for each transporter
    SELECT
        vtm.transporter_name,
        -- Count of trips with high-risk vehicles
        COUNT(CASE WHEN trs.risk_score > 75 THEN vah.invoice_number END) AS high_risk_trip_count,
        -- Total count of all trips
        COUNT(vah.invoice_number) AS total_trip_count
    FROM vts_alert_history vah
    -- Join to get transporter name
    JOIN vts_truck_master vtm ON vah.tl_number = vtm.truck_no
    -- Join to get risk score
    JOIN tt_risk_score trs ON vah.tl_number = trs.tt_number
    WHERE vah.vts_end_datetime >= CURRENT_DATE - INTERVAL '90 days' -- Define "active" as last 90 days
    GROUP BY vtm.transporter_name
)
-- Step 2: Calculate the percentage and filter
SELECT transporter_name, high_risk_trip_count, total_trip_count
FROM TransporterActivity
WHERE (high_risk_trip_count * 100.0 / NULLIF(total_trip_count, 0)) > 50;
"""),

    # ===== NEW: Corrected Transporter Violation Query (Fix for bad cache example) =====
    ("which transporters have more violations?",
    """
SELECT
    vtm.transporter_name,
    SUM(
        COALESCE(vah.stoppage_violations_count, 0) + COALESCE(vah.route_deviation_count, 0) +
        COALESCE(vah.speed_violation_count, 0) + COALESCE(vah.night_driving_count, 0) +
        COALESCE(vah.device_offline_count, 0) + COALESCE(vah.device_tamper_count, 0) +
        COALESCE(vah.continuous_driving_count, 0) + COALESCE(vah.no_halt_zone_count, 0) +
        COALESCE(vah.main_supply_removal_count, 0)
    ) AS total_violations
FROM vts_alert_history vah
JOIN vts_truck_master vtm ON vah.tl_number = vtm.truck_no
GROUP BY vtm.transporter_name
ORDER BY total_violations DESC;"""),

    ("Show the dense rank of transporters based on their average vehicle risk score",
    """WITH transporter_avg_risk AS (
    SELECT
        vtm.transporter_name,
        AVG(trs.risk_score) as avg_risk
    FROM tt_risk_score trs
    JOIN vts_truck_master vtm ON trs.tt_number = vtm.truck_no
    WHERE vtm.transporter_name IS NOT NULL
    GROUP BY vtm.transporter_name
)
SELECT
    'transporter_risk_rank' AS "Data Table",
    transporter_name,
    avg_risk::NUMERIC(10, 2) as average_risk,
    DENSE_RANK() OVER (ORDER BY avg_risk DESC) as risk_rank
FROM transporter_avg_risk;
"""),

    # ===== Complex Multi-Table Joins (4+ tables) =====
    ("For open 'HIGH' severity alerts, show the driver name, vehicle risk score, and whether it was an emlock trip",
    """SELECT
    'complex_alert_context' AS "Data Table",
    a.vehicle_number,
    vot.driver_name,
    trs.risk_score,
    tam.isemlocktrip,
    a.alert_category,
    a.created_at
FROM alerts a
LEFT JOIN vts_ongoing_trips vot ON a.vehicle_number = vot.tt_number AND vot.actual_trip_start_datetime IS NOT NULL AND vot.event_end_datetime IS NULL -- Join on active trips
LEFT JOIN tt_risk_score trs ON a.vehicle_number = trs.tt_number
LEFT JOIN vts_tripauditmaster tam ON a.invoice_number = tam.invoicenumber
WHERE a.alert_section = 'VTS'
  AND a.alert_status = 'OPEN'
  AND a.severity = 'HIGH'
ORDER BY a.created_at DESC;
"""),

    # ===== Queries with Complex CASE Statements =====
    ("Categorize all vehicles based on their risk score and blacklisted status",
    """SELECT
    'vehicle_categorization' AS "Data Table",
    vtm.truck_no,
    trs.risk_score,
    CASE
        WHEN vtm.whether_truck_blacklisted = 'Y' THEN 'blacklisted'
        WHEN trs.risk_score > 80 THEN 'Very High Risk'
        WHEN trs.risk_score > 60 THEN 'High Risk'
        WHEN trs.risk_score > 40 THEN 'Medium Risk'
        ELSE 'Low Risk'
    END as risk_category
FROM vts_truck_master vtm
LEFT JOIN tt_risk_score trs ON vtm.truck_no = trs.tt_number;
"""),

    # ===== Queries with HAVING clause on multiple aggregations =====
    ("Find transporters who have more than 10 vehicles and an average vehicle risk score below 50",
    """SELECT
    'efficient_transporters' AS "Data Table",
    vtm.transporter_name,
    COUNT(DISTINCT vtm.truck_no) as vehicle_count,
    AVG(trs.risk_score)::NUMERIC(10, 2) as average_risk
FROM vts_truck_master vtm
JOIN tt_risk_score trs ON vtm.truck_no = trs.tt_number
WHERE vtm.transporter_name IS NOT NULL
GROUP BY vtm.transporter_name
HAVING COUNT(DISTINCT vtm.truck_no) > 10 AND AVG(trs.risk_score) < 50
ORDER BY average_risk ASC;
"""),

    # ===== More Array Operations (`@>`) =====
    ("Find trips that had both a 'stoppage violation' and a 'speed violation'",
    """SELECT
    'vts_alert_history' AS "Data Table",
    tl_number,
    invoice_number,
    vts_end_datetime,
    array_to_string(violation_type, ', ') as violations
FROM vts_alert_history
WHERE violation_type @> ARRAY['stoppage violation', 'speed violation']
ORDER BY vts_end_datetime DESC;
"""),

    # ===== Self-Join Pattern (less common but powerful) =====
    ("Find pairs of alerts for the same vehicle that occurred within 1 hour of each other",
    """SELECT
    'alert_pairs' AS "Data Table",
    a1.vehicle_number,
    a1.alert_category as first_alert,
    a1.created_at as first_alert_time,
    a2.alert_category as second_alert,
    a2.created_at as second_alert_time
FROM alerts a1
JOIN alerts a2 ON a1.vehicle_number = a2.vehicle_number
WHERE a1.id < a2.id -- Avoid duplicate pairs and self-joins
  AND a1.alert_section = 'VTS' AND a2.alert_section = 'VTS'
  AND a2.created_at BETWEEN a1.created_at AND a1.created_at + INTERVAL '1 hour'
ORDER BY a1.vehicle_number, a1.created_at;
"""),

    # =============================================================================================
    # ===== NEW COMPREHENSIVE TRAINING DATA (MAJOR EXPANSION) =====
    # =============================================================================================

    # ===== ADVANCED SEQUENTIAL & TIME-GAP ANALYSIS =====
    ("For vehicles with more than 5 `device_tamper_count` events, what is the average time until the next `device_offline_count` event?",
    """WITH tamper_events AS (
    SELECT
        vah.tl_number,
        vah.vts_end_datetime
    FROM vts_alert_history vah
    WHERE vah.device_tamper_count > 0
      AND vah.tl_number IN (
        -- Subquery to find vehicles with more than 5 total tamper events
        SELECT tl_number
        FROM vts_alert_history
        WHERE device_tamper_count > 0 
        GROUP BY tl_number
        HAVING SUM(device_tamper_count) > 5
      )
),
event_stream AS (
    SELECT
        te.tl_number,
        te.vts_end_datetime AS tamper_time,
        -- Use a correlated subquery to find the first offline event after the tamper event. This is more compatible than IGNORE NULLS.
        (SELECT MIN(vah_inner.vts_end_datetime)
         FROM vts_alert_history vah_inner 
         WHERE vah_inner.tl_number = te.tl_number
           AND vah_inner.device_offline_count > 0
           AND vah_inner.vts_end_datetime > te.vts_end_datetime
        ) AS next_offline_time
    FROM tamper_events te
)
SELECT
    'sequential_analysis' AS "Data Table", 
    AVG(next_offline_time - tamper_time) AS average_time_to_next_offline
FROM event_stream
WHERE next_offline_time IS NOT NULL;
"""),

    ("Which violation type has the highest probability of being followed by a 'device offline' event within 3 hours?",
    """WITH event_pairs AS (
    SELECT
        vt.violation_type_element as current_violation,
        LEAD(vah.device_offline_count > 0) OVER (PARTITION BY vah.tl_number ORDER BY vah.vts_end_datetime) as next_is_offline,
        LEAD(vah.vts_end_datetime) OVER (PARTITION BY vah.tl_number ORDER BY vah.vts_end_datetime) as next_event_time,
        vah.vts_end_datetime as current_event_time 
    FROM vts_alert_history vah,
    LATERAL UNNEST(vah.violation_type) AS vt(violation_type_element)
    WHERE vah.violation_type IS NOT NULL AND array_length(vah.violation_type, 1) > 0
),
violation_stats AS (
    SELECT
        current_violation,
        COUNT(*) as total_occurrences,
        COUNT(*) FILTER (WHERE next_is_offline = true AND (next_event_time - current_event_time) <= INTERVAL '3 hours') as followed_by_offline_count 
    FROM event_pairs
    WHERE current_violation != 'device_offline_count > 0' -- Exclude offline as the starting event
    GROUP BY current_violation
)
SELECT
    'violation_probability' AS "Data Table", 
    current_violation,
    total_occurrences,
    followed_by_offline_count,
    ROUND(100.0 * followed_by_offline_count / NULLIF(total_occurrences, 0), 2) as probability_percent
FROM violation_stats
WHERE total_occurrences > 50 -- Only consider violations that occur frequently enough to be statistically relevant
ORDER BY probability_percent DESC;
"""),

    # ===== ANOMALY & CONTRADICTION DETECTION =====
    ("Are there any blacklisted vehicles that are currently on an ongoing trip?",
    """SELECT
    'anomaly_blacklisted_active' AS "Data Table",
    vot.tt_number,
    vot.transporter_name,
    vot.vehicle_location,
    vot.created_at AS trip_start_time
FROM vts_ongoing_trips vot
JOIN vts_truck_master vtm ON vot.tt_number = vtm.truck_no
WHERE vtm.whether_truck_blacklisted = 'Y'
"""),

    # ===== CORRELATION & STATISTICAL ANALYSIS =====
    ("What is the correlation between `stoppage_violation` score in `tt_risk_score` and the actual `stoppage_violations_count` in `vts_alert_history` for the top 10 riskiest vehicles?",
    """WITH top_vehicles AS (
    SELECT tt_number
    FROM tt_risk_score
    ORDER BY risk_score DESC
    LIMIT 10
),
risk_data AS (
    SELECT
        tt_number, 
        stoppage_violation as risk_stoppage_score
    FROM tt_risk_score
    WHERE tt_number IN (SELECT tt_number FROM top_vehicles)
),
violation_data AS (
    SELECT
        tl_number, 
        SUM(stoppage_violations_count) as actual_stoppage_count
    FROM vts_alert_history
    WHERE tl_number IN (SELECT tt_number FROM top_vehicles)
    GROUP BY tl_number
)
SELECT
    'correlation_analysis' AS "Data Table",
    rd.tt_number, 
    rd.risk_stoppage_score,
    vd.actual_stoppage_count,
    CORR(rd.risk_stoppage_score, vd.actual_stoppage_count) OVER () as correlation_coefficient
FROM risk_data rd
JOIN violation_data vd ON rd.tt_number = vd.tl_number;
"""),

    # ===== DATA QUALITY & AUDITING =====
    ("Which vehicles appear in `vts_alert_history` but are missing from the `vts_truck_master` table?",
    """SELECT DISTINCT
    'data_quality_orphan_vehicles' AS "Data Table",
    vah.tl_number 
FROM vts_alert_history vah
LEFT JOIN vts_truck_master vtm ON vah.tl_number = vtm.truck_no
WHERE vtm.truck_no IS NULL;
"""),

    # ===== CLIENT-SIDE / BUSINESS-FOCUSED QUESTIONS =====
    ("Which transporter has the most efficient routes, measured by lowest number of violations per trip?",
    """SELECT
    'transporter_efficiency' AS "Data Table",
    vtm.transporter_name,
    COUNT(DISTINCT vah.invoice_number) AS total_trips,
    SUM(vah.route_deviation_count + vah.stoppage_violations_count) AS total_violations, 
    (SUM(vah.route_deviation_count + vah.stoppage_violations_count) * 1.0) / NULLIF(COUNT(DISTINCT vah.invoice_number), 0) AS avg_violations_per_trip
FROM vts_alert_history vah
JOIN vts_truck_master vtm ON vah.tl_number = vtm.truck_no
WHERE vah.invoice_number IS NOT NULL AND vtm.transporter_name IS NOT NULL
GROUP BY vtm.transporter_name
HAVING COUNT(DISTINCT vah.invoice_number) > 10 -- Only consider transporters with a meaningful number of trips
ORDER BY avg_violations_per_trip ASC
LIMIT 10;
"""),

    ("What is the financial impact of violations this month, assuming each violation costs $50?",
    """SELECT
    'financial_impact' AS "Data Table",
    SUM(stoppage_violations_count + route_deviation_count + speed_violation_count + night_driving_count + device_offline_count + device_tamper_count + continuous_driving_count + no_halt_zone_count + main_supply_removal_count) as total_violations, 
    SUM(stoppage_violations_count + route_deviation_count + speed_violation_count + night_driving_count + device_offline_count + device_tamper_count + continuous_driving_count + no_halt_zone_count + main_supply_removal_count) * 50.00 as estimated_cost
FROM vts_alert_history
WHERE vts_end_datetime >= DATE_TRUNC('month', CURRENT_DATE);
"""),

    ("Give me a dashboard summary for today's operations",
    """SELECT 'dashboard' AS "Data Table", 'Active Trips' as metric, COUNT(*) as value FROM vts_ongoing_trips
UNION ALL
SELECT 'dashboard' AS "Data Table", 'Open High-Severity Alerts' as metric, COUNT(*) as value FROM alerts WHERE alert_status = 'OPEN' AND severity = 'HIGH' AND alert_section = 'VTS'
UNION ALL
SELECT 'dashboard' AS "Data Table", 'Total Violations Today' as metric, SUM(stoppage_violations_count + route_deviation_count + speed_violation_count + night_driving_count + device_offline_count + device_tamper_count + continuous_driving_count + no_halt_zone_count + main_supply_removal_count) as value FROM vts_alert_history WHERE vts_end_datetime::date = CURRENT_DATE;
"""),
("Show me alerts with their assigned user and role",
    """SELECT
    'alerts' AS "Data Table",
    unique_id,
    alert_category,
    assigned_to,
    assigned_to_role,
    created_at
FROM alerts
WHERE assigned_to IS NOT NULL AND alert_section = 'VTS'
ORDER BY created_at DESC
LIMIT 20;
"""),

    ("What was the root cause analysis (RCA) for high severity alerts last week?",
    """SELECT
    'alerts' AS "Data Table",
    unique_id,
    rca,
    rca_type,
    severity,
    closed_at
FROM alerts
WHERE rca IS NOT NULL
  AND severity = 'HIGH'
  AND alert_section = 'VTS'
  AND closed_at >= CURRENT_DATE - INTERVAL '7 days'
ORDER BY closed_at DESC;
"""),


    ("List alerts related to a specific SOP ID",
    """SELECT
    'alerts' AS "Data Table",
    sop_id,
    alert_message,
    vehicle_number,
    created_at
FROM alerts
WHERE sop_id = 'SOP123' AND alert_section = 'VTS'
LIMIT 50;
"""),

    # ===== `vts_alert_history` Table Expansion =====
    ("Which trips were approved by 'system_auto_unblock'?",
    """SELECT
    'vts_alert_history' AS "Data Table",
    tl_number,
    invoice_number,
    approved_by,
    vts_end_datetime
FROM vts_alert_history
WHERE approved_by ILIKE 'system_auto_unblock'
ORDER BY vts_end_datetime DESC
LIMIT 50;
"""),

    ("Show me the report duration for trips with more than 5 total violations",
    """SELECT
    'vts_alert_history' AS "Data Table",
    tl_number,
    report_duration,
    (stoppage_violations_count + route_deviation_count + speed_violation_count + night_driving_count + device_offline_count + device_tamper_count + continuous_driving_count + no_halt_zone_count + main_supply_removal_count) as total_violations
FROM vts_alert_history
WHERE (stoppage_violations_count + route_deviation_count + speed_violation_count + night_driving_count + device_offline_count + device_tamper_count + continuous_driving_count + no_halt_zone_count + main_supply_removal_count) > 5
ORDER BY total_violations DESC
LIMIT 100;
"""),

    # ===== `vts_ongoing_trips` Table Expansion =====
    ("List ongoing trips with their scheduled vs actual start times",
    """SELECT
    'vts_ongoing_trips' AS "Data Table",
    tt_number,
    trip_id,
    scheduled_start_datetime,
    actual_trip_start_datetime,
    (actual_trip_start_datetime - scheduled_start_datetime) as start_delay
FROM vts_ongoing_trips
WHERE actual_trip_start_datetime IS NOT NULL AND scheduled_start_datetime IS NOT NULL
ORDER BY start_delay DESC
LIMIT 100;
"""),

    ("Which drivers are currently on a trip?",
    """SELECT DISTINCT
    'vts_ongoing_trips' AS "Data Table",
    driver_name,
    tt_number,
    transporter_name
FROM vts_ongoing_trips
WHERE driver_name IS NOT NULL;
"""),

    # ===== `vts_truck_master` Table Expansion =====
    ("What is the capacity and ownership type for blacklisted vehicles?",
    """SELECT
    'vts_truck_master' AS "Data Table",
    truck_no,
    transporter_name,
    capacity_of_the_truck,
    ownership_type,
    whether_truck_blacklisted
FROM vts_truck_master
WHERE whether_truck_blacklisted = 'Y';
"""),

    ("How many compartments do vehicles of type '20 KL' typically have?",
    """SELECT
    'vts_truck_master' AS "Data Table",
    vehicle_type_desc,
    AVG(no_of_compartments)::NUMERIC(10,2) as avg_compartments,
    MIN(no_of_compartments) as min_compartments,
    MAX(no_of_compartments) as max_compartments
FROM vts_truck_master
WHERE vehicle_type_desc = '20 KL'
GROUP BY vehicle_type_desc;
"""),

    # ===== `vts_tripauditmaster` Table Expansion =====
    ("Show me trip audit records for a specific lock ID",
    """SELECT
    'vts_tripauditmaster' AS "Data Table",
    trucknumber,
    invoicenumber,
    lockid1,
    lockid2,
    createdat
FROM vts_tripauditmaster
WHERE lockid1 = 'LOCK_XYZ' OR lockid2 = 'LOCK_XYZ'
ORDER BY createdat DESC
LIMIT 100;
"""),

    ("What percentage of trips are emlock trips?",
    """SELECT
    'vts_tripauditmaster' AS "Data Table",
    COUNT(*) AS total_trips,
    COUNT(*) FILTER (WHERE isemlocktrip = 'Y') AS emlock_trips,
    ROUND(100.0 * COUNT(*) FILTER (WHERE isemlocktrip = 'Y') / COUNT(*), 2) AS percentage_emlock
FROM vts_tripauditmaster;
"""),

    # ===== `tt_risk_score` & `transporter_risk_score` Expansion =====
    ("Which vehicles have a high device removed score but a low overall risk score?",
    """SELECT
    'tt_risk_score' AS "Data Table",
    tt_number,
    risk_score,
    device_removed
FROM tt_risk_score
WHERE device_removed > 60 AND risk_score < 40
ORDER BY device_removed DESC;
"""),

    ("Compare the average power disconnect scores for the top 5 transporters by risk",
    """WITH top_transporters AS (
    SELECT transporter_code
    FROM transporter_risk_score
    ORDER BY risk_score DESC
    LIMIT 5
)
SELECT
    'transporter_risk_score' AS "Data Table",
    transporter_code,
    risk_score,
    power_disconnect
FROM transporter_risk_score
WHERE transporter_code IN (SELECT transporter_code FROM top_transporters);
"""),

    # ===== ADVANCED MULTI-TABLE JOIN EXAMPLES =====

    ("For blacklisted vehicles, what were their last open alerts before being blacklisted?",
    """WITH ranked_alerts AS (
    SELECT
        a.vehicle_number,
        a.alert_category,
        a.severity,
        a.created_at,
        vtm.last_changed_date AS blacklisted_date,
        ROW_NUMBER() OVER(PARTITION BY a.vehicle_number ORDER BY a.created_at DESC) as rn
    FROM alerts a
    JOIN vts_truck_master vtm ON a.vehicle_number = vtm.truck_no
    WHERE vtm.whether_truck_blacklisted = 'Y'
      AND a.alert_status = 'OPEN'
      AND a.created_at < vtm.last_changed_date
)
SELECT
    'blacklisted_vehicle_alerts' AS "Data Table",
    vehicle_number,
    alert_category,
    severity,
    created_at
FROM ranked_alerts
WHERE rn = 1;
"""),

    ("Show me the drivers of vehicles that had a swipe out failure on an emlock trip",
    """SELECT DISTINCT
    'driver_swipe_failure' AS "Data Table",
    vot.driver_name,
    tam.trucknumber,
    tam.invoicenumber
FROM vts_tripauditmaster tam
JOIN vts_ongoing_trips vot ON tam.invoicenumber = vot.invoice_no
WHERE tam.isemlocktrip = 'Y'
  AND (tam.swipeoutl1 = 'False' OR tam.swipeoutl2 = 'False')
  AND vot.driver_name IS NOT NULL;
"""),

    ("What is the average vehicle risk score for each vehicle ownership type?",
    """SELECT
    'risk_by_ownership' AS "Data Table",
    vtm.ownership_type,
    AVG(trs.risk_score)::NUMERIC(10, 2) as average_risk_score,
    COUNT(vtm.truck_no) as vehicle_count
FROM vts_truck_master vtm
JOIN tt_risk_score trs ON vtm.truck_no = trs.tt_number
WHERE vtm.ownership_type IS NOT NULL
GROUP BY vtm.ownership_type
ORDER BY average_risk_score DESC;
"""),

    # ===== DATA AUDIT & QUALITY QUERIES =====

    ("Are there any vehicles in the risk table that are not in the master truck table?",
    """SELECT
    'data_audit_orphan_risk' AS "Data Table",
    trs.tt_number
FROM tt_risk_score trs
LEFT JOIN vts_truck_master vtm ON trs.tt_number = vtm.truck_no
WHERE vtm.truck_no IS NULL;
"""),

    ("Find trips where the actual start time is before the scheduled start time",
    """SELECT
    'data_audit_early_start' AS "Data Table",
    tt_number,
    trip_id,
    scheduled_start_datetime,
    actual_trip_start_datetime
FROM vts_ongoing_trips
WHERE actual_trip_start_datetime < scheduled_start_datetime;
"""),

    # =============================================================================================
    # ===== NEW: COMPLEX ANALYTICAL QUERIES (FOR 100% ACCURACY GOAL) =====
    # =============================================================================================

    # ===== Anomaly & Contradiction Detection =====
    ("List vehicles that have a risk_score in tt_risk_score but their transporter has zero recorded trips in transporter_risk_score",
    """SELECT
    'tt_risk_score' AS "Data Table",
    trs.tt_number,
    trs.risk_score AS vehicle_risk_score,
    trs.transporter_code,
    trs2.total_trips AS transporter_total_trips
FROM
    tt_risk_score trs
JOIN
    transporter_risk_score trs2 ON trs.transporter_code = trs2.transporter_code
WHERE
    trs2.total_trips = 0;
"""),

    ("Find vehicles with a high risk score (>80) that have not had any violations recorded in the last 30 days.",
    """SELECT
    'high_risk_no_violations' AS "Data Table",
    trs.tt_number,
    trs.risk_score
FROM tt_risk_score trs
LEFT JOIN vts_alert_history vah ON trs.tt_number = vah.tl_number
    AND vah.vts_end_datetime >= CURRENT_DATE - INTERVAL '30 days'
WHERE trs.risk_score > 80 AND vah.id IS NULL;
"""),

    # ===== Performance & Efficiency Metrics =====
    ("What is the average time it takes to close a 'HIGH' severity alert?",
    """SELECT
    'alerts' AS "Data Table",
    AVG(closed_at - created_at) as average_closure_time
FROM alerts
WHERE alert_status = 'Close'
  AND severity = 'HIGH'
  AND alert_section = 'VTS'
  AND closed_at IS NOT NULL AND created_at IS NOT NULL;
"""),

    ("What is the average delay between a trip's scheduled start and actual start for trips in the last month?",
    """SELECT
    'vts_ongoing_trips' AS "Data Table",
    AVG(actual_trip_start_datetime - scheduled_start_datetime) AS "Average Start Delay"
FROM vts_ongoing_trips
WHERE
    actual_trip_start_datetime IS NOT NULL
    AND scheduled_start_datetime IS NOT NULL
    AND actual_trip_start_datetime >= CURRENT_DATE - INTERVAL '1 month';
"""),

    # ===== Multi-Table Logic & Joins =====
    ("Show me vehicles with swipe out failures that also had a high severity alert on the same day.",
    """SELECT
    'cross_table_failure_alert' AS "Data Table",
    t1.trucknumber,
    t1.event_date
FROM (
    SELECT trucknumber, createdat::date AS event_date
    FROM vts_tripauditmaster
    WHERE swipeoutl1 = 'False' OR swipeoutl2 = 'False'
) AS t1
JOIN (
    SELECT vehicle_number, created_at::date AS event_date
    FROM alerts
    WHERE severity = 'HIGH' AND alert_section = 'VTS'
) AS t2 ON t1.trucknumber = t2.vehicle_number AND t1.event_date = t2.event_date
GROUP BY t1.trucknumber, t1.event_date
ORDER BY t1.event_date DESC;
"""),

    ("Which trips, currently ongoing, started more than 2 hours late?",
    """SELECT
    'vts_ongoing_trips' AS "Data Table",
    tt_number,
    trip_id,
    (actual_trip_start_datetime - scheduled_start_datetime) AS delay
FROM vts_ongoing_trips
WHERE (actual_trip_start_datetime - scheduled_start_datetime) > INTERVAL '2 hours'
ORDER BY delay DESC;
"""),

    # ===== Data Quality & Auditing =====
    ("How many trips have a recorded `actual_trip_start_datetime` but are missing an `event_end_datetime`?",
    """SELECT
    'vts_ongoing_trips' AS "Data Table",
    COUNT(*) as missing_end_time_count
FROM vts_ongoing_trips
WHERE actual_trip_start_datetime IS NOT NULL AND event_end_datetime IS NULL;
"""),

    ("List all emlock trips that also had a swipe-in failure.",
    """SELECT
    'vts_tripauditmaster' AS "Data Table",
    trucknumber,
    invoicenumber,
    createdat
FROM vts_tripauditmaster
WHERE isemlocktrip = 'Y'
  AND (swipeinl1 = 'False' OR swipeinl2 = 'False')
ORDER BY createdat DESC;
"""),

("Find alerts where the raw_data contains a 'battery_voltage' below 3.5",
    """SELECT
    'alerts' AS "Data Table",
    vehicle_number,
    alert_category,
    raw_data ->> 'battery_voltage' AS battery_voltage,
    created_at
FROM alerts
WHERE alert_section = 'VTS'
  AND (raw_data ->> 'battery_voltage')::numeric < 3.5
ORDER BY created_at DESC
LIMIT 100;
"""),

    ("Count alerts by the 'event_code' found in the raw_data json",
    """SELECT
    'alerts' AS "Data Table",
    raw_data ->> 'event_code' AS event_code,
    COUNT(*) as alert_count
FROM alerts
WHERE alert_section = 'VTS'
  AND raw_data ? 'event_code' -- Check if the key exists
GROUP BY raw_data ->> 'event_code'
ORDER BY alert_count DESC;
"""),

    # ===== Advanced Window Functions (RANK, DENSE_RANK) =====
    ("For each zone, rank the top 3 vehicles by their total number of violations",
    """WITH vehicle_violations AS (
    SELECT
        zone,
        tl_number,
        SUM(stoppage_violations_count + route_deviation_count + speed_violation_count + night_driving_count + device_offline_count + device_tamper_count + continuous_driving_count + no_halt_zone_count + main_supply_removal_count) as total_violations
    FROM vts_alert_history
    WHERE zone IS NOT NULL
    GROUP BY zone, tl_number
),
ranked_vehicles AS (
    SELECT
        zone,
        tl_number,
        total_violations,
        RANK() OVER (PARTITION BY zone ORDER BY total_violations DESC) as rank_in_zone
    FROM vehicle_violations
)
SELECT
    'ranked_violators_by_zone' AS "Data Table",
    zone,
    tl_number,
    total_violations,
    rank_in_zone
FROM ranked_vehicles
WHERE rank_in_zone <= 3;
"""),

    ("Show the dense rank of transporters based on their average vehicle risk score",
    """WITH transporter_avg_risk AS (
    SELECT
        vtm.transporter_name,
        AVG(trs.risk_score) as avg_risk
    FROM tt_risk_score trs
    JOIN vts_truck_master vtm ON trs.tt_number = vtm.truck_no
    WHERE vtm.transporter_name IS NOT NULL
    GROUP BY vtm.transporter_name
)
SELECT
    'transporter_risk_rank' AS "Data Table",
    transporter_name,
    avg_risk::NUMERIC(10, 2) as average_risk,
    DENSE_RANK() OVER (ORDER BY avg_risk DESC) as risk_rank
FROM transporter_avg_risk;
"""),

    # ===== Complex Multi-Table Joins (4+ tables) =====
    ("For open 'HIGH' severity alerts, show the driver name, vehicle risk score, and whether it was an emlock trip",
    """SELECT
    'complex_alert_context' AS "Data Table",
    a.vehicle_number,
    vot.driver_name,
    trs.risk_score,
    tam.isemlocktrip,
    a.alert_category,
    a.created_at
FROM alerts a
LEFT JOIN vts_ongoing_trips vot ON a.vehicle_number = vot.tt_number AND vot.actual_trip_start_datetime IS NOT NULL AND vot.event_end_datetime IS NULL -- Join on active trips
LEFT JOIN tt_risk_score trs ON a.vehicle_number = trs.tt_number
LEFT JOIN vts_tripauditmaster tam ON a.invoice_number = tam.invoicenumber
WHERE a.alert_section = 'VTS'
  AND a.alert_status = 'OPEN'
  AND a.severity = 'HIGH'
ORDER BY a.created_at DESC;
"""),

    # ===== Queries with Complex CASE Statements =====
    ("Categorize all vehicles based on their risk score and blacklisted status",
    """SELECT
    'vehicle_categorization' AS "Data Table",
    vtm.truck_no,
    trs.risk_score,
    CASE
        WHEN vtm.whether_truck_blacklisted = 'Y' THEN 'blacklisted'
        WHEN trs.risk_score > 80 THEN 'Very High Risk'
        WHEN trs.risk_score > 60 THEN 'High Risk'
        WHEN trs.risk_score > 40 THEN 'Medium Risk'
        ELSE 'Low Risk'
    END as risk_category
FROM vts_truck_master vtm
LEFT JOIN tt_risk_score trs ON vtm.truck_no = trs.tt_number;
"""),

    # ===== Queries with HAVING clause on multiple aggregations =====
    ("Find transporters who have more than 10 vehicles and an average vehicle risk score below 50",
    """SELECT
    'efficient_transporters' AS "Data Table",
    vtm.transporter_name,
    COUNT(DISTINCT vtm.truck_no) as vehicle_count,
    AVG(trs.risk_score)::NUMERIC(10, 2) as average_risk
FROM vts_truck_master vtm
JOIN tt_risk_score trs ON vtm.truck_no = trs.tt_number
WHERE vtm.transporter_name IS NOT NULL
GROUP BY vtm.transporter_name
HAVING COUNT(DISTINCT vtm.truck_no) > 10 AND AVG(trs.risk_score) < 50
ORDER BY average_risk ASC;
"""),

    # ===== More Array Operations (`@>`) =====
    ("Find trips that had both a 'stoppage violation' and a 'speed violation'",
    """SELECT
    'vts_alert_history' AS "Data Table",
    tl_number,
    invoice_number,
    vts_end_datetime,
    array_to_string(violation_type, ', ') as violations
FROM vts_alert_history
WHERE violation_type @> ARRAY['stoppage violation', 'speed violation']
ORDER BY vts_end_datetime DESC;
"""),

    # ===== Self-Join Pattern (less common but powerful) =====
    ("Find pairs of alerts for the same vehicle that occurred within 1 hour of each other",
    """SELECT
    'alert_pairs' AS "Data Table",
    a1.vehicle_number,
    a1.alert_category as first_alert,
    a1.created_at as first_alert_time,
    a2.alert_category as second_alert,
    a2.created_at as second_alert_time
FROM alerts a1
JOIN alerts a2 ON a1.vehicle_number = a2.vehicle_number
WHERE a1.id < a2.id -- Avoid duplicate pairs and self-joins
  AND a1.alert_section = 'VTS' AND a2.alert_section = 'VTS'
  AND a2.created_at BETWEEN a1.created_at AND a1.created_at + INTERVAL '1 hour'
ORDER BY a1.vehicle_number, a1.created_at;
"""),
("How many alerts were marked as false, and what was their average time to closure?",
    """SELECT
    'alerts' AS "Data Table",
    COUNT(*) as marked_as_false_count,
    AVG(closed_at - created_at) as avg_closure_time
FROM alerts
WHERE mark_as_false = true
  AND alert_section = 'VTS'
  AND alert_status = 'Close';
"""),

    ("List alerts that have been escalated to R2 or R3 but are still open",
    """SELECT
    'alerts' AS "Data Table",
    vehicle_number,
    alert_category,
    severity,
    created_at,
    r2_time,
    r3_time
FROM alerts
WHERE alert_status = 'OPEN'
  AND alert_section = 'VTS'
  AND (r2_time IS NOT NULL OR r3_time IS NOT NULL)
ORDER BY created_at ASC;
"""),

    ("What are the most common RCA types for 'CRITICAL' severity alerts?",
    """SELECT
    'alerts' AS "Data Table",
    rca_type,
    COUNT(*) as count
FROM alerts
WHERE severity = 'CRITICAL'
  AND alert_section = 'VTS'
  AND rca_type IS NOT NULL
GROUP BY rca_type
ORDER BY count DESC;
"""),

   ("what are the common columns in tt_risk_score and transporter_risk_score",
    """SELECT 
    c1.column_name,
    c1.data_type
FROM information_schema.columns c1
JOIN information_schema.columns c2 
    ON c1.column_name = c2.column_name
   AND c1.data_type = c2.data_type
WHERE 
    c1.table_name = 'tt_risk_score'
    AND c2.table_name = 'transporter_risk_score'
ORDER BY c1.column_name;"""),


    # ===== `vts_truck_master` for Context/Filtering ONLY =====
    ("Show me the total violations for all vehicles with 'Market' ownership type in the last 30 days",
    """SELECT
    'violations_by_ownership' AS "Data Table",
    vtm.ownership_type,
    SUM(vah.stoppage_violations_count + vah.route_deviation_count + vah.speed_violation_count + vah.night_driving_count + vah.device_offline_count + vah.device_tamper_count + vah.continuous_driving_count + vah.no_halt_zone_count + vah.main_supply_removal_count) as total_violations
FROM vts_alert_history vah
JOIN vts_truck_master vtm ON vah.tl_number = vtm.truck_no
WHERE vtm.ownership_type = 'Market'
  AND vah.vts_end_datetime >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY vtm.ownership_type;
"""),

    ("What is the average risk score for vehicles of type '20 KL' vs '12 KL'?",
    """SELECT
    'risk_by_vehicle_type' AS "Data Table",
    vtm.vehicle_type_desc,
    AVG(trs.risk_score)::NUMERIC(10, 2) as average_risk_score
FROM tt_risk_score trs
JOIN vts_truck_master vtm ON trs.tt_number = vtm.truck_no
WHERE vtm.vehicle_type_desc IN ('20 KL', '12 KL')
GROUP BY vtm.vehicle_type_desc;
"""),

    # ===== Advanced Geospatial & Location Logic =====
    ("Find vehicles that had a speed violation within a specific geographical box (e.g., around Mumbai)",
    """SELECT
    'geospatial_violations' AS "Data Table",
    vot.tt_number,
    vot.vehicle_location,
    vot.vehicle_latitude,
    vot.vehicle_longitude
FROM vts_ongoing_trips vot
JOIN vts_alert_history vah ON vot.invoice_no = vah.invoice_number
WHERE
    vah.speed_violation_count > 0
    AND vot.vehicle_latitude BETWEEN 18.9 AND 19.2 -- Example latitude for Mumbai
    AND vot.vehicle_longitude BETWEEN 72.8 AND 73.0; -- Example longitude for Mumbai
"""),

    # ===== Deeper `vts_tripauditmaster` Analysis =====
    ("Which Business Unit (BU) has the highest rate of swipe-in failures for emlock trips?",
    """SELECT
    'swipe_failure_rate_by_bu' AS "Data Table",
    bu,
    COUNT(*) as emlock_trips,
    COUNT(*) FILTER (WHERE swipeinl1 = 'False' OR swipeinl2 = 'False') as failed_swipe_ins,
    ROUND(100.0 * COUNT(*) FILTER (WHERE swipeinl1 = 'False' OR swipeinl2 = 'False') / COUNT(*), 2) as failure_rate_percent
FROM vts_tripauditmaster
WHERE isemlocktrip = 'Y' AND bu IS NOT NULL
GROUP BY bu
ORDER BY failure_rate_percent DESC;
"""),

    # ===== Complex Conditional Aggregation (FILTER) =====
    ("For each transporter, show the count of 'device tamper' vs 'power disconnect' violations",
    """SELECT
    'transporter_violation_breakdown' AS "Data Table",
    vtm.transporter_name,
    COUNT(vah.id) FILTER (WHERE vah.device_tamper_count > 0) as tamper_events,
    COUNT(vah.id) FILTER (WHERE vah.main_supply_removal_count > 0) as power_disconnect_events
FROM vts_alert_history vah
JOIN vts_truck_master vtm ON vah.tl_number = vtm.truck_no
WHERE vtm.transporter_name IS NOT NULL
GROUP BY vtm.transporter_name
ORDER BY (tamper_events + power_disconnect_events) DESC;
"""),

    # ===== Advanced Data Quality & Auditing =====
    ("List trips that have an event_end_datetime but are missing an actual_trip_start_datetime",
    """SELECT
    'data_quality_missing_start' AS "Data Table",
    tt_number,
    trip_id,
    event_end_datetime
FROM vts_ongoing_trips
WHERE actual_trip_start_datetime IS NULL AND event_end_datetime IS NOT NULL;
"""),

    ("Are there any alerts assigned to roles that do not exist in the system (e.g., not 'Admin', 'Manager', 'User')?",
    """SELECT
    'data_quality_invalid_role' AS "Data Table",
    assigned_to_role,
    COUNT(*) as alert_count
FROM alerts
WHERE assigned_to_role IS NOT NULL
  AND alert_section = 'VTS'
  AND assigned_to_role NOT IN ('Admin', 'Manager', 'User', 'Transporter') -- Assuming these are the valid roles
GROUP BY assigned_to_role;
"""),

    # ===== Correlating Risk Components with Actuals =====
    ("Find transporters where their `power_disconnect` risk component is high (>0.5) but they have zero actual `main_supply_removal_count` violations in the last 90 days.",
    """SELECT
    'risk_component_mismatch' AS "Data Table",
    trs.transporter_name,
    trs.power_disconnect as risk_component_score
FROM transporter_risk_score trs
WHERE trs.power_disconnect > 0.5
  AND NOT EXISTS (
    SELECT 1
    FROM vts_alert_history vah
    JOIN vts_truck_master vtm ON vah.tl_number = vtm.truck_no
    WHERE vtm.transporter_code = trs.transporter_code
      AND vah.main_supply_removal_count > 0
      AND vah.vts_end_datetime >= CURRENT_DATE - INTERVAL '90 days'
  );
"""),
("Which drivers are consistently late starting their trips? Show me the top 5 with the worst average delay.",
    """SELECT
    'vts_ongoing_trips' AS "Data Table",
    driver_name,
    COUNT(*) as trip_count,
    AVG(actual_trip_start_datetime - scheduled_start_datetime) as average_start_delay
FROM vts_ongoing_trips
WHERE driver_name IS NOT NULL
  AND actual_trip_start_datetime > scheduled_start_datetime
GROUP BY driver_name
HAVING COUNT(*) > 5 -- Only consider drivers with more than 5 trips
ORDER BY average_start_delay DESC
LIMIT 5;
"""),


    # ===== Deeper Anomaly & Contradiction Detection =====
    ("Show me transporters who have a high number of 'device offline' alerts but a low `power_disconnect` risk score.",
    """WITH offline_alerts AS (
    SELECT
        transporter_code,
        COUNT(*) as offline_alert_count
    FROM alerts
    WHERE alert_category = 'Device Offline'
      AND alert_section = 'VTS'
    GROUP BY transporter_code
)
SELECT
    'anomaly_alerts_vs_risk' AS "Data Table",
    trs.transporter_name,
    trs.power_disconnect as power_disconnect_risk,
    oa.offline_alert_count
FROM transporter_risk_score trs
JOIN offline_alerts oa ON trs.transporter_code = oa.transporter_code
WHERE trs.power_disconnect < 0.2 -- Low risk score
  AND oa.offline_alert_count > 10 -- High number of actual alerts
ORDER BY oa.offline_alert_count DESC;
"""),

    ("Which vehicles have a high stoppage violation count but a low stoppage violation risk component in their score?",
    """SELECT
    'anomaly_violations_vs_risk' AS "Data Table",
    trs.tt_number,
    trs.stoppage_violation as stoppage_risk_component,
    SUM(vah.stoppage_violations_count) as actual_stoppage_violations
FROM tt_risk_score trs
JOIN vts_alert_history vah ON trs.tt_number = vah.tl_number
WHERE vah.vts_end_datetime >= CURRENT_DATE - INTERVAL '60 days'
GROUP BY trs.tt_number, trs.stoppage_violation
HAVING trs.stoppage_violation < 0.3 AND SUM(vah.stoppage_violations_count) > 15
ORDER BY actual_stoppage_violations DESC;
"""),

    # ===== Data Quality & Auditing =====
    ("List alerts that were marked as false but have no Root Cause Analysis (RCA) text.",
    """SELECT
    'data_quality_missing_rca' AS "Data Table",
    vehicle_number,
    alert_category,
    severity,
    created_at,
    closed_at
FROM alerts
WHERE mark_as_false = true
  AND (rca IS NULL OR rca = '')
  AND alert_section = 'VTS'
ORDER BY created_at DESC;
"""),

    ("Are there any trips in the audit master that are marked as emlock trips but have no associated lock IDs?",
    """SELECT
    'data_quality_missing_locks' AS "Data Table",
    trucknumber,
    invoicenumber,
    createdat
FROM vts_tripauditmaster
WHERE isemlocktrip = 'Y'
  AND (lockid1 IS NULL OR lockid1 = '')
  AND (lockid2 IS NOT NULL AND lockid2 != ''); -- Assuming at least one lock ID should be present
"""),

    # ===== Complex Cohort Analysis =====
    ("Compare the top 3 violation types for 'OWNER' vs 'Market' ownership vehicles.",
    """WITH ranked_violations AS (
    SELECT vtm.ownership_type, vt.violation_type_element, COUNT(*) as count,
           ROW_NUMBER() OVER(PARTITION BY vtm.ownership_type ORDER BY COUNT(*) DESC) as rn
    FROM vts_alert_history vah
    JOIN vts_truck_master vtm ON vah.tl_number = vtm.truck_no
    , LATERAL UNNEST(vah.violation_type) AS vt(violation_type_element)
    WHERE vtm.ownership_type IN ('OWNER', 'Market')
    GROUP BY vtm.ownership_type, vt.violation_type_element
)
SELECT 'violation_by_ownership_cohort' AS "Data Table", ownership_type, violation_type_element, count
FROM ranked_violations
WHERE rn <= 3;
"""),

("Which drivers are consistently late starting their trips? Show me the top 5 with the worst average delay.",
    """SELECT
    'vts_ongoing_trips' AS "Data Table",
    driver_name,
    COUNT(*) as trip_count,
    AVG(actual_trip_start_datetime - scheduled_start_datetime) as average_start_delay
FROM vts_ongoing_trips
WHERE driver_name IS NOT NULL
  AND actual_trip_start_datetime > scheduled_start_datetime
GROUP BY driver_name
HAVING COUNT(*) > 5 -- Only consider drivers with more than 5 trips
ORDER BY average_start_delay DESC
LIMIT 5;
"""),

    ("What is our alert closure rate? How many alerts were opened versus closed last week?",
    """SELECT
    'alerts' AS "Data Table",
    COUNT(*) FILTER (WHERE created_at >= CURRENT_DATE - INTERVAL '7 days') as new_alerts_last_week,
    COUNT(*) FILTER (WHERE closed_at >= CURRENT_DATE - INTERVAL '7 days') as closed_alerts_last_week,
    ROUND(
        100.0 * COUNT(*) FILTER (WHERE closed_at >= CURRENT_DATE - INTERVAL '7 days') /
        NULLIF(COUNT(*) FILTER (WHERE created_at >= CURRENT_DATE - INTERVAL '7 days'), 0), 2
    ) as closure_rate_percent
FROM alerts
WHERE alert_section = 'VTS';
"""),

    # ===== Deeper Anomaly & Contradiction Detection =====
    ("Show me transporters who have a high number of 'device offline' alerts but a low `power_disconnect` risk score.",
    """WITH offline_alerts AS (
    SELECT
        transporter_code,
        COUNT(*) as offline_alert_count
    FROM alerts
    WHERE alert_category = 'Device Offline'
      AND alert_section = 'VTS'
      AND created_at >= CURRENT_DATE - INTERVAL '30 days'
    GROUP BY transporter_code
)
SELECT
    'anomaly_alerts_vs_risk' AS "Data Table",
    trs.transporter_name,
    trs.power_disconnect as power_disconnect_risk,
    oa.offline_alert_count
FROM transporter_risk_score trs
JOIN offline_alerts oa ON trs.transporter_code = oa.transporter_code
WHERE trs.power_disconnect < 0.2 -- Low risk score
  AND oa.offline_alert_count > 10 -- High number of actual alerts
ORDER BY oa.offline_alert_count DESC;
"""),

    ("Which vehicles have a high stoppage violation count but a low stoppage violation risk component in their score?",
    """SELECT
    'anomaly_violations_vs_risk' AS "Data Table",
    trs.tt_number,
    trs.stoppage_violation as stoppage_risk_component,
    SUM(vah.stoppage_violations_count) as actual_stoppage_violations
FROM tt_risk_score trs
JOIN vts_alert_history vah ON trs.tt_number = vah.tl_number
GROUP BY trs.tt_number, trs.stoppage_violation
HAVING trs.stoppage_violation < 0.3 AND SUM(vah.stoppage_violations_count) > 15
ORDER BY actual_stoppage_violations DESC;
"""),

    # ===== Data Quality & Auditing =====
    ("List alerts that were marked as false but have no Root Cause Analysis (RCA) text.",
    """SELECT
    'data_quality_missing_rca' AS "Data Table",
    vehicle_number,
    alert_category,
    severity,
    created_at,
    closed_at
FROM alerts
WHERE mark_as_false = true
  AND (rca IS NULL OR rca = '')
  AND alert_section = 'VTS'
ORDER BY created_at DESC;
"""),

    ("Are there any trips in the audit master that are marked as emlock trips but have no associated lock IDs?",
    """SELECT
    'data_quality_missing_locks' AS "Data Table",
    trucknumber,
    invoicenumber,
    createdat
FROM vts_tripauditmaster
WHERE isemlocktrip = 'Y'
  AND (lockid1 IS NULL OR lockid1 = '')
  AND (lockid2 IS NULL OR lockid2 = '');
"""),

    # ===== Complex Cohort Analysis =====
    ("Compare the top 3 violation types for 'OWNER' vs 'Market' ownership vehicles.",
    """WITH ranked_violations AS (
    SELECT vtm.ownership_type, vt.violation_type_element, COUNT(*) as count,
           ROW_NUMBER() OVER(PARTITION BY vtm.ownership_type ORDER BY COUNT(*) DESC) as rn
    FROM vts_alert_history vah
    JOIN vts_truck_master vtm ON vah.tl_number = vtm.truck_no
    , LATERAL UNNEST(vah.violation_type) AS vt(violation_type_element)
    WHERE vtm.ownership_type IN ('OWNER', 'Market')
    GROUP BY vtm.ownership_type, vt.violation_type_element
)
SELECT 'violation_by_ownership_cohort' AS "Data Table", ownership_type, violation_type_element, count
FROM ranked_violations
WHERE rn <= 3;
"""),

    # =============================================================================================
    # ===== NEW: CLIENT-FOCUSED ANALYTICAL QUERIES (ROUND 5) =====
    # =============================================================================================

    # ===== Proactive Risk & Performance Benchmarking =====
    ("Which drivers have the highest rate of 'continuous driving' violations per trip?",
    """WITH driver_stats AS (
    SELECT
        vot.driver_name, -- Driver name is only available in ongoing trips
        COUNT(DISTINCT vot.trip_id) as total_trips,
        SUM(vah.continuous_driving_count) as total_continuous_driving_violations
    FROM vts_ongoing_trips vot
    JOIN vts_alert_history vah ON vot.invoice_no = vah.invoice_number
    WHERE vot.driver_name IS NOT NULL AND vah.continuous_driving_count > 0
    GROUP BY vot.driver_name -- Grouping by driver name from ongoing trips
)
SELECT
    'driver_performance' AS "Data Table",
    driver_name,
    total_trips,
    total_continuous_driving_violations,
    (total_continuous_driving_violations * 1.0 / total_trips)::NUMERIC(10, 2) as violations_per_trip
FROM driver_stats
WHERE total_trips > 5 -- Only consider drivers with a significant number of trips
ORDER BY violations_per_trip DESC
LIMIT 100;
"""),

    # ===== NEW: PRIORITY - Complex Utilization & Risk Analysis (Active Trips) =====
    ("Which transporters are over-utilizing their high-risk vehicles? Show transporters where more than 60% of their active trips involve vehicles with a risk score over 80.",
    """
WITH ActiveTrips AS (
    -- Step 1: Get all currently active trips and join with risk scores
    SELECT
        vot.transporter_name,
        vot.tt_number,
        COALESCE(trs.risk_score, 0) as risk_score
    FROM vts_ongoing_trips vot
    LEFT JOIN tt_risk_score trs ON vot.tt_number = trs.tt_number
    WHERE vot.actual_trip_start_datetime IS NOT NULL 
      AND vot.event_end_datetime IS NULL -- Ensure trip is currently active
),
TransporterStats AS (
    -- Step 2: Aggregate stats per transporter
    SELECT
        transporter_name,
        COUNT(*) as total_active_trips,
        COUNT(CASE WHEN risk_score > 80 THEN 1 END) as high_risk_trips
    FROM ActiveTrips
    WHERE transporter_name IS NOT NULL
    GROUP BY transporter_name
)
-- Step 3: Calculate percentage and filter
SELECT
    'transporter_utilization' AS "Data Table",
    transporter_name,
    total_active_trips,
    high_risk_trips,
    ROUND(100.0 * high_risk_trips / NULLIF(total_active_trips, 0), 2) as high_risk_percentage
FROM TransporterStats
WHERE (100.0 * high_risk_trips / NULLIF(total_active_trips, 0)) > 60
ORDER BY high_risk_percentage DESC;
"""),

    # ===== NEW: PRIORITY - Trip Pending Closure & Exclusion Logic =====
    ("Are there any vehicles that are not in `vts_ongoing_trips` but have an open 'Trip pending closure (TC)' violation?",
    """
SELECT
    'anomaly_tc_not_ongoing' AS "Data Table",
    vah.tl_number,
    vah.violation_type
FROM vts_alert_history vah
LEFT JOIN vts_ongoing_trips vot ON vah.tl_number = vot.tt_number
WHERE
    -- Check for TC violation in history (assuming it flows there) or check alerts
    'TC' = ANY(vah.violation_type)
    -- Ensure the vehicle is NOT currently in the ongoing trips table
    AND vot.tt_number IS NULL
    AND vah.vts_end_datetime >= CURRENT_DATE - INTERVAL '7 days';
"""),

    ("Compare the average risk score of vehicles currently on a trip vs those not on a trip.",
    """
SELECT
    'risk_comparison_active_vs_idle' AS "Data Table",
    CASE 
        WHEN vot.tt_number IS NOT NULL THEN 'Currently on Trip' 
        ELSE 'Idle / Not on Trip' 
    END as vehicle_status,
    COUNT(DISTINCT trs.tt_number) as vehicle_count,
    AVG(trs.risk_score)::NUMERIC(10, 2) as average_risk_score
FROM tt_risk_score trs
LEFT JOIN vts_ongoing_trips vot ON trs.tt_number = vot.tt_number AND vot.event_end_datetime IS NULL
GROUP BY 1
ORDER BY average_risk_score DESC;
"""),

    ("List vehicles that have been blacklisted but are still showing up in ongoing trips.",
    """
SELECT
    'compliance_anomaly' AS "Data Table",
    vot.tt_number,
    vot.transporter_name,
    vot.vehicle_location
FROM vts_ongoing_trips vot
JOIN vts_truck_master vtm ON vot.tt_number = vtm.truck_no
WHERE vtm.whether_truck_blacklisted = 'Y';
"""),

    ("Identify 'silent performers' - vehicles with low risk scores but a high number of recent, low-severity alerts.",
    """WITH recent_low_severity_alerts AS (
    SELECT
        vehicle_number,
        COUNT(*) as low_sev_alert_count
    FROM alerts
    WHERE severity = 'LOW'
      AND alert_section = 'VTS'
      AND created_at >= CURRENT_DATE - INTERVAL '30 days'
    GROUP BY vehicle_number
)
SELECT
    'silent_performers_anomaly' AS "Data Table",
    trs.tt_number,
    trs.risk_score,
    rlsa.low_sev_alert_count
FROM tt_risk_score trs
JOIN recent_low_severity_alerts rlsa ON trs.tt_number = rlsa.vehicle_number
WHERE trs.risk_score < 40 -- Low risk score
  AND rlsa.low_sev_alert_count > 20 -- High number of low-severity alerts
ORDER BY rlsa.low_sev_alert_count DESC;
"""),

    # ===== Operational Accountability & Efficiency =====
    ("Are we getting better at closing high-severity alerts? Compare this month to last month.",
    """WITH this_month AS (
    SELECT
        'This Month' as period,
        AVG(closed_at - created_at) as avg_closure_time
    FROM alerts
    WHERE severity = 'HIGH'
      AND alert_section = 'VTS'
      AND closed_at >= DATE_TRUNC('month', CURRENT_DATE)
),
last_month AS (
    SELECT
        'Last Month' as period,
        AVG(closed_at - created_at) as avg_closure_time
    FROM alerts
    WHERE severity = 'HIGH'
      AND alert_section = 'VTS'
      AND closed_at >= DATE_TRUNC('month', CURRENT_DATE - INTERVAL '1 month')
      AND closed_at < DATE_TRUNC('month', CURRENT_DATE)
)
SELECT 'alert_closure_trend' AS "Data Table", period, avg_closure_time FROM this_month
UNION ALL
SELECT 'alert_closure_trend' AS "Data Table", period, avg_closure_time FROM last_month;
"""),

    ("Which zones are the worst for night driving violations?",
    """SELECT
    'vts_alert_history' AS "Data Table",
    zone,
    SUM(night_driving_count) as total_night_driving_violations
FROM vts_alert_history
WHERE zone IS NOT NULL AND night_driving_count > 0
GROUP BY zone
ORDER BY total_night_driving_violations DESC
LIMIT 1000;
"""),

    # ===== Deeper Data Quality & Auditing =====
    ("Are there any vehicles that have been blocked for more than 10 days?",
    """SELECT
    'alerts' AS "Data Table",
    vehicle_number,
    transporter_name,
    vehicle_blocked_start_date,
    (CURRENT_DATE - vehicle_blocked_start_date::date) as days_blocked
FROM alerts
WHERE vehicle_blocked_start_date IS NOT NULL
  AND vehicle_unblocked_date IS NULL
  AND (CURRENT_DATE - vehicle_blocked_start_date::date) > 10
ORDER BY days_blocked DESC;
"""),

("Are there any vehicles that have been blocked for more than 20 days",
     """SELECT
    'alerts' AS "Data Table",
    vehicle_number,
    transporter_name,
    vehicle_blocked_start_date,
    (CURRENT_DATE - vehicle_blocked_start_date::date) as days_blocked
FROM alerts
WHERE vehicle_blocked_start_date IS NOT NULL
  AND vehicle_unblocked_date IS NULL
  AND (CURRENT_DATE - vehicle_blocked_start_date::date) > 20
ORDER BY days_blocked DESC"""),

    # Edge case: Moving average
    ("What's the moving average of alerts over the last 30 days",
     """WITH daily_counts AS (
    SELECT
        created_at::date AS date,
        COUNT(*) AS daily_count
    FROM alerts
    WHERE alert_section = 'VTS'
      AND created_at >= CURRENT_DATE - INTERVAL '30 days'
    GROUP BY created_at::date
)
SELECT
    'moving_average_alerts' AS "Data Table",
    date,
    daily_count,
    AVG(daily_count) OVER (ORDER BY date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)::NUMERIC(10, 2) AS "7_day_moving_average"
FROM daily_counts
ORDER BY date DESC"""),

    # Edge case: Missing location data
    ("which records have missing location information",
     """SELECT 'vts_ongoing_trips' AS "Data Table", 'zone' AS "Missing Column", COUNT(*) AS missing_count 
FROM vts_ongoing_trips WHERE zone IS NULL
UNION ALL
SELECT 'vts_ongoing_trips' AS "Data Table", 'region' AS "Missing Column", COUNT(*) 
FROM vts_ongoing_trips WHERE region IS NULL
UNION ALL
SELECT 'vts_alert_history' AS "Data Table", 'zone' AS "Missing Column", COUNT(*) 
FROM vts_alert_history WHERE zone IS NULL
UNION ALL
SELECT 'vts_alert_history' AS "Data Table", 'region' AS "Missing Column", COUNT(*) 
FROM vts_alert_history WHERE region IS NULL"""),

 # ===== NEW: PRIORITY - Complex Trend Analysis (Fix for failure in logs) =====
    ("Find transporters whose average vehicle risk score is decreasing, but their total number of high-severity alerts is increasing month-over-month.",
    """
WITH monthly_risk AS (
    -- Step 1: Calculate the average vehicle risk score for each transporter per month
    SELECT
        vtm.transporter_code,
        vtm.transporter_name,
        -- NOTE: tt_risk_score has no timestamp. We use the alert history date as a proxy for the event month.
        DATE_TRUNC('month', vah.vts_end_datetime) AS month,
        AVG(trs.risk_score) as avg_risk_score
    FROM tt_risk_score trs
    JOIN vts_truck_master vtm ON trs.tt_number = vtm.truck_no
    -- Join to alerts to get a relevant timestamp for the risk score
    JOIN vts_alert_history vah ON vtm.truck_no = vah.tl_number 
    WHERE vtm.transporter_code IS NOT NULL AND vah.vts_end_datetime IS NOT NULL
    GROUP BY vtm.transporter_code, vtm.transporter_name, DATE_TRUNC('month', vah.vts_end_datetime)
),
monthly_alerts AS (
    -- Step 2: Count high-severity alerts for each transporter per month
    SELECT
        vtm.transporter_code,
        DATE_TRUNC('month', a.created_at) AS month,
        COUNT(*) as high_severity_alerts
    FROM alerts a
    JOIN vts_truck_master vtm ON a.vehicle_number = vtm.truck_no
    WHERE a.severity = 'HIGH' AND a.alert_section = 'VTS' AND vtm.transporter_code IS NOT NULL
    GROUP BY vtm.transporter_code, DATE_TRUNC('month', a.created_at)
),
trends AS (
    -- Step 3: Use LAG() to compare this month vs. last month for both metrics
    SELECT
        mr.transporter_name,
        mr.month,
        mr.avg_risk_score,
        LAG(mr.avg_risk_score, 1, mr.avg_risk_score) OVER (PARTITION BY mr.transporter_code ORDER BY mr.month) as prev_month_risk,
        ma.high_severity_alerts,
        LAG(ma.high_severity_alerts, 1, ma.high_severity_alerts) OVER (PARTITION BY mr.transporter_code ORDER BY mr.month) as prev_month_alerts
    FROM monthly_risk mr
    JOIN monthly_alerts ma ON mr.transporter_code = ma.transporter_code AND mr.month = ma.month
)
-- Step 4: Filter for the specified contradictory trend
SELECT transporter_name, month, avg_risk_score, prev_month_risk, high_severity_alerts, prev_month_alerts
FROM trends
WHERE avg_risk_score < prev_month_risk AND high_severity_alerts > prev_month_alerts;
"""),

    # ===== NEW: PRIORITY - Fix for "Good Risk but High Violations" =====
    ("Which transporters have a good risk score (< 40) but a high number of total violations (> 50) in the last quarter?",
    """
WITH transporter_violations AS (
    -- First, calculate total violations per transporter in the last quarter
    SELECT
        vtm.transporter_code,
        SUM(vah.stoppage_violations_count + vah.route_deviation_count + vah.speed_violation_count + vah.night_driving_count + vah.device_offline_count + vah.device_tamper_count + vah.continuous_driving_count + vah.no_halt_zone_count + vah.main_supply_removal_count) as total_violations
    FROM vts_alert_history vah
    JOIN vts_truck_master vtm ON vah.tl_number = vtm.truck_no
    WHERE vah.vts_end_datetime >= CURRENT_DATE - INTERVAL '3 months'
    GROUP BY vtm.transporter_code
)
-- Then, join with transporter risk scores and apply the conditions
SELECT
    'anomaly_risk_vs_violations' AS "Data Table",
    trs.transporter_name,
    trs.risk_score,
    tv.total_violations
FROM transporter_risk_score trs
JOIN transporter_violations tv ON trs.transporter_code = tv.transporter_code
WHERE trs.risk_score < 40 AND tv.total_violations > 50
ORDER BY tv.total_violations DESC;
"""),

    # ===== NEW: PRIORITY - Fix for "Good Risk but High Violations" =====
    ("Top 5 Vehicles with highest total violations last month?",
    """SELECT
    'vts_alert_history' AS "Data Table",
    tl_number AS vehicle_number,
    SUM(
        stoppage_violations_count +
        route_deviation_count +
        speed_violation_count +
        night_driving_count +
        device_offline_count +
        device_tamper_count +
        continuous_driving_count +
        no_halt_zone_count +
        main_supply_removal_count
    ) AS total_violations
FROM vts_alert_history
WHERE vts_end_datetime >= DATE_TRUNC('month', CURRENT_DATE - INTERVAL '1 month')
  AND vts_end_datetime < DATE_TRUNC('month', CURRENT_DATE)
GROUP BY tl_number
ORDER BY total_violations DESC
LIMIT 5;
"""),

    ("what are the top 2 violations and vehicles in last month",
    """SELECT
    'vts_alert_history' AS "Data Table",
    tl_number AS vehicle_number,
    SUM(
        stoppage_violations_count +
        route_deviation_count +
        speed_violation_count +
        night_driving_count +
        device_offline_count +
        device_tamper_count +
        continuous_driving_count +
        no_halt_zone_count +
        main_supply_removal_count
    ) AS total_violations
FROM vts_alert_history
WHERE vts_end_datetime >= DATE_TRUNC('month', CURRENT_DATE - INTERVAL '1 month')
  AND vts_end_datetime < DATE_TRUNC('month', CURRENT_DATE)
GROUP BY tl_number
ORDER BY total_violations DESC
LIMIT 2;
"""),

    # ===== NEW: Top and Bottom Ranking (UNION) =====
    ("what are the top 2 risk score vehicles and their transporter and also top 2 lower risk score vehicle and its transporter",
    """
(SELECT
    'Top Risk Vehicles' AS "Category",
    trs.tt_number,
    trs.risk_score,
    vtm.transporter_name
FROM tt_risk_score trs
JOIN vts_truck_master vtm ON trs.tt_number = vtm.truck_no
WHERE trs.risk_score IS NOT NULL AND vtm.transporter_name IS NOT NULL
ORDER BY trs.risk_score DESC
LIMIT 2)
UNION ALL
(SELECT
    'Lowest Risk Vehicles' AS "Category",
    trs.tt_number,
    trs.risk_score,
    vtm.transporter_name
FROM tt_risk_score trs
JOIN vts_truck_master vtm ON trs.tt_number = vtm.truck_no
WHERE trs.risk_score IS NOT NULL AND vtm.transporter_name IS NOT NULL
ORDER BY trs.risk_score ASC
LIMIT 2)
"""),

    # ===== NEW: FIX FOR IMPLICIT DATE FILTERS =====
    ("which transporters have less than 20 violations and a risk score less than 40",
    """WITH transporter_violations AS (
    SELECT
        vtm.transporter_code,
        SUM(vah.stoppage_violations_count + vah.route_deviation_count + vah.speed_violation_count + vah.night_driving_count + vah.device_offline_count + vah.device_tamper_count + vah.continuous_driving_count + vah.no_halt_zone_count + vah.main_supply_removal_count) as total_violations
    FROM vts_alert_history vah
    JOIN vts_truck_master vtm ON vah.tl_number = vtm.truck_no
    -- No date filter applied as none was requested
    GROUP BY vtm.transporter_code
)
SELECT
    'anomaly_risk_vs_violations' AS "Data Table",
    trs.transporter_name,
    trs.risk_score,
    tv.total_violations
FROM transporter_risk_score trs
JOIN transporter_violations tv ON trs.transporter_code = tv.transporter_code
WHERE tv.total_violations < 20 AND trs.risk_score < 40
ORDER BY trs.risk_score ASC, tv.total_violations ASC;
"""),

    # ===== NEW: GOLD STANDARD - Anomaly: Blocked but still on a trip =====
    ("Which vehicles were blocked due to alerts but continued to appear in ongoing trips",
    """
SELECT
    'anomaly_blocked_but_active' AS "Data Table",
    a.vehicle_number,
    a.alert_category,
    a.vehicle_blocked_start_date,
    vot.trip_id,
    vot.event_start_datetime
FROM alerts a
JOIN vts_ongoing_trips vot ON a.vehicle_number = vot.tt_number
WHERE
    a.alert_status = 'Open'
    AND a.vehicle_unblocked_date IS NULL
    AND vot.event_start_datetime > a.vehicle_blocked_start_date
ORDER BY a.vehicle_blocked_start_date DESC;
"""),

    # ===== NEW: GOLD STANDARD - Last Alert Before Block (Fixes undefined column) =====
    ("Are there any vehicles that have been blocked for more than 30 days, and what was the last alert they had before being blocked?",
    """WITH last_alert_before_block AS (
    -- Step 1: For each vehicle, find the last alert that occurred BEFORE it was blocked.
    SELECT
        a.vehicle_number,
        vah.vts_end_datetime AS last_alert_time,
        array_to_string(vah.violation_type, ', ') AS last_violation_type,
        -- Use ROW_NUMBER to rank alerts in descending order of time before the block
        ROW_NUMBER() OVER(PARTITION BY a.vehicle_number ORDER BY vah.vts_end_datetime DESC) as rn
    FROM alerts a
    -- Join with alert history to find past violations
    JOIN vts_alert_history vah ON a.vehicle_number = vah.tl_number
    WHERE
        -- Condition 1: The vehicle is currently blocked
        a.vehicle_blocked_start_date IS NOT NULL
        AND a.vehicle_unblocked_date IS NULL
        -- Condition 2: The historical alert occurred BEFORE the block started
        AND vah.vts_end_datetime < a.vehicle_blocked_start_date
)
-- Step 2: Select the currently blocked vehicles and join with their last alert information.
SELECT
    'alerts' AS "Data Table",
    a.vehicle_number,
    (CURRENT_DATE - a.vehicle_blocked_start_date::date) as days_blocked,
    labb.last_alert_time,
    labb.last_violation_type
FROM alerts a
LEFT JOIN last_alert_before_block labb ON a.vehicle_number = labb.vehicle_number AND labb.rn = 1 -- Join only the most recent alert (rn=1)
WHERE a.vehicle_blocked_start_date IS NOT NULL
  AND a.vehicle_unblocked_date IS NULL
  AND a.alert_status = 'Open' AND a.alert_section = 'VTS'
  AND (CURRENT_DATE - a.vehicle_blocked_start_date::date) > 30"""),

    # ===== NEW: Ranking within Groups =====
    ("Rank vehicles by risk score within each zone",
    """SELECT
    'tt_risk_score' AS "Data Table",
    vtm.zone,
    trs.tt_number,
    trs.risk_score,
    RANK() OVER (PARTITION BY vtm.zone ORDER BY trs.risk_score DESC) as risk_rank
FROM tt_risk_score trs
JOIN vts_truck_master vtm ON trs.tt_number = vtm.truck_no
WHERE vtm.zone IS NOT NULL AND trs.risk_score IS NOT NULL
ORDER BY vtm.zone, risk_rank
LIMIT 1000;"""),

    # ===== NEW: Existence Check =====
    ("Check if vehicle KA13AA2040 exists in the master table",
    """SELECT 'vts_truck_master' AS "Data Table", CASE WHEN EXISTS (SELECT 1 FROM vts_truck_master WHERE truck_no ILIKE 'KA13AA2040') THEN 'Exists' ELSE 'Does Not Exist' END as status;"""),

    # ===== NEW: High Risk but No Recent Violations (Anomaly) =====
    ("List vehicles with high risk score (>80) but no violations in the last 30 days",
    """SELECT 'tt_risk_score' AS "Data Table", trs.tt_number, trs.risk_score
    FROM tt_risk_score trs
    LEFT JOIN vts_alert_history vah ON trs.tt_number = vah.tl_number AND vah.vts_end_datetime >= CURRENT_DATE - INTERVAL '30 days'
    WHERE trs.risk_score > 80 AND vah.id IS NULL
    ORDER BY trs.risk_score DESC"""),

    # ===== NEW: Trip Duration Analysis =====
    ("What is the average trip duration for each transporter?",
    """SELECT 'vts_ongoing_trips' AS "Data Table", transporter_name, AVG(event_end_datetime - actual_trip_start_datetime) as avg_duration
    FROM vts_ongoing_trips
    WHERE actual_trip_start_datetime IS NOT NULL AND event_end_datetime IS NOT NULL
    GROUP BY transporter_name
    ORDER BY avg_duration DESC"""),

    # ===== NEW: Zone-specific Violation Search =====
    ("Which vehicles have 'Speeding' violations in 'Mumbai' zone?",
    """SELECT 'vts_alert_history' AS "Data Table", tl_number, speed_violation_count, zone
    FROM vts_alert_history
    WHERE zone ILIKE '%Mumbai%' AND speed_violation_count > 0
    ORDER BY speed_violation_count DESC"""),

    # ===== NEW: Anomaly - High Alerts but Low Risk (No Time Filter) =====
    ("show transporters with high device offline alerts but low power disconnect risk",
    """WITH offline_alerts AS (
    SELECT
        transporter_code,
        COUNT(*) as offline_alert_count
    FROM alerts
    WHERE alert_category = 'Device Offline'
      AND alert_section = 'VTS'
    GROUP BY transporter_code
)
SELECT
    trs.transporter_name,
    trs.power_disconnect as power_disconnect_risk,
    oa.offline_alert_count
FROM transporter_risk_score trs
JOIN offline_alerts oa ON trs.transporter_code = oa.transporter_code
WHERE trs.power_disconnect < 0.2 
  AND oa.offline_alert_count > 10 
ORDER BY oa.offline_alert_count DESC"""),
    # ===== NEW: ROBUSTNESS - ALERTS TABLE (Sensor/Device Alerts) =====
    ("Show me all critical device alerts for the Southern region",
    """SELECT
    'alerts' AS "Data Table",
    a.device_name,
    a.alert_message,
    a.severity,
    a.r1_time as alert_time
FROM alerts a
WHERE a.region = 'South' 
  AND a.severity = 'CRITICAL'
ORDER BY a.r1_time DESC
LIMIT 20;"""),

    # ===== NEW: ROBUSTNESS - TRIP AUDIT (Emlock/Swipe) =====
    ("List trip audit records for vehicle KA01X1234 involving lock ID 998877",
    """SELECT
    'vts_tripauditmaster' AS "Data Table",
    trucknumber,
    lockid1,
    lockid2,
    createdat
FROM vts_tripauditmaster
WHERE trucknumber ILIKE 'KA01X1234'
  AND (lockid1 = '998877' OR lockid2 = '998877')
ORDER BY createdat DESC;"""),

    # ===== NEW: ROBUSTNESS - TRUCK MASTER SPECS =====
    ("Which vehicles have a capacity greater than 20 KL?",
    """SELECT
    'vts_truck_master' AS "Data Table",
    truck_no,
    capacity_of_the_truck,
    vehicle_type_desc
FROM vts_truck_master
WHERE capacity_of_the_truck > 20
ORDER BY capacity_of_the_truck DESC;"""),
    # False alert analysis - CRITICAL: List individual vehicles, not just transporter aggregates
    (
        "List vehicles that were flagged for device tampering and later marked as false alerts — how often does this happen per transporter?",
        """SELECT 
    'vts_flagged_false_alerts' AS "Data Table",
    vah.tl_number AS vehicle_number,
    vtm.transporter_name,
    COUNT(vah.id) as tamper_false_alert_count
FROM vts_alert_history vah
JOIN vts_truck_master vtm ON vah.tl_number = vtm.truck_no
WHERE vah.auto_unblock = TRUE
  AND 'device_tamper' = ANY(vah.violation_type)
GROUP BY vah.tl_number, vtm.transporter_name
HAVING COUNT(vah.id) > 0
ORDER BY vtm.transporter_name, tamper_false_alert_count DESC"""
    ),
    # False alert + high risk - Demonstrates proper threshold definition and non-redundant filtering
    (
        "Identify transporters where alerts are frequently closed as 'false', but risk scores remain high",
        """SELECT 
    'transporter_false_alerts_high_risk' AS "Data Table",
    vtm.transporter_name,
    trs.risk_score,
    COUNT(a.id) as false_alert_count
FROM alerts a
JOIN vts_truck_master vtm ON a.vehicle_number = vtm.truck_no
JOIN transporter_risk_score trs ON vtm.transporter_code = trs.transporter_code
WHERE 
    a.alert_section = 'VTS'
    AND a.mark_as_false = TRUE
    AND a.closed_at >= CURRENT_DATE - INTERVAL '30 days'
    AND trs.risk_score > 50
GROUP BY 
    vtm.transporter_name, trs.risk_score
HAVING 
    COUNT(a.id) > 5
ORDER BY 
    trs.risk_score DESC, false_alert_count DESC"""
    ),

    (
"Get all open alerts with vehicle, transporter, region, zone, and location details.",
"""
SELECT a.*, tm.transporter_name, tm.transporter_code, tm.region, tm.zone, tm.location_name
FROM alerts a
JOIN vts_truck_master tm
  ON a.vehicle_number = tm.truck_no
WHERE a.alert_status = 'Open';
"""
),

(
"Find alerts linked to route deviation violations in alert history.",
"""
SELECT a.*, vh.route_deviation_count
FROM alerts a
JOIN vts_alert_history vh
  ON a.unique_id = vh.alert_id
WHERE vh.route_deviation_count > 0;
"""
),

(
"List alerts with high severity and transporter fleet risk score above average.",
"""
SELECT a.*, tr.risk_score
FROM alerts a
JOIN transporter_risk_score tr
  ON a.transporter_code = tr.transporter_code
WHERE a.severity = 'High'
  AND tr.risk_score > (SELECT AVG(risk_score) FROM transporter_risk_score);
"""
),

(
"Identify alerts associated with blacklisted trucks.",
"""
SELECT a.*, tm.whether_truck_blacklisted
FROM alerts a
JOIN vts_truck_master tm
  ON a.vehicle_number = tm.truck_no
WHERE tm.whether_truck_blacklisted = 'Y';
"""
),

(
"Find alerts during ongoing trips exceeding scheduled duration.",
"""
SELECT a.*, ot.trip_id, ot.scheduled_end_datetime, ot.actual_end_start_datetime
FROM alerts a
JOIN vts_ongoing_trips ot
  ON a.vehicle_number = ot.tt_number
WHERE ot.actual_end_start_datetime > ot.scheduled_end_datetime;
"""
),

(
"Retrieve alerts with live GPS coordinates from ongoing trips.",
"""
SELECT a.*, ot.vehicle_latitude, ot.vehicle_longitude, ot.vehicle_location
FROM alerts a
JOIN vts_ongoing_trips ot
  ON a.vehicle_number = ot.tt_number;
"""
),

(
"Show alerts linked to swipe mismatches in trip audits.",
"""
SELECT a.*, ta.swipeinl1, ta.swipeoutl1
FROM alerts a
JOIN vts_tripauditmaster ta
  ON a.vehicle_number = ta.trucknumber
WHERE ta.swipeinl1 <> ta.swipeoutl1;
"""
),

(
"List alerts caused by night driving violations.",
"""
SELECT a.*, vh.night_driving_count
FROM alerts a
JOIN vts_alert_history vh
  ON a.unique_id = vh.alert_id
WHERE vh.night_driving_count > 0;
"""
),

(
"Get alerts with vehicle-level risk score above 80.",
"""
SELECT a.*, trs.risk_score
FROM alerts a
JOIN tt_risk_score trs
  ON a.vehicle_number = trs.tt_number
WHERE trs.risk_score > 80;
"""
),

(
"Count alerts by region and zone.",
"""
SELECT tm.region, tm.zone, COUNT(a.id) AS alert_count
FROM alerts a
JOIN vts_truck_master tm
  ON a.vehicle_number = tm.truck_no
GROUP BY tm.region, tm.zone;
"""
),

(
"Find false-positive alerts by transporter.",
"""
SELECT tm.transporter_name, COUNT(a.id)
FROM alerts a
JOIN vts_truck_master tm
  ON a.vehicle_number = tm.truck_no
WHERE a.is_flagged_false = TRUE
GROUP BY tm.transporter_name;
"""
),

(
"Identify alerts overlapping dry-out periods and active trips.",
"""
SELECT a.*, ot.trip_id
FROM alerts a
JOIN vts_ongoing_trips ot
  ON a.vehicle_number = ot.tt_number
WHERE ot.actual_trip_start_datetime
BETWEEN a.dry_out_start_time AND a.dry_out_end_time;
"""
),

(
"Show alerts with delayed ATG acknowledgment.",
"""
SELECT a.*
FROM alerts a
WHERE a.atg_ack = TRUE
  AND a.atg_ack_time > a.created_at + INTERVAL '1 hour';
"""
),

(
"List alerts with continuous driving violations.",
"""
SELECT a.*, vh.continuous_driving_count
FROM alerts a
JOIN vts_alert_history vh
  ON a.unique_id = vh.alert_id
WHERE vh.continuous_driving_count > 0;
"""
),

(
"Aggregate alerts by product code and region.",
"""
SELECT a.product_code, tm.region, COUNT(a.id)
FROM alerts a
JOIN vts_truck_master tm
  ON a.vehicle_number = tm.truck_no
GROUP BY a.product_code, tm.region;
"""
),

(
"Find alerts involving device tampering.",
"""
SELECT a.*, vh.device_tamper_count
FROM alerts a
JOIN vts_alert_history vh
  ON a.unique_id = vh.alert_id
WHERE vh.device_tamper_count > 0;
"""
),

(
"List alerts where vehicle was blocked at creation time.",
"""
SELECT *
FROM alerts
WHERE created_at BETWEEN vehicle_blocked_start_date AND vehicle_blocked_end_date;
"""
),

(
"Show alerts grouped by terminal and servicing plant.",
"""
SELECT terminal_plant_name, servicing_plant_name, COUNT(id)
FROM alerts
GROUP BY terminal_plant_name, servicing_plant_name;
"""
),

(
"Find alerts linked to trips exceeding scheduled end time.",
"""
SELECT a.*, vh.vts_end_datetime, vh.scheduled_trip_end_datetime
FROM alerts a
JOIN vts_alert_history vh
  ON a.unique_id = vh.alert_id
WHERE vh.vts_end_datetime > vh.scheduled_trip_end_datetime;
"""
),

(
"Identify alerts related to power disconnection events.",
"""
SELECT a.*, vh.main_supply_removal_count
FROM alerts a
JOIN vts_alert_history vh
  ON a.unique_id = vh.alert_id
WHERE vh.main_supply_removal_count > 0;
"""
),

(
"Retrieve alerts where GPS device went offline.",
"""
SELECT a.*, vh.device_offline_count
FROM alerts a
JOIN vts_alert_history vh
  ON a.unique_id = vh.alert_id
WHERE vh.device_offline_count > 0;
"""
),

(
"List alerts with unauthorized stoppage violations.",
"""
SELECT a.*, vh.stoppage_violations_count
FROM alerts a
JOIN vts_alert_history vh
  ON a.unique_id = vh.alert_id
WHERE vh.stoppage_violations_count > 0;
"""
),

(
"Show alerts linked to high-risk transporters.",
"""
SELECT a.*, tr.risk_score
FROM alerts a
JOIN transporter_risk_score tr
  ON a.transporter_code = tr.transporter_code
WHERE tr.risk_score > 75;
"""
),

(
"Find alerts where audit load number mismatched alert load.",
"""
SELECT a.*, ta.loadnumber
FROM alerts a
JOIN vts_tripauditmaster ta
  ON a.vehicle_number = ta.trucknumber
WHERE a.tt_load_number <> ta.loadnumber;
"""
),

(
"Get alerts assigned to roles with active workflow instances.",
"""
SELECT a.*
FROM alerts a
WHERE a.workflow_instance_id IS NOT NULL;
"""
),
(
"Retrieve alert history records with associated alert severity and status.",
"""
SELECT
  vh.*, a.severity, a.alert_status
FROM vts_alert_history vh
JOIN alerts a
  ON vh.alert_id = a.unique_id;
"""
),

(
"Find trips with route deviation violations and vehicle details.",
"""
SELECT
  vh.route_deviation_count, vh.tl_number,
  tm.truck_no, tm.transporter_name
FROM vts_alert_history vh
JOIN vts_truck_master tm
  ON vh.tl_number = tm.truck_no
WHERE vh.route_deviation_count > 0;
"""
),

(
"List alert history entries related to night driving incidents.",
"""
SELECT
  vh.*, a.alert_message
FROM vts_alert_history vh
JOIN alerts a
  ON vh.alert_id = a.unique_id
WHERE vh.night_driving_count > 0;
"""
),

(
"Count alert history violations by region and zone.",
"""
SELECT
  vh.region, vh.zone,
  SUM(vh.stoppage_violations_count) AS stoppage_count
FROM vts_alert_history vh
GROUP BY vh.region, vh.zone;
"""
),

(
"Identify trips where GPS device went offline and alerts were generated.",
"""
SELECT
  vh.device_offline_count, a.alert_message
FROM vts_alert_history vh
JOIN alerts a
  ON vh.alert_id = a.unique_id
WHERE vh.device_offline_count > 0;
"""
),

(
"Find trips with continuous driving violations linked to alerts.",
"""
SELECT
  vh.continuous_driving_count, a.id
FROM vts_alert_history vh
JOIN alerts a
  ON vh.alert_id = a.unique_id
WHERE vh.continuous_driving_count > 0;
"""
),

(
"Show alert history with approval and auto-unblock status.",
"""
SELECT
  vh.approved_by, vh.auto_unblock, a.alert_status
FROM vts_alert_history vh
JOIN alerts a
  ON vh.alert_id = a.unique_id;
"""
),

(
"Retrieve trips where scheduled and actual VTS times mismatch.",
"""
SELECT
  vh.scheduled_trip_start_datetime,
  vh.scheduled_trip_end_datetime,
  vh.vts_start_datetime,
  vh.vts_end_datetime
FROM vts_alert_history vh
WHERE vh.vts_end_datetime > vh.scheduled_trip_end_datetime;
"""
),

(
"Aggregate speed violations by transporter.",
"""
SELECT
  tm.transporter_name,
  SUM(vh.speed_violation_count) AS total_speed_violations
FROM vts_alert_history vh
JOIN vts_truck_master tm
  ON vh.tl_number = tm.truck_no
GROUP BY tm.transporter_name;
"""
),

(
"Find trips with main supply removal violations and alerts.",
"""
SELECT
  vh.main_supply_removal_count, a.alert_category
FROM vts_alert_history vh
JOIN alerts a
  ON vh.alert_id = a.unique_id
WHERE vh.main_supply_removal_count > 0;
"""
),

(
"List alert history entries grouped by business unit.",
"""
SELECT
  vh.bu, COUNT(vh.id) AS history_count
FROM vts_alert_history vh
GROUP BY vh.bu;
"""
),

(
"Identify trips with no-halt-zone violations.",
"""
SELECT
  vh.no_halt_zone_count, a.alert_message
FROM vts_alert_history vh
JOIN alerts a
  ON vh.alert_id = a.unique_id
WHERE vh.no_halt_zone_count > 0;
"""
),

(
"Find alert history records tied to specific SAP locations.",
"""
SELECT
  vh.sap_id, vh.location_name, COUNT(vh.id)
FROM vts_alert_history vh
GROUP BY vh.sap_id, vh.location_name;
"""
),

(
"Compare original vs corrected route deviation counts.",
"""
SELECT
  vh.route_deviation_count_orig,
  vh.route_deviation_count,
  a.id
FROM vts_alert_history vh
JOIN alerts a
  ON vh.alert_id = a.unique_id
WHERE vh.route_deviation_count <> vh.route_deviation_count_orig;
"""
),

(
"Retrieve alert history with base and destination locations.",
"""
SELECT
  vh.base_location_name,
  vh.destination_code,
  a.alert_status
FROM vts_alert_history vh
JOIN alerts a
  ON vh.alert_id = a.unique_id;
"""
),

(
"List alert history entries with invoice numbers and alerts.",
"""
SELECT
  vh.invoice_number, a.tt_load_number
FROM vts_alert_history vh
JOIN alerts a
  ON vh.alert_id = a.unique_id;
"""
),

(
"Identify trips with high stoppage violations.",
"""
SELECT
  vh.stoppage_violations_count, tm.truck_no
FROM vts_alert_history vh
JOIN vts_truck_master tm
  ON vh.tl_number = tm.truck_no
WHERE vh.stoppage_violations_count > 5;
"""
),

(
"Show alert history created within the same day as alerts.",
"""
SELECT
  vh.created_at, a.created_at
FROM vts_alert_history vh
JOIN alerts a
  ON vh.alert_id = a.unique_id
WHERE DATE(vh.created_at) = DATE(a.created_at);
"""
),

(
"Find trips with device tampering events.",
"""
SELECT
  vh.device_tamper_count, a.alert_message
FROM vts_alert_history vh
JOIN alerts a
  ON vh.alert_id = a.unique_id
WHERE vh.device_tamper_count > 0;
"""
),

(
"Aggregate total trips by region.",
"""
SELECT
  vh.region, SUM(vh.total_trips) AS total_trips
FROM vts_alert_history vh
GROUP BY vh.region;
"""
),

(
"List alert history with TT type and vehicle ownership.",
"""
SELECT
  vh.tt_type, tm.ownership_type
FROM vts_alert_history vh
JOIN vts_truck_master tm
  ON vh.tl_number = tm.truck_no;
"""
),

(
"Identify alert history records linked to blacklisted vehicles.",
"""
SELECT
  vh.*, tm.whether_truck_blacklisted
FROM vts_alert_history vh
JOIN vts_truck_master tm
  ON vh.tl_number = tm.truck_no
WHERE tm.whether_truck_blacklisted = 'Y';
"""
),

(
"Find alert history entries where trips started late.",
"""
SELECT
  vh.scheduled_trip_start_datetime,
  vh.vts_start_datetime
FROM vts_alert_history vh
WHERE vh.vts_start_datetime > vh.scheduled_trip_start_datetime;
"""
),

(
"List alert history entries grouped by location type.",
"""
SELECT
  vh.location_type, COUNT(vh.id)
FROM vts_alert_history vh
GROUP BY vh.location_type;
"""
),

(
"Retrieve alert history records with associated alert workflow state.",
"""
SELECT
  vh.id, a.alert_state
FROM vts_alert_history vh
JOIN alerts a
  ON vh.alert_id = a.unique_id;
"""
),

(
"Retrieve trip audits with alert severity, transporter, and region details.",
"""
SELECT
  ta.*, a.severity, tm.transporter_name, tm.region
FROM vts_tripauditmaster ta
JOIN alerts a
  ON ta.trucknumber = a.vehicle_number
JOIN vts_truck_master tm
  ON ta.trucknumber = tm.truck_no;
"""
),

(
"Find trip audits where swipe mismatches led to active alerts.",
"""
SELECT
  ta.trucknumber, ta.swipeinl1, ta.swipeoutl1, a.alert_status
FROM vts_tripauditmaster ta
JOIN alerts a
  ON ta.trucknumber = a.vehicle_number
JOIN vts_alert_history vh
  ON a.unique_id = vh.alert_id
WHERE ta.swipeinl1 <> ta.swipeoutl1;
"""
),

(
"List trip audits associated with route deviation violations.",
"""
SELECT
  ta.trucknumber, vh.route_deviation_count, a.alert_category
FROM vts_tripauditmaster ta
JOIN alerts a
  ON ta.trucknumber = a.vehicle_number
JOIN vts_alert_history vh
  ON a.unique_id = vh.alert_id
WHERE vh.route_deviation_count > 0;
"""
),

(
"Identify trip audits for blacklisted vehicles with alerts.",
"""
SELECT
  ta.trucknumber, tm.whether_truck_blacklisted, a.alert_message
FROM vts_tripauditmaster ta
JOIN vts_truck_master tm
  ON ta.trucknumber = tm.truck_no
JOIN alerts a
  ON ta.trucknumber = a.vehicle_number
WHERE tm.whether_truck_blacklisted = 'Y';
"""
),

(
"Retrieve trip audits where vehicle risk score is high.",
"""
SELECT
  ta.trucknumber, trs.risk_score, a.severity
FROM vts_tripauditmaster ta
JOIN tt_risk_score trs
  ON ta.trucknumber = trs.tt_number
JOIN alerts a
  ON ta.trucknumber = a.vehicle_number
WHERE trs.risk_score > 80;
"""
),

(
"Find trip audits linked to transporters with high fleet risk.",
"""
SELECT
  ta.trucknumber, tr.transporter_name, tr.risk_score
FROM vts_tripauditmaster ta
JOIN alerts a
  ON ta.trucknumber = a.vehicle_number
JOIN transporter_risk_score tr
  ON a.transporter_code = tr.transporter_code
WHERE tr.risk_score > 75;
"""
),

(
"List trip audits during which continuous driving violations occurred.",
"""
SELECT
  ta.trucknumber, vh.continuous_driving_count, a.alert_status
FROM vts_tripauditmaster ta
JOIN alerts a
  ON ta.trucknumber = a.vehicle_number
JOIN vts_alert_history vh
  ON a.unique_id = vh.alert_id
WHERE vh.continuous_driving_count > 0;
"""
),

(
"Show trip audits where night driving violations triggered alerts.",
"""
SELECT
  ta.trucknumber, vh.night_driving_count, a.alert_message
FROM vts_tripauditmaster ta
JOIN alerts a
  ON ta.trucknumber = a.vehicle_number
JOIN vts_alert_history vh
  ON a.unique_id = vh.alert_id
WHERE vh.night_driving_count > 0;
"""
),

(
"Identify trip audits with GPS device offline incidents.",
"""
SELECT
  ta.trucknumber, vh.device_offline_count, tm.zone
FROM vts_tripauditmaster ta
JOIN vts_truck_master tm
  ON ta.trucknumber = tm.truck_no
JOIN vts_alert_history vh
  ON ta.trucknumber = vh.tl_number
WHERE vh.device_offline_count > 0;
"""
),

(
"Retrieve trip audits where alerts were marked false positive.",
"""
SELECT
  ta.trucknumber, a.is_flagged_false, tm.transporter_name
FROM vts_tripauditmaster ta
JOIN alerts a
  ON ta.trucknumber = a.vehicle_number
JOIN vts_truck_master tm
  ON ta.trucknumber = tm.truck_no
WHERE a.is_flagged_false = TRUE;
"""
),

(
"Find trip audits overlapping dry-out periods.",
"""
SELECT
  ta.trucknumber, a.dry_out_start_time, a.dry_out_end_time
FROM vts_tripauditmaster ta
JOIN alerts a
  ON ta.trucknumber = a.vehicle_number
JOIN vts_ongoing_trips ot
  ON ta.trucknumber = ot.tt_number
WHERE ot.actual_trip_start_datetime
BETWEEN a.dry_out_start_time AND a.dry_out_end_time;
"""
),

(
"List trip audits where audit invoice mismatched alert load.",
"""
SELECT
  ta.trucknumber, ta.invoicenumber, a.tt_load_number
FROM vts_tripauditmaster ta
JOIN alerts a
  ON ta.trucknumber = a.vehicle_number
JOIN vts_alert_history vh
  ON a.unique_id = vh.alert_id
WHERE ta.loadnumber <> a.tt_load_number;
"""
),

(
"Retrieve trip audits for vehicles blocked during alert creation.",
"""
SELECT
  ta.trucknumber, a.vehicle_blocked_start_date, a.vehicle_blocked_end_date
FROM vts_tripauditmaster ta
JOIN alerts a
  ON ta.trucknumber = a.vehicle_number
JOIN vts_truck_master tm
  ON ta.trucknumber = tm.truck_no
WHERE a.created_at BETWEEN a.vehicle_blocked_start_date AND a.vehicle_blocked_end_date;
""",
),

(
"Find trip audits associated with power disconnection events.",
"""
SELECT
  ta.trucknumber, vh.main_supply_removal_count, a.alert_category
FROM vts_tripauditmaster ta
JOIN alerts a
  ON ta.trucknumber = a.vehicle_number
JOIN vts_alert_history vh
  ON a.unique_id = vh.alert_id
WHERE vh.main_supply_removal_count > 0;
"""
),

(
"Show trip audits grouped by region and alert severity.",
"""
SELECT
  tm.region, a.severity, COUNT(ta.id)
FROM vts_tripauditmaster ta
JOIN vts_truck_master tm
  ON ta.trucknumber = tm.truck_no
JOIN alerts a
  ON ta.trucknumber = a.vehicle_number
GROUP BY tm.region, a.severity;
"""
),

(
"Identify trip audits where trips exceeded scheduled end time.",
"""
SELECT
  ta.trucknumber, vh.scheduled_trip_end_datetime, vh.vts_end_datetime
FROM vts_tripauditmaster ta
JOIN alerts a
  ON ta.trucknumber = a.vehicle_number
JOIN vts_alert_history vh
  ON a.unique_id = vh.alert_id
WHERE vh.vts_end_datetime > vh.scheduled_trip_end_datetime;
"""
),

(
"Retrieve trip audits with vehicle capacity and alert count.",
"""
SELECT
  tm.capacity_of_the_truck, COUNT(a.id) AS alert_count
FROM vts_tripauditmaster ta
JOIN vts_truck_master tm
  ON ta.trucknumber = tm.truck_no
JOIN alerts a
  ON ta.trucknumber = a.vehicle_number
GROUP BY tm.capacity_of_the_truck;
"""
),

(
"Find trip audits linked to no-halt zone violations.",
"""
SELECT
  ta.trucknumber, vh.no_halt_zone_count, tm.zone
FROM vts_tripauditmaster ta
JOIN vts_truck_master tm
  ON ta.trucknumber = tm.truck_no
JOIN vts_alert_history vh
  ON ta.trucknumber = vh.tl_number
WHERE vh.no_halt_zone_count > 0;
"""
),

(
"Show trip audits with alert workflow states and assignment roles.",
"""
SELECT
  ta.trucknumber, a.alert_state, a.assigned_to_role
FROM vts_tripauditmaster ta
JOIN alerts a
  ON ta.trucknumber = a.vehicle_number
JOIN vts_alert_history vh
  ON a.unique_id = vh.alert_id;
"""
),

(
"Identify trip audits with stoppage violations and transporter risk.",
"""
SELECT
  ta.trucknumber, vh.stoppage_violations_count, tr.risk_score
FROM vts_tripauditmaster ta
JOIN vts_alert_history vh
  ON ta.trucknumber = vh.tl_number
JOIN transporter_risk_score tr
  ON vh.vendor_id = tr.transporter_code
WHERE vh.stoppage_violations_count > 0;
"""
),

(
"Retrieve trip audits for ongoing trips with active alerts.",
"""
SELECT
  ta.trucknumber, ot.trip_id, a.alert_status
FROM vts_tripauditmaster ta
JOIN vts_ongoing_trips ot
  ON ta.trucknumber = ot.tt_number
JOIN alerts a
  ON ta.trucknumber = a.vehicle_number;
"""
),

(
"Find trip audits with base vs destination mismatches.",
"""
SELECT
  ta.trucknumber, vh.base_location_name, vh.destination_code
FROM vts_tripauditmaster ta
JOIN alerts a
  ON ta.trucknumber = a.vehicle_number
JOIN vts_alert_history vh
  ON a.unique_id = vh.alert_id
WHERE vh.base_location_name <> vh.destination_code;
"""
),

(
"Show trip audits grouped by business unit and alert category.",
"""
SELECT
  ta.bu, a.alert_category, COUNT(ta.id)
FROM vts_tripauditmaster ta
JOIN alerts a
  ON ta.trucknumber = a.vehicle_number
JOIN vts_truck_master tm
  ON ta.trucknumber = tm.truck_no
GROUP BY ta.bu, a.alert_category;
"""
),

(
"Retrieve trip audits with transporter ownership type and violations.",
"""
SELECT
  ta.trucknumber, tm.ownership_type, vh.speed_violation_count
FROM vts_tripauditmaster ta
JOIN vts_truck_master tm
  ON ta.trucknumber = tm.truck_no
JOIN vts_alert_history vh
  ON ta.trucknumber = vh.tl_number
WHERE vh.speed_violation_count > 0;
"""
),

(
"Find vehicles whose average risk score is high but alert resolution progress is low.",
"""
WITH vehicle_risk AS (
  SELECT tt_number, AVG(risk_score) AS avg_risk
  FROM tt_risk_score
  GROUP BY tt_number
),
vehicle_alert_progress AS (
  SELECT vehicle_number, AVG(progress_rate) AS avg_progress
  FROM alerts
  GROUP BY vehicle_number
)
SELECT
  tm.truck_no,
  vr.avg_risk,
  vap.avg_progress
FROM vts_truck_master tm
JOIN vehicle_risk vr
  ON tm.truck_no = vr.tt_number
JOIN vehicle_alert_progress vap
  ON tm.truck_no = vap.vehicle_number
WHERE vr.avg_risk > 70
  AND vap.avg_progress < 50;
"""
),

(
"Identify vehicles with increasing alerts but decreasing trip counts.",
"""
WITH alert_counts AS (
  SELECT vehicle_number, COUNT(*) AS alert_count
  FROM alerts
  GROUP BY vehicle_number
),
trip_counts AS (
  SELECT tl_number, SUM(total_trips) AS trip_count
  FROM vts_alert_history
  GROUP BY tl_number
)
SELECT
  tm.truck_no,
  ac.alert_count,
  tc.trip_count
FROM vts_truck_master tm
JOIN alert_counts ac
  ON tm.truck_no = ac.vehicle_number
JOIN trip_counts tc
  ON tm.truck_no = tc.tl_number
WHERE ac.alert_count > tc.trip_count;
"""
),

(
"Detect blacklisted vehicles that still have ongoing trips and active alerts.",
"""
SELECT
  tm.truck_no,
  ot.trip_id,
  a.alert_status
FROM vts_truck_master tm
JOIN vts_ongoing_trips ot
  ON tm.truck_no = ot.tt_number
JOIN alerts a
  ON tm.truck_no = a.vehicle_number
WHERE tm.whether_truck_blacklisted = 'Y'
  AND a.alert_status = 'Open';
"""
),

(
"Find vehicles where stoppage violations exceed total trips.",
"""
SELECT
  tm.truck_no,
  SUM(vh.stoppage_violations_count) AS stoppages,
  SUM(vh.total_trips) AS trips
FROM vts_truck_master tm
JOIN vts_alert_history vh
  ON tm.truck_no = vh.tl_number
JOIN alerts a
  ON tm.truck_no = a.vehicle_number
GROUP BY tm.truck_no
HAVING SUM(vh.stoppage_violations_count) > SUM(vh.total_trips);
"""
),

(
"Identify vehicles whose alert severity is high despite low risk score.",
"""
SELECT
  tm.truck_no,
  AVG(trs.risk_score) AS avg_risk,
  COUNT(a.id) FILTER (WHERE a.severity = 'High') AS high_alerts
FROM vts_truck_master tm
JOIN tt_risk_score trs
  ON tm.truck_no = trs.tt_number
JOIN alerts a
  ON tm.truck_no = a.vehicle_number
GROUP BY tm.truck_no
HAVING AVG(trs.risk_score) < 40
   AND COUNT(a.id) FILTER (WHERE a.severity = 'High') > 3;
"""
),

(
"Compare vehicle capacity with frequency of overload-related alerts.",
"""
SELECT
  tm.truck_no,
  tm.capacity_of_the_truck,
  COUNT(a.id) AS alert_count
FROM vts_truck_master tm
JOIN alerts a
  ON tm.truck_no = a.vehicle_number
JOIN vts_alert_history vh
  ON a.unique_id = vh.alert_id
GROUP BY tm.truck_no, tm.capacity_of_the_truck
HAVING COUNT(a.id) > 5;
"""
),

(
"Find vehicles where night driving violations correlate with delayed alert closure.",
"""
SELECT
  tm.truck_no,
  AVG(EXTRACT(EPOCH FROM (a.closed_at - a.created_at))/3600) AS avg_closure_hours,
  SUM(vh.night_driving_count) AS night_violations
FROM vts_truck_master tm
JOIN alerts a
  ON tm.truck_no = a.vehicle_number
JOIN vts_alert_history vh
  ON a.unique_id = vh.alert_id
GROUP BY tm.truck_no
HAVING SUM(vh.night_driving_count) > 0
   AND AVG(EXTRACT(EPOCH FROM (a.closed_at - a.created_at))/3600) > 6;
"""
),

(
"Detect vehicles with frequent alerts but no corresponding violations.",
"""
SELECT
  tm.truck_no,
  COUNT(a.id) AS alerts,
  SUM(
    vh.route_deviation_count +
    vh.speed_violation_count +
    vh.stoppage_violations_count
  ) AS violations
FROM vts_truck_master tm
JOIN alerts a
  ON tm.truck_no = a.vehicle_number
JOIN vts_alert_history vh
  ON a.unique_id = vh.alert_id
GROUP BY tm.truck_no
HAVING COUNT(a.id) > 5
   AND SUM(
     vh.route_deviation_count +
     vh.speed_violation_count +
     vh.stoppage_violations_count
   ) = 0;
"""
),

(
"Find vehicles where alerts occur during dry-out but trips still continue.",
"""
SELECT
  tm.truck_no,
  COUNT(a.id) AS alert_count
FROM vts_truck_master tm
JOIN alerts a
  ON tm.truck_no = a.vehicle_number
JOIN vts_ongoing_trips ot
  ON tm.truck_no = ot.tt_number
WHERE ot.actual_trip_start_datetime
      BETWEEN a.dry_out_start_time AND a.dry_out_end_time
GROUP BY tm.truck_no;
"""
),

(
"Identify vehicles with high device tampering but low transporter risk.",
"""
SELECT
  tm.truck_no,
  SUM(vh.device_tamper_count) AS tamper_events,
  tr.risk_score
FROM vts_truck_master tm
JOIN vts_alert_history vh
  ON tm.truck_no = vh.tl_number
JOIN transporter_risk_score tr
  ON tm.transporter_code = tr.transporter_code
GROUP BY tm.truck_no, tr.risk_score
HAVING SUM(vh.device_tamper_count) > 3
   AND tr.risk_score < 50;
"""
),

(
"Find vehicles where alerts spike despite decreasing trip frequency.",
"""
WITH alert_cte AS (
  SELECT vehicle_number, COUNT(*) AS alert_count
  FROM alerts
  GROUP BY vehicle_number
),
trip_cte AS (
  SELECT tl_number, SUM(total_trips) AS trip_count
  FROM vts_alert_history
  GROUP BY tl_number
)
SELECT
  tm.truck_no,
  ac.alert_count,
  tc.trip_count
FROM vts_truck_master tm
JOIN alert_cte ac ON tm.truck_no = ac.vehicle_number
JOIN trip_cte tc ON tm.truck_no = tc.tl_number
WHERE ac.alert_count > tc.trip_count;
"""
),

(
"Detect vehicles whose risk score increased after repeated alerts.",
"""
SELECT
  tm.truck_no,
  COUNT(a.id) AS alert_count,
  AVG(trs.risk_score) AS avg_risk
FROM vts_truck_master tm
JOIN alerts a
  ON tm.truck_no = a.vehicle_number
JOIN tt_risk_score trs
  ON tm.truck_no = trs.tt_number
GROUP BY tm.truck_no
HAVING COUNT(a.id) > 5
   AND AVG(trs.risk_score) > 60;
"""
),

(
"Find vehicles where alert assignment churn is high relative to trips.",
"""
SELECT
  tm.truck_no,
  COUNT(DISTINCT a.assigned_to) AS assignees,
  SUM(vh.total_trips) AS trips
FROM vts_truck_master tm
JOIN alerts a
  ON tm.truck_no = a.vehicle_number
JOIN vts_alert_history vh
  ON tm.truck_no = vh.tl_number
GROUP BY tm.truck_no
HAVING COUNT(DISTINCT a.assigned_to) > 3
   AND SUM(vh.total_trips) < 5;
"""
),

(
"Identify vehicles where alerts remain open longer than average trip duration.",
"""
SELECT
  tm.truck_no,
  AVG(EXTRACT(EPOCH FROM (a.closed_at - a.created_at))/3600) AS alert_hours,
  AVG(EXTRACT(EPOCH FROM (vh.vts_end_datetime - vh.vts_start_datetime))/3600) AS trip_hours
FROM vts_truck_master tm
JOIN alerts a
  ON tm.truck_no = a.vehicle_number
JOIN vts_alert_history vh
  ON a.unique_id = vh.alert_id
GROUP BY tm.truck_no
HAVING AVG(EXTRACT(EPOCH FROM (a.closed_at - a.created_at))/3600)
     > AVG(EXTRACT(EPOCH FROM (vh.vts_end_datetime - vh.vts_start_datetime))/3600);
"""
),

(
"Find vehicles with low utilization but high alert density.",
"""
SELECT
  tm.truck_no,
  COUNT(a.id) AS alerts,
  SUM(vh.total_trips) AS trips
FROM vts_truck_master tm
JOIN alerts a
  ON tm.truck_no = a.vehicle_number
JOIN vts_alert_history vh
  ON tm.truck_no = vh.tl_number
GROUP BY tm.truck_no
HAVING SUM(vh.total_trips) < 5
   AND COUNT(a.id) > 5;
"""
),

(
"Identify vehicles where transporter fleet risk contradicts vehicle behavior.",
"""
SELECT
  tm.truck_no,
  tr.risk_score AS transporter_risk,
  AVG(trs.risk_score) AS vehicle_risk
FROM vts_truck_master tm
JOIN transporter_risk_score tr
  ON tm.transporter_code = tr.transporter_code
JOIN tt_risk_score trs
  ON tm.truck_no = trs.tt_number
GROUP BY tm.truck_no, tr.risk_score
HAVING tr.risk_score < 40
   AND AVG(trs.risk_score) > 70;
"""
),

(
"Detect vehicles where alerts are frequent but violations are minimal.",
"""
SELECT
  tm.truck_no,
  COUNT(a.id) AS alerts,
  SUM(vh.route_deviation_count + vh.speed_violation_count) AS violations
FROM vts_truck_master tm
JOIN alerts a
  ON tm.truck_no = a.vehicle_number
JOIN vts_alert_history vh
  ON a.unique_id = vh.alert_id
GROUP BY tm.truck_no
HAVING COUNT(a.id) > 10
   AND SUM(vh.route_deviation_count + vh.speed_violation_count) < 3;
"""
),

(
"Find vehicles with repeated alerts but no ongoing trips.",
"""
SELECT
  tm.truck_no,
  COUNT(a.id) AS alert_count
FROM vts_truck_master tm
JOIN alerts a
  ON tm.truck_no = a.vehicle_number
LEFT JOIN vts_ongoing_trips ot
  ON tm.truck_no = ot.tt_number
GROUP BY tm.truck_no
HAVING COUNT(a.id) > 5
   AND COUNT(ot.id) = 0;
"""
),

(
"Identify vehicles where alerts cluster within short time windows.",
"""
SELECT
  tm.truck_no,
  COUNT(a.id) AS alerts
FROM vts_truck_master tm
JOIN alerts a
  ON tm.truck_no = a.vehicle_number
JOIN vts_alert_history vh
  ON a.unique_id = vh.alert_id
WHERE a.created_at BETWEEN vh.vts_start_datetime
                        AND vh.vts_start_datetime + INTERVAL '1 hour'
GROUP BY tm.truck_no
HAVING COUNT(a.id) > 3;
"""
),

(
"Find vehicles whose alert count exceeds transporter average.",
"""
WITH transporter_avg AS (
  SELECT
    transporter_code,
    AVG(alert_count) AS avg_alerts
  FROM (
    SELECT
      transporter_code,
      COUNT(id) AS alert_count
    FROM alerts
    GROUP BY transporter_code
  ) t
  GROUP BY transporter_code
)
SELECT
  tm.truck_no,
  COUNT(a.id) AS alert_count
FROM vts_truck_master tm
JOIN alerts a
  ON tm.truck_no = a.vehicle_number
JOIN transporter_avg ta
  ON tm.transporter_code = ta.transporter_code
GROUP BY tm.truck_no, ta.avg_alerts
HAVING COUNT(a.id) > ta.avg_alerts;
"""
),

(
"Detect vehicles with alerts but zero completed trips.",
"""
SELECT
  tm.truck_no,
  COUNT(a.id) AS alerts,
  SUM(vh.total_trips) AS trips
FROM vts_truck_master tm
JOIN alerts a
  ON tm.truck_no = a.vehicle_number
LEFT JOIN vts_alert_history vh
  ON tm.truck_no = vh.tl_number
GROUP BY tm.truck_no
HAVING COUNT(a.id) > 0
   AND COALESCE(SUM(vh.total_trips),0) = 0;
"""
),

(
"Find vehicles where alert false positives exceed real violations.",
"""
SELECT
  tm.truck_no,
  COUNT(a.id) FILTER (WHERE a.is_flagged_false = TRUE) AS false_alerts,
  SUM(vh.route_deviation_count + vh.speed_violation_count) AS violations
FROM vts_truck_master tm
JOIN alerts a
  ON tm.truck_no = a.vehicle_number
JOIN vts_alert_history vh
  ON a.unique_id = vh.alert_id
GROUP BY tm.truck_no
HAVING COUNT(a.id) FILTER (WHERE a.is_flagged_false = TRUE)
     > SUM(vh.route_deviation_count + vh.speed_violation_count);
"""
),

(
"Identify vehicles with alerts despite low mileage indicators.",
"""
SELECT
  tm.truck_no,
  SUM(vh.total_trips) AS trips,
  COUNT(a.id) AS alerts
FROM vts_truck_master tm
JOIN alerts a
  ON tm.truck_no = a.vehicle_number
JOIN vts_alert_history vh
  ON tm.truck_no = vh.tl_number
GROUP BY tm.truck_no
HAVING SUM(vh.total_trips) < 3
   AND COUNT(a.id) > 3;
"""
),


(
"Find vehicles whose average risk score is high but alert resolution progress is low.",
"""
WITH vehicle_risk AS (
  SELECT tt_number, AVG(risk_score) AS avg_risk
  FROM tt_risk_score
  GROUP BY tt_number
),
vehicle_alert_progress AS (
  SELECT vehicle_number, AVG(progress_rate) AS avg_progress
  FROM alerts
  GROUP BY vehicle_number
)
SELECT
  tm.truck_no,
  vr.avg_risk,
  vap.avg_progress
FROM vts_truck_master tm
JOIN vehicle_risk vr
  ON tm.truck_no = vr.tt_number
JOIN vehicle_alert_progress vap
  ON tm.truck_no = vap.vehicle_number
WHERE vr.avg_risk > 70
  AND vap.avg_progress < 50;
"""
),

(
"Identify vehicles with increasing alerts but decreasing trip counts.",
"""
WITH alert_counts AS (
  SELECT vehicle_number, COUNT(*) AS alert_count
  FROM alerts
  GROUP BY vehicle_number
),
trip_counts AS (
  SELECT tl_number, SUM(total_trips) AS trip_count
  FROM vts_alert_history
  GROUP BY tl_number
)
SELECT
  tm.truck_no,
  ac.alert_count,
  tc.trip_count
FROM vts_truck_master tm
JOIN alert_counts ac
  ON tm.truck_no = ac.vehicle_number
JOIN trip_counts tc
  ON tm.truck_no = tc.tl_number
WHERE ac.alert_count > tc.trip_count;
"""
),


(
"Find vehicles where stoppage violations exceed total trips.",
"""
SELECT
  tm.truck_no,
  SUM(vh.stoppage_violations_count) AS stoppages,
  SUM(vh.total_trips) AS trips
FROM vts_truck_master tm
JOIN vts_alert_history vh
  ON tm.truck_no = vh.tl_number
JOIN alerts a
  ON tm.truck_no = a.vehicle_number
GROUP BY tm.truck_no
HAVING SUM(vh.stoppage_violations_count) > SUM(vh.total_trips);
"""
),

(
"Identify vehicles whose alert severity is high despite low risk score.",
"""
SELECT
  tm.truck_no,
  AVG(trs.risk_score) AS avg_risk,
  COUNT(a.id) FILTER (WHERE a.severity = 'High') AS high_alerts
FROM vts_truck_master tm
JOIN tt_risk_score trs
  ON tm.truck_no = trs.tt_number
JOIN alerts a
  ON tm.truck_no = a.vehicle_number
GROUP BY tm.truck_no
HAVING AVG(trs.risk_score) < 40
   AND COUNT(a.id) FILTER (WHERE a.severity = 'High') > 3;
"""
),

(
"Compare vehicle capacity with frequency of overload-related alerts.",
"""
SELECT
  tm.truck_no,
  tm.capacity_of_the_truck,
  COUNT(a.id) AS alert_count
FROM vts_truck_master tm
JOIN alerts a
  ON tm.truck_no = a.vehicle_number
JOIN vts_alert_history vh
  ON a.unique_id = vh.alert_id
GROUP BY tm.truck_no, tm.capacity_of_the_truck
HAVING COUNT(a.id) > 5;
"""
),

(
"Find vehicles where night driving violations correlate with delayed alert closure.",
"""
SELECT
  tm.truck_no,
  AVG(EXTRACT(EPOCH FROM (a.closed_at - a.created_at))/3600) AS avg_closure_hours,
  SUM(vh.night_driving_count) AS night_violations
FROM vts_truck_master tm
JOIN alerts a
  ON tm.truck_no = a.vehicle_number
JOIN vts_alert_history vh
  ON a.unique_id = vh.alert_id
GROUP BY tm.truck_no
HAVING SUM(vh.night_driving_count) > 0
   AND AVG(EXTRACT(EPOCH FROM (a.closed_at - a.created_at))/3600) > 6;
"""
),

(
"Detect vehicles with frequent alerts but no corresponding violations.",
"""
SELECT
  tm.truck_no,
  COUNT(a.id) AS alerts,
  SUM(
    vh.route_deviation_count +
    vh.speed_violation_count +
    vh.stoppage_violations_count
  ) AS violations
FROM vts_truck_master tm
JOIN alerts a
  ON tm.truck_no = a.vehicle_number
JOIN vts_alert_history vh
  ON a.unique_id = vh.alert_id
GROUP BY tm.truck_no
HAVING COUNT(a.id) > 5
   AND SUM(
     vh.route_deviation_count +
     vh.speed_violation_count +
     vh.stoppage_violations_count
   ) = 0;
"""
),

(
"Find vehicles where alerts occur during dry-out but trips still continue.",
"""
SELECT
  tm.truck_no,
  COUNT(a.id) AS alert_count
FROM vts_truck_master tm
JOIN alerts a
  ON tm.truck_no = a.vehicle_number
JOIN vts_ongoing_trips ot
  ON tm.truck_no = ot.tt_number
WHERE ot.actual_trip_start_datetime
      BETWEEN a.dry_out_start_time AND a.dry_out_end_time
GROUP BY tm.truck_no;
"""
),

(
"Identify vehicles with high device tampering but low transporter risk.",
"""
SELECT
  tm.truck_no,
  SUM(vh.device_tamper_count) AS tamper_events,
  tr.risk_score
FROM vts_truck_master tm
JOIN vts_alert_history vh
  ON tm.truck_no = vh.tl_number
JOIN transporter_risk_score tr
  ON tm.transporter_code = tr.transporter_code
GROUP BY tm.truck_no, tr.risk_score
HAVING SUM(vh.device_tamper_count) > 3
   AND tr.risk_score < 50;
"""
),

(
"Find vehicles where alerts spike despite decreasing trip frequency.",
"""
WITH alert_cte AS (
  SELECT vehicle_number, COUNT(*) AS alert_count
  FROM alerts
  GROUP BY vehicle_number
),
trip_cte AS (
  SELECT tl_number, SUM(total_trips) AS trip_count
  FROM vts_alert_history
  GROUP BY tl_number
)
SELECT
  tm.truck_no,
  ac.alert_count,
  tc.trip_count
FROM vts_truck_master tm
JOIN alert_cte ac ON tm.truck_no = ac.vehicle_number
JOIN trip_cte tc ON tm.truck_no = tc.tl_number
WHERE ac.alert_count > tc.trip_count;
"""
),

(
"Detect vehicles whose risk score increased after repeated alerts.",
"""
SELECT
  tm.truck_no,
  COUNT(a.id) AS alert_count,
  AVG(trs.risk_score) AS avg_risk
FROM vts_truck_master tm
JOIN alerts a
  ON tm.truck_no = a.vehicle_number
JOIN tt_risk_score trs
  ON tm.truck_no = trs.tt_number
GROUP BY tm.truck_no
HAVING COUNT(a.id) > 5
   AND AVG(trs.risk_score) > 60;
"""
),

(
"Find vehicles where alert assignment churn is high relative to trips.",
"""
SELECT
  tm.truck_no,
  COUNT(DISTINCT a.assigned_to) AS assignees,
  SUM(vh.total_trips) AS trips
FROM vts_truck_master tm
JOIN alerts a
  ON tm.truck_no = a.vehicle_number
JOIN vts_alert_history vh
  ON tm.truck_no = vh.tl_number
GROUP BY tm.truck_no
HAVING COUNT(DISTINCT a.assigned_to) > 3
   AND SUM(vh.total_trips) < 5;
"""
),

(
"Identify vehicles where alerts remain open longer than average trip duration.",
"""
SELECT
  tm.truck_no,
  AVG(EXTRACT(EPOCH FROM (a.closed_at - a.created_at))/3600) AS alert_hours,
  AVG(EXTRACT(EPOCH FROM (vh.vts_end_datetime - vh.vts_start_datetime))/3600) AS trip_hours
FROM vts_truck_master tm
JOIN alerts a
  ON tm.truck_no = a.vehicle_number
JOIN vts_alert_history vh
  ON a.unique_id = vh.alert_id
GROUP BY tm.truck_no
HAVING AVG(EXTRACT(EPOCH FROM (a.closed_at - a.created_at))/3600)
     > AVG(EXTRACT(EPOCH FROM (vh.vts_end_datetime - vh.vts_start_datetime))/3600);
"""
),

(
"Find vehicles with low utilization but high alert density.",
"""
SELECT
  tm.truck_no,
  COUNT(a.id) AS alerts,
  SUM(vh.total_trips) AS trips
FROM vts_truck_master tm
JOIN alerts a
  ON tm.truck_no = a.vehicle_number
JOIN vts_alert_history vh
  ON tm.truck_no = vh.tl_number
GROUP BY tm.truck_no
HAVING SUM(vh.total_trips) < 5
   AND COUNT(a.id) > 5;
"""
),

(
"Identify vehicles where transporter fleet risk contradicts vehicle behavior.",
"""
SELECT
  tm.truck_no,
  tr.risk_score AS transporter_risk,
  AVG(trs.risk_score) AS vehicle_risk
FROM vts_truck_master tm
JOIN transporter_risk_score tr
  ON tm.transporter_code = tr.transporter_code
JOIN tt_risk_score trs
  ON tm.truck_no = trs.tt_number
GROUP BY tm.truck_no, tr.risk_score
HAVING tr.risk_score < 40
   AND AVG(trs.risk_score) > 70;
"""
),

(
"Detect vehicles where alerts are frequent but violations are minimal.",
"""
SELECT
  tm.truck_no,
  COUNT(a.id) AS alerts,
  SUM(vh.route_deviation_count + vh.speed_violation_count) AS violations
FROM vts_truck_master tm
JOIN alerts a
  ON tm.truck_no = a.vehicle_number
JOIN vts_alert_history vh
  ON a.unique_id = vh.alert_id
GROUP BY tm.truck_no
HAVING COUNT(a.id) > 10
   AND SUM(vh.route_deviation_count + vh.speed_violation_count) < 3;
"""
),

(
"Find vehicles with repeated alerts but no ongoing trips.",
"""
SELECT
  tm.truck_no,
  COUNT(a.id) AS alert_count
FROM vts_truck_master tm
JOIN alerts a
  ON tm.truck_no = a.vehicle_number
LEFT JOIN vts_ongoing_trips ot
  ON tm.truck_no = ot.tt_number
GROUP BY tm.truck_no
HAVING COUNT(a.id) > 5
   AND COUNT(ot.id) = 0;
"""
),

(
"Identify vehicles where alerts cluster within short time windows.",
"""
SELECT
  tm.truck_no,
  COUNT(a.id) AS alerts
FROM vts_truck_master tm
JOIN alerts a
  ON tm.truck_no = a.vehicle_number
JOIN vts_alert_history vh
  ON a.unique_id = vh.alert_id
WHERE a.created_at BETWEEN vh.vts_start_datetime
                        AND vh.vts_start_datetime + INTERVAL '1 hour'
GROUP BY tm.truck_no
HAVING COUNT(a.id) > 3;
"""
),

(
"Find vehicles whose alert count exceeds transporter average.",
"""
WITH transporter_avg AS (
  SELECT
    transporter_code,
    AVG(alert_count) AS avg_alerts
  FROM (
    SELECT
      transporter_code,
      COUNT(id) AS alert_count
    FROM alerts
    GROUP BY transporter_code
  ) t
  GROUP BY transporter_code
)
SELECT
  tm.truck_no,
  COUNT(a.id) AS alert_count
FROM vts_truck_master tm
JOIN alerts a
  ON tm.truck_no = a.vehicle_number
JOIN transporter_avg ta
  ON tm.transporter_code = ta.transporter_code
GROUP BY tm.truck_no, ta.avg_alerts
HAVING COUNT(a.id) > ta.avg_alerts;
"""
),

(
"Detect vehicles with alerts but zero completed trips.",
"""
SELECT
  tm.truck_no,
  COUNT(a.id) AS alerts,
  SUM(vh.total_trips) AS trips
FROM vts_truck_master tm
JOIN alerts a
  ON tm.truck_no = a.vehicle_number
LEFT JOIN vts_alert_history vh
  ON tm.truck_no = vh.tl_number
GROUP BY tm.truck_no
HAVING COUNT(a.id) > 0
   AND COALESCE(SUM(vh.total_trips),0) = 0;
"""
),

(
"Find vehicles where alert false positives exceed real violations.",
"""
SELECT
  tm.truck_no,
  COUNT(a.id) FILTER (WHERE a.is_flagged_false = TRUE) AS false_alerts,
  SUM(vh.route_deviation_count + vh.speed_violation_count) AS violations
FROM vts_truck_master tm
JOIN alerts a
  ON tm.truck_no = a.vehicle_number
JOIN vts_alert_history vh
  ON a.unique_id = vh.alert_id
GROUP BY tm.truck_no
HAVING COUNT(a.id) FILTER (WHERE a.is_flagged_false = TRUE)
     > SUM(vh.route_deviation_count + vh.speed_violation_count);
"""
),

(
"Identify vehicles with alerts despite low mileage indicators.",
"""
SELECT
  tm.truck_no,
  SUM(vh.total_trips) AS trips,
  COUNT(a.id) AS alerts
FROM vts_truck_master tm
JOIN alerts a
  ON tm.truck_no = a.vehicle_number
JOIN vts_alert_history vh
  ON tm.truck_no = vh.tl_number
GROUP BY tm.truck_no
HAVING SUM(vh.total_trips) < 3
   AND COUNT(a.id) > 3;
"""
),



(
"Identify vehicles whose risk score increased after multiple alerts were raised.",
"""
SELECT
  trs.tt_number,
  COUNT(a.id) AS alert_count,
  AVG(trs.risk_score) AS avg_risk
FROM tt_risk_score trs
JOIN alerts a
  ON trs.tt_number = a.vehicle_number
JOIN vts_truck_master tm
  ON trs.tt_number = tm.truck_no
WHERE trs.calculated_at > a.created_at
GROUP BY trs.tt_number
HAVING COUNT(a.id) > 5
   AND AVG(trs.risk_score) > 60;
"""
),

(
"Find vehicles with high risk score but low historical violation counts.",
"""
SELECT
  trs.tt_number,
  AVG(trs.risk_score) AS avg_risk,
  SUM(vh.route_deviation_count + vh.speed_violation_count) AS violations
FROM tt_risk_score trs
JOIN vts_alert_history vh
  ON trs.tt_number = vh.tl_number
JOIN vts_truck_master tm
  ON trs.tt_number = tm.truck_no
GROUP BY trs.tt_number
HAVING AVG(trs.risk_score) > 70
   AND SUM(vh.route_deviation_count + vh.speed_violation_count) < 3;
"""
),

(
"Detect vehicles whose risk score contradicts transporter fleet risk.",
"""
SELECT
  trs.tt_number,
  AVG(trs.risk_score) AS vehicle_risk,
  tr.risk_score AS transporter_risk
FROM tt_risk_score trs
JOIN vts_truck_master tm
  ON trs.tt_number = tm.truck_no
JOIN transporter_risk_score tr
  ON tm.transporter_code = tr.transporter_code
GROUP BY trs.tt_number, tr.risk_score
HAVING AVG(trs.risk_score) > 70
   AND tr.risk_score < 40;
"""
),

(
"Find vehicles where rising risk score aligns with increasing alert frequency.",
"""
WITH alert_counts AS (
  SELECT vehicle_number, COUNT(*) AS alert_count
  FROM alerts
  GROUP BY vehicle_number
)
SELECT
  trs.tt_number,
  AVG(trs.risk_score) AS avg_risk,
  ac.alert_count
FROM tt_risk_score trs
JOIN alert_counts ac
  ON trs.tt_number = ac.vehicle_number
JOIN vts_truck_master tm
  ON trs.tt_number = tm.truck_no
GROUP BY trs.tt_number, ac.alert_count
HAVING AVG(trs.risk_score) > 60
   AND ac.alert_count > 5;
"""
),

(
"Identify vehicles with decreasing risk score despite frequent alerts.",
"""
SELECT
  trs.tt_number,
  COUNT(a.id) AS alerts,
  AVG(trs.risk_score) AS avg_risk
FROM tt_risk_score trs
JOIN alerts a
  ON trs.tt_number = a.vehicle_number
JOIN vts_truck_master tm
  ON trs.tt_number = tm.truck_no
GROUP BY trs.tt_number
HAVING COUNT(a.id) > 5
   AND AVG(trs.risk_score) < 40;
"""
),

(
"Find vehicles whose risk score spikes during ongoing trips.",
"""
SELECT
  ot.trip_id,
  trs.tt_number,
  AVG(trs.risk_score) AS avg_risk
FROM tt_risk_score trs
JOIN vts_ongoing_trips ot
  ON trs.tt_number = ot.tt_number
JOIN vts_truck_master tm
  ON trs.tt_number = tm.truck_no
WHERE trs.calculated_at > ot.actual_trip_start_datetime
GROUP BY ot.trip_id, trs.tt_number
HAVING AVG(trs.risk_score) > 65;
"""
),

(
"Detect vehicles with high risk score but minimal trip activity.",
"""
SELECT
  trs.tt_number,
  AVG(trs.risk_score) AS avg_risk,
  SUM(vh.total_trips) AS trips
FROM tt_risk_score trs
JOIN vts_alert_history vh
  ON trs.tt_number = vh.tl_number
JOIN vts_truck_master tm
  ON trs.tt_number = tm.truck_no
GROUP BY trs.tt_number
HAVING AVG(trs.risk_score) > 60
   AND SUM(vh.total_trips) < 3;
"""
),

(
"Identify vehicles where risk score rose after night driving violations.",
"""
SELECT
  trs.tt_number,
  AVG(trs.risk_score) AS avg_risk,
  SUM(vh.night_driving_count) AS night_violations
FROM tt_risk_score trs
JOIN vts_alert_history vh
  ON trs.tt_number = vh.tl_number
JOIN alerts a
  ON trs.tt_number = a.vehicle_number
GROUP BY trs.tt_number
HAVING SUM(vh.night_driving_count) > 0
   AND AVG(trs.risk_score) > 55;
"""
),

(
"Find vehicles where risk score remains high despite alert closures.",
"""
SELECT
  trs.tt_number,
  AVG(trs.risk_score) AS avg_risk,
  COUNT(a.id) FILTER (WHERE a.alert_status = 'Closed') AS closed_alerts
FROM tt_risk_score trs
JOIN alerts a
  ON trs.tt_number = a.vehicle_number
JOIN vts_truck_master tm
  ON trs.tt_number = tm.truck_no
GROUP BY trs.tt_number
HAVING COUNT(a.id) FILTER (WHERE a.alert_status = 'Closed') > 3
   AND AVG(trs.risk_score) > 65;
"""
),

(
"Detect vehicles whose risk score increased faster than alert resolution speed.",
"""
SELECT
  trs.tt_number,
  AVG(trs.risk_score) AS avg_risk,
  AVG(EXTRACT(EPOCH FROM (a.closed_at - a.created_at))/3600) AS avg_close_hours
FROM tt_risk_score trs
JOIN alerts a
  ON trs.tt_number = a.vehicle_number
JOIN vts_truck_master tm
  ON trs.tt_number = tm.truck_no
WHERE a.closed_at IS NOT NULL
GROUP BY trs.tt_number
HAVING AVG(trs.risk_score) > 60
   AND AVG(EXTRACT(EPOCH FROM (a.closed_at - a.created_at))/3600) > 6;
"""
),

(
"Find vehicles with high risk score but low alert severity mix.",
"""
SELECT
  trs.tt_number,
  AVG(trs.risk_score) AS avg_risk,
  COUNT(a.id) FILTER (WHERE a.severity = 'High') AS high_severity_alerts
FROM tt_risk_score trs
JOIN alerts a
  ON trs.tt_number = a.vehicle_number
JOIN vts_truck_master tm
  ON trs.tt_number = tm.truck_no
GROUP BY trs.tt_number
HAVING AVG(trs.risk_score) > 65
   AND COUNT(a.id) FILTER (WHERE a.severity = 'High') < 2;
"""
),

(
"Identify vehicles whose risk score remains elevated despite low violations.",
"""
SELECT
  trs.tt_number,
  AVG(trs.risk_score) AS avg_risk,
  SUM(vh.route_deviation_count + vh.speed_violation_count) AS violations
FROM tt_risk_score trs
JOIN vts_alert_history vh
  ON trs.tt_number = vh.tl_number
JOIN alerts a
  ON trs.tt_number = a.vehicle_number
GROUP BY trs.tt_number
HAVING AVG(trs.risk_score) > 70
   AND SUM(vh.route_deviation_count + vh.speed_violation_count) < 2;
"""
),

(
"Detect vehicles where risk score contradicts alert false-positive rate.",
"""
SELECT
  trs.tt_number,
  AVG(trs.risk_score) AS avg_risk,
  COUNT(a.id) FILTER (WHERE a.is_flagged_false = TRUE) AS false_alerts
FROM tt_risk_score trs
JOIN alerts a
  ON trs.tt_number = a.vehicle_number
JOIN vts_truck_master tm
  ON trs.tt_number = tm.truck_no
GROUP BY trs.tt_number
HAVING COUNT(a.id) FILTER (WHERE a.is_flagged_false = TRUE) > 3
   AND AVG(trs.risk_score) > 65;
"""
),

(
"Find vehicles whose risk score trend is rising while trip count is falling.",
"""
SELECT
  trs.tt_number,
  AVG(trs.risk_score) AS avg_risk,
  SUM(vh.total_trips) AS trips
FROM tt_risk_score trs
JOIN vts_alert_history vh
  ON trs.tt_number = vh.tl_number
JOIN vts_truck_master tm
  ON trs.tt_number = tm.truck_no
GROUP BY trs.tt_number
HAVING AVG(trs.risk_score) > 60
   AND SUM(vh.total_trips) < 5;
"""
),

(
"Identify vehicles with high risk score during dry-out periods.",
"""
SELECT
  trs.tt_number,
  AVG(trs.risk_score) AS avg_risk
FROM tt_risk_score trs
JOIN alerts a
  ON trs.tt_number = a.vehicle_number
JOIN vts_truck_master tm
  ON trs.tt_number = tm.truck_no
WHERE trs.calculated_at BETWEEN a.dry_out_start_time AND a.dry_out_end_time
GROUP BY trs.tt_number
HAVING AVG(trs.risk_score) > 60;
""",
),

(
"Detect vehicles where risk score increased after route deviations.",
"""
SELECT
  trs.tt_number,
  AVG(trs.risk_score) AS avg_risk,
  SUM(vh.route_deviation_count) AS deviations
FROM tt_risk_score trs
JOIN vts_alert_history vh
  ON trs.tt_number = vh.tl_number
JOIN alerts a
  ON trs.tt_number = a.vehicle_number
GROUP BY trs.tt_number
HAVING SUM(vh.route_deviation_count) > 0
   AND AVG(trs.risk_score) > 55;
"""
),

(
"Find vehicles with high risk score but no ongoing trips.",
"""
SELECT
  trs.tt_number,
  AVG(trs.risk_score) AS avg_risk
FROM tt_risk_score trs
JOIN alerts a
  ON trs.tt_number = a.vehicle_number
LEFT JOIN vts_ongoing_trips ot
  ON trs.tt_number = ot.tt_number
GROUP BY trs.tt_number
HAVING AVG(trs.risk_score) > 60
   AND COUNT(ot.trip_id) = 0;
"""
),

(
"Identify vehicles where risk score correlates with alert density per trip.",
"""
SELECT
  trs.tt_number,
  AVG(trs.risk_score) AS avg_risk,
  COUNT(a.id)::float / NULLIF(SUM(vh.total_trips),0) AS alerts_per_trip
FROM tt_risk_score trs
JOIN alerts a
  ON trs.tt_number = a.vehicle_number
JOIN vts_alert_history vh
  ON trs.tt_number = vh.tl_number
GROUP BY trs.tt_number
HAVING COUNT(a.id)::float / NULLIF(SUM(vh.total_trips),0) > 2;
"""
),

(
"Detect vehicles with high risk score despite transporter low alert averages.",
"""
WITH transporter_avg AS (
  SELECT transporter_code, AVG(cnt) AS avg_alerts
  FROM (
    SELECT transporter_code, COUNT(*) AS cnt
    FROM alerts
    GROUP BY transporter_code
  ) t
  GROUP BY transporter_code
)
SELECT
  trs.tt_number,
  AVG(trs.risk_score) AS avg_risk
FROM tt_risk_score trs
JOIN vts_truck_master tm
  ON trs.tt_number = tm.truck_no
JOIN alerts a
  ON trs.tt_number = a.vehicle_number
JOIN transporter_avg ta
  ON tm.transporter_code = ta.transporter_code
GROUP BY trs.tt_number, ta.avg_alerts
HAVING AVG(trs.risk_score) > 65
   AND COUNT(a.id) < ta.avg_alerts;
"""
),

(
"Find vehicles where risk score remains high despite alert reassignment churn.",
"""
SELECT
  trs.tt_number,
  AVG(trs.risk_score) AS avg_risk,
  COUNT(DISTINCT a.assigned_to) AS assignees
FROM tt_risk_score trs
JOIN alerts a
  ON trs.tt_number = a.vehicle_number
JOIN vts_truck_master tm
  ON trs.tt_number = tm.truck_no
GROUP BY trs.tt_number
HAVING COUNT(DISTINCT a.assigned_to) > 3
   AND AVG(trs.risk_score) > 60;
"""
),

(
"Identify vehicles where risk score increased after device tampering.",
"""
SELECT
  trs.tt_number,
  AVG(trs.risk_score) AS avg_risk,
  SUM(vh.device_tamper_count) AS tamper_events
FROM tt_risk_score trs
JOIN vts_alert_history vh
  ON trs.tt_number = vh.tl_number
JOIN alerts a
  ON trs.tt_number = a.vehicle_number
GROUP BY trs.tt_number
HAVING SUM(vh.device_tamper_count) > 0
   AND AVG(trs.risk_score) > 55;
"""
),

(
"Detect vehicles whose risk score exceeds transporter risk by large margin.",
"""
SELECT
  trs.tt_number,
  AVG(trs.risk_score) AS vehicle_risk,
  tr.risk_score AS transporter_risk
FROM tt_risk_score trs
JOIN vts_truck_master tm
  ON trs.tt_number = tm.truck_no
JOIN transporter_risk_score tr
  ON tm.transporter_code = tr.transporter_code
GROUP BY trs.tt_number, tr.risk_score
HAVING AVG(trs.risk_score) - tr.risk_score > 30;
"""
),

(
"Find vehicles with high risk score but short historical trip durations.",
"""
SELECT
  trs.tt_number,
  AVG(trs.risk_score) AS avg_risk,
  AVG(EXTRACT(EPOCH FROM (vh.vts_end_datetime - vh.vts_start_datetime))/3600) AS avg_trip_hours
FROM tt_risk_score trs
JOIN vts_alert_history vh
  ON trs.tt_number = vh.tl_number
JOIN vts_truck_master tm
  ON trs.tt_number = tm.truck_no
GROUP BY trs.tt_number
HAVING AVG(trs.risk_score) > 60
   AND AVG(EXTRACT(EPOCH FROM (vh.vts_end_datetime - vh.vts_start_datetime))/3600) < 4;
"""
),


(
"Find transporters whose fleet risk score is high but most alerts are marked false.",
"""
SELECT
  tr.transporter_code,
  tr.transporter_name,
  tr.risk_score,
  COUNT(a.id) FILTER (WHERE a.is_flagged_false = TRUE) AS false_alerts,
  COUNT(a.id) AS total_alerts
FROM transporter_risk_score tr
JOIN vts_truck_master tm
  ON tr.transporter_code = tm.transporter_code
JOIN alerts a
  ON tm.truck_no = a.vehicle_number
GROUP BY tr.transporter_code, tr.transporter_name, tr.risk_score
HAVING tr.risk_score > 60
   AND COUNT(a.id) FILTER (WHERE a.is_flagged_false = TRUE)::FLOAT
       / NULLIF(COUNT(a.id),0) > 0.5;
"""
),

(
"Identify transporters with high fleet risk but low average vehicle risk.",
"""
SELECT
  tr.transporter_code,
  tr.transporter_name,
  tr.risk_score AS fleet_risk,
  AVG(tt.risk_score) AS avg_vehicle_risk
FROM transporter_risk_score tr
JOIN vts_truck_master tm
  ON tr.transporter_code = tm.transporter_code
JOIN tt_risk_score tt
  ON tm.truck_no = tt.tt_number
GROUP BY tr.transporter_code, tr.transporter_name, tr.risk_score
HAVING tr.risk_score > 60
   AND AVG(tt.risk_score) < 40;
"""
),

(
"Find transporters whose fleet risk is driven mainly by route deviations.",
"""
SELECT
  tr.transporter_code,
  tr.route_deviation,
  tr.risk_score,
  SUM(vh.route_deviation_count) AS total_deviations
FROM transporter_risk_score tr
JOIN vts_truck_master tm
  ON tr.transporter_code = tm.transporter_code
JOIN vts_alert_history vh
  ON tm.truck_no = vh.tl_number
GROUP BY tr.transporter_code, tr.route_deviation, tr.risk_score
HAVING tr.route_deviation > tr.stoppage_violation
   AND SUM(vh.route_deviation_count) > 0;
"""
),

(
"Detect transporters with high fleet risk but fast alert resolution.",
"""
SELECT
  tr.transporter_code,
  tr.risk_score,
  AVG(EXTRACT(EPOCH FROM (a.closed_at - a.created_at))/3600) AS avg_resolution_hours
FROM transporter_risk_score tr
JOIN vts_truck_master tm
  ON tr.transporter_code = tm.transporter_code
JOIN alerts a
  ON tm.truck_no = a.vehicle_number
WHERE a.closed_at IS NOT NULL
GROUP BY tr.transporter_code, tr.risk_score
HAVING tr.risk_score > 60
   AND AVG(EXTRACT(EPOCH FROM (a.closed_at - a.created_at))/3600) < 4;
"""
),

(
"Identify transporters whose fleet risk is high despite low trip volumes.",
"""
SELECT
  tr.transporter_code,
  tr.risk_score,
  SUM(vh.total_trips) AS total_trips
FROM transporter_risk_score tr
JOIN vts_truck_master tm
  ON tr.transporter_code = tm.transporter_code
JOIN vts_alert_history vh
  ON tm.truck_no = vh.tl_number
GROUP BY tr.transporter_code, tr.risk_score
HAVING tr.risk_score > 60
   AND SUM(vh.total_trips) < 10;
"""
),

(
"Find transporters where device-related risk exceeds actual device tampering events.",
"""
SELECT
  tr.transporter_code,
  tr.device_removed,
  SUM(vh.device_tamper_count) AS tamper_events
FROM transporter_risk_score tr
JOIN vts_truck_master tm
  ON tr.transporter_code = tm.transporter_code
JOIN vts_alert_history vh
  ON tm.truck_no = vh.tl_number
GROUP BY tr.transporter_code, tr.device_removed
HAVING tr.device_removed > 50
   AND SUM(vh.device_tamper_count) = 0;
"""
),

(
"Detect transporters with high fleet risk but mostly closed alerts.",
"""
SELECT
  tr.transporter_code,
  tr.risk_score,
  COUNT(a.id) FILTER (WHERE a.alert_status = 'Closed') AS closed_alerts,
  COUNT(a.id) AS total_alerts
FROM transporter_risk_score tr
JOIN vts_truck_master tm
  ON tr.transporter_code = tm.transporter_code
JOIN alerts a
  ON tm.truck_no = a.vehicle_number
GROUP BY tr.transporter_code, tr.risk_score
HAVING tr.risk_score > 60
   AND COUNT(a.id) FILTER (WHERE a.alert_status = 'Closed')::FLOAT
       / NULLIF(COUNT(a.id),0) > 0.8;
"""
),

(
"Find transporters whose fleet risk is driven by a small subset of vehicles.",
"""
SELECT
  tr.transporter_code,
  COUNT(DISTINCT tm.truck_no) AS vehicles,
  COUNT(DISTINCT CASE WHEN tt.risk_score > 70 THEN tm.truck_no END) AS high_risk_vehicles
FROM transporter_risk_score tr
JOIN vts_truck_master tm
  ON tr.transporter_code = tm.transporter_code
JOIN tt_risk_score tt
  ON tm.truck_no = tt.tt_number
GROUP BY tr.transporter_code
HAVING COUNT(DISTINCT CASE WHEN tt.risk_score > 70 THEN tm.truck_no END) < 0.2 * COUNT(DISTINCT tm.truck_no);
"""
),

(
"Identify transporters with high fleet risk but minimal ongoing violations.",
"""
SELECT
  tr.transporter_code,
  tr.risk_score,
  COUNT(ot.id) AS ongoing_trips
FROM transporter_risk_score tr
JOIN vts_truck_master tm
  ON tr.transporter_code = tm.transporter_code
LEFT JOIN vts_ongoing_trips ot
  ON tm.truck_no = ot.tt_number
GROUP BY tr.transporter_code, tr.risk_score
HAVING tr.risk_score > 60
   AND COUNT(ot.id) < 2;
"""
),

(
"Find transporters whose fleet risk contradicts alert severity mix.",
"""
SELECT
  tr.transporter_code,
  tr.risk_score,
  COUNT(a.id) FILTER (WHERE a.severity = 'High') AS high_sev_alerts
FROM transporter_risk_score tr
JOIN vts_truck_master tm
  ON tr.transporter_code = tm.transporter_code
JOIN alerts a
  ON tm.truck_no = a.vehicle_number
GROUP BY tr.transporter_code, tr.risk_score
HAVING tr.risk_score > 60
   AND COUNT(a.id) FILTER (WHERE a.severity = 'High') = 0;
"""
),

(
"Detect transporters where fleet risk remains high despite frequent reassignment of alerts.",
"""
SELECT
  tr.transporter_code,
  tr.risk_score,
  COUNT(DISTINCT a.assigned_to) AS assignees
FROM transporter_risk_score tr
JOIN vts_truck_master tm
  ON tr.transporter_code = tm.transporter_code
JOIN alerts a
  ON tm.truck_no = a.vehicle_number
GROUP BY tr.transporter_code, tr.risk_score
HAVING COUNT(DISTINCT a.assigned_to) > 5
   AND tr.risk_score > 60;
"""
),

(
"Find transporters with high fleet risk but short average trip durations.",
"""
SELECT
  tr.transporter_code,
  tr.risk_score,
  AVG(EXTRACT(EPOCH FROM (vh.vts_end_datetime - vh.vts_start_datetime))/3600) AS avg_trip_hours
FROM transporter_risk_score tr
JOIN vts_truck_master tm
  ON tr.transporter_code = tm.transporter_code
JOIN vts_alert_history vh
  ON tm.truck_no = vh.tl_number
GROUP BY tr.transporter_code, tr.risk_score
HAVING tr.risk_score > 60
   AND AVG(EXTRACT(EPOCH FROM (vh.vts_end_datetime - vh.vts_start_datetime))/3600) < 3;
"""
),

(
"Identify transporters whose fleet risk exceeds regional average risk.",
"""
WITH regional_avg AS (
  SELECT
    tm.region,
    AVG(tr.risk_score) AS avg_risk
  FROM transporter_risk_score tr
  JOIN vts_truck_master tm
    ON tr.transporter_code = tm.transporter_code
  GROUP BY tm.region
)
SELECT
  tr.transporter_code,
  tr.risk_score,
  ra.avg_risk
FROM transporter_risk_score tr
JOIN vts_truck_master tm
  ON tr.transporter_code = tm.transporter_code
JOIN regional_avg ra
  ON tm.region = ra.region
WHERE tr.risk_score > ra.avg_risk;
"""
),

(
"Find transporters whose fleet risk is dominated by stoppage violations.",
"""
SELECT
  tr.transporter_code,
  tr.stoppage_violation,
  tr.route_deviation,
  SUM(vh.stoppage_violations_count) AS stoppages
FROM transporter_risk_score tr
JOIN vts_truck_master tm
  ON tr.transporter_code = tm.transporter_code
JOIN vts_alert_history vh
  ON tm.truck_no = vh.tl_number
GROUP BY tr.transporter_code, tr.stoppage_violation, tr.route_deviation
HAVING tr.stoppage_violation > tr.route_deviation
   AND SUM(vh.stoppage_violations_count) > 0;
"""
),

(
"Detect transporters whose fleet risk stays high despite minimal night driving violations.",
"""
SELECT
  tr.transporter_code,
  tr.risk_score,
  SUM(vh.night_driving_count) AS night_violations
FROM transporter_risk_score tr
JOIN vts_truck_master tm
  ON tr.transporter_code = tm.transporter_code
JOIN vts_alert_history vh
  ON tm.truck_no = vh.tl_number
GROUP BY tr.transporter_code, tr.risk_score
HAVING tr.risk_score > 60
   AND SUM(vh.night_driving_count) = 0;
"""
),

(
"Find transporters whose fleet risk is inconsistent with alert density per trip.",
"""
SELECT
  tr.transporter_code,
  tr.risk_score,
  COUNT(a.id)::FLOAT / NULLIF(SUM(vh.total_trips),0) AS alerts_per_trip
FROM transporter_risk_score tr
JOIN vts_truck_master tm
  ON tr.transporter_code = tm.transporter_code
JOIN alerts a
  ON tm.truck_no = a.vehicle_number
JOIN vts_alert_history vh
  ON tm.truck_no = vh.tl_number
GROUP BY tr.transporter_code, tr.risk_score
HAVING tr.risk_score > 60
   AND COUNT(a.id)::FLOAT / NULLIF(SUM(vh.total_trips),0) < 0.5;
"""
),

(
"Find alerts where vehicle risk is high but alert progress rate remains low.",
"""
SELECT
  a.unique_id,
  a.vehicle_number,
  a.progress_rate,
  AVG(trs.risk_score) AS avg_risk
FROM alerts a
JOIN tt_risk_score trs
  ON a.vehicle_number = trs.tt_number
JOIN vts_truck_master tm
  ON a.vehicle_number = tm.truck_no
GROUP BY a.unique_id, a.vehicle_number, a.progress_rate
HAVING AVG(trs.risk_score) > 65
   AND a.progress_rate < 30;
""",
),

(
"Identify alerts generated during trips that later showed route deviations.",
"""
SELECT
  a.unique_id,
  a.created_at,
  SUM(vh.route_deviation_count) AS deviations
FROM alerts a
JOIN vts_alert_history vh
  ON a.vehicle_number = vh.tl_number
JOIN vts_truck_master tm
  ON a.vehicle_number = tm.truck_no
WHERE a.created_at BETWEEN vh.vts_start_datetime AND vh.vts_end_datetime
GROUP BY a.unique_id, a.created_at
HAVING SUM(vh.route_deviation_count) > 0;
"""
),

(
"Detect alerts raised for vehicles whose transporter fleet risk is low.",
"""
SELECT
  a.unique_id,
  tr.transporter_code,
  tr.risk_score AS fleet_risk
FROM alerts a
JOIN vts_truck_master tm
  ON a.vehicle_number = tm.truck_no
JOIN transporter_risk_score tr
  ON tm.transporter_code = tr.transporter_code
WHERE tr.risk_score < 40;
"""
),

(
"Find alerts where vehicle was blacklisted but no device tampering occurred.",
"""
SELECT
  a.unique_id,
  tm.whether_truck_blacklisted,
  SUM(vh.device_tamper_count) AS tamper_events
FROM alerts a
JOIN vts_truck_master tm
  ON a.vehicle_number = tm.truck_no
JOIN vts_alert_history vh
  ON tm.truck_no = vh.tl_number
GROUP BY a.unique_id, tm.whether_truck_blacklisted
HAVING tm.whether_truck_blacklisted = 'Y'
   AND SUM(vh.device_tamper_count) = 0;
"""
),

(
"Identify alerts raised for vehicles with unusually short historical trips.",
"""
SELECT
  a.unique_id,
  AVG(EXTRACT(EPOCH FROM (vh.vts_end_datetime - vh.vts_start_datetime))/3600) AS avg_trip_hours
FROM alerts a
JOIN vts_alert_history vh
  ON a.vehicle_number = vh.tl_number
JOIN vts_truck_master tm
  ON a.vehicle_number = tm.truck_no
GROUP BY a.unique_id
HAVING AVG(EXTRACT(EPOCH FROM (vh.vts_end_datetime - vh.vts_start_datetime))/3600) < 2;
""",
),

(
"Find alerts where transporter fleet risk contradicts alert severity.",
"""
SELECT
  a.unique_id,
  a.severity,
  tr.risk_score
FROM alerts a
JOIN vts_truck_master tm
  ON a.vehicle_number = tm.truck_no
JOIN transporter_risk_score tr
  ON tm.transporter_code = tr.transporter_code
WHERE a.severity = 'High'
  AND tr.risk_score < 35;
"""
),

(
"Identify alerts created after repeated night driving violations.",
"""
SELECT
  a.unique_id,
  SUM(vh.night_driving_count) AS night_violations
FROM alerts a
JOIN vts_alert_history vh
  ON a.vehicle_number = vh.tl_number
JOIN vts_truck_master tm
  ON tm.truck_no = a.vehicle_number
WHERE vh.vts_end_datetime < a.created_at
GROUP BY a.unique_id
HAVING SUM(vh.night_driving_count) > 3;
"""
),

(
"Find alert history records where vehicle risk increased despite fewer alerts.",
"""
SELECT
  vh.tl_number,
  SUM(vh.total_trips) AS trips,
  AVG(trs.risk_score) AS avg_risk,
  COUNT(a.id) AS alerts
FROM vts_alert_history vh
JOIN tt_risk_score trs
  ON vh.tl_number = trs.tt_number
JOIN alerts a
  ON vh.tl_number = a.vehicle_number
GROUP BY vh.tl_number
HAVING AVG(trs.risk_score) > 60
   AND COUNT(a.id) < 3;
"""
),

(
"Identify alert history entries where transporter risk is driven by few vehicles.",
"""
SELECT
  tm.transporter_code,
  COUNT(DISTINCT vh.tl_number) AS vehicles,
  SUM(vh.route_deviation_count) AS deviations
FROM vts_alert_history vh
JOIN vts_truck_master tm
  ON vh.tl_number = tm.truck_no
JOIN transporter_risk_score tr
  ON tm.transporter_code = tr.transporter_code
GROUP BY tm.transporter_code
HAVING SUM(vh.route_deviation_count) > 0
   AND COUNT(DISTINCT vh.tl_number) < 3;
"""
),

(
"Find historical trips where alerts were raised but no violations occurred.",
"""
SELECT
  vh.id,
  COUNT(a.id) AS alerts
FROM vts_alert_history vh
JOIN alerts a
  ON vh.tl_number = a.vehicle_number
JOIN vts_truck_master tm
  ON vh.tl_number = tm.truck_no
GROUP BY vh.id
HAVING COUNT(a.id) > 0
   AND SUM(
     vh.route_deviation_count +
     vh.speed_violation_count +
     vh.stoppage_violations_count
   ) = 0;
"""
),

(
"Detect alert history entries with high violations but low vehicle risk.",
"""
SELECT
  vh.tl_number,
  SUM(vh.route_deviation_count + vh.speed_violation_count) AS violations,
  AVG(trs.risk_score) AS avg_risk
FROM vts_alert_history vh
JOIN tt_risk_score trs
  ON vh.tl_number = trs.tt_number
JOIN vts_truck_master tm
  ON tm.truck_no = vh.tl_number
GROUP BY vh.tl_number
HAVING SUM(vh.route_deviation_count + vh.speed_violation_count) > 5
   AND AVG(trs.risk_score) < 40;
"""
),

(
"Find alert history where trips exceeded schedule and alerts followed.",
"""
SELECT
  vh.id,
  COUNT(a.id) AS alerts_after_trip
FROM vts_alert_history vh
JOIN alerts a
  ON vh.tl_number = a.vehicle_number
JOIN vts_truck_master tm
  ON tm.truck_no = vh.tl_number
WHERE vh.vts_end_datetime > vh.scheduled_trip_end_datetime
  AND a.created_at > vh.vts_end_datetime
GROUP BY vh.id;
"""
),
(
"Identify vehicles whose ownership type correlates with higher alert density.",
"""
SELECT
  tm.ownership_type,
  COUNT(a.id)::FLOAT / COUNT(DISTINCT tm.truck_no) AS alerts_per_vehicle
FROM vts_truck_master tm
JOIN alerts a
  ON tm.truck_no = a.vehicle_number
JOIN vts_alert_history vh
  ON tm.truck_no = vh.tl_number
GROUP BY tm.ownership_type;
"""
),

(
"Find trucks where high capacity does not translate to lower risk.",
"""
SELECT
  tm.truck_no,
  tm.capacity_of_the_truck,
  AVG(trs.risk_score) AS avg_risk
FROM vts_truck_master tm
JOIN tt_risk_score trs
  ON tm.truck_no = trs.tt_number
JOIN alerts a
  ON tm.truck_no = a.vehicle_number
GROUP BY tm.truck_no, tm.capacity_of_the_truck
HAVING tm.capacity_of_the_truck > 20000
   AND AVG(trs.risk_score) > 60;
"""
),

(
"Detect trucks whose blacklist status does not align with alert volume.",
"""
SELECT
  tm.truck_no,
  tm.whether_truck_blacklisted,
  COUNT(a.id) AS alerts
FROM vts_truck_master tm
JOIN alerts a
  ON tm.truck_no = a.vehicle_number
JOIN vts_alert_history vh
  ON tm.truck_no = vh.tl_number
GROUP BY tm.truck_no, tm.whether_truck_blacklisted
HAVING tm.whether_truck_blacklisted = 'N'
   AND COUNT(a.id) > 10;
"""
),

(
"Find trucks where transporter fleet risk is low but vehicle risk is high.",
"""
SELECT
  tm.truck_no,
  AVG(trs.risk_score) AS vehicle_risk,
  tr.risk_score AS fleet_risk
FROM vts_truck_master tm
JOIN tt_risk_score trs
  ON tm.truck_no = trs.tt_number
JOIN transporter_risk_score tr
  ON tm.transporter_code = tr.transporter_code
GROUP BY tm.truck_no, tr.risk_score
HAVING AVG(trs.risk_score) > 65
   AND tr.risk_score < 40;
"""
),

(
"Identify trucks with frequent alerts but minimal trip activity.",
"""
SELECT
  tm.truck_no,
  COUNT(a.id) AS alerts,
  SUM(vh.total_trips) AS trips
FROM vts_truck_master tm
JOIN alerts a
  ON tm.truck_no = a.vehicle_number
JOIN vts_alert_history vh
  ON tm.truck_no = vh.tl_number
GROUP BY tm.truck_no
HAVING COUNT(a.id) > 8
   AND SUM(vh.total_trips) < 3;
"""
),
    # Critical alerts by region and transporter - Demonstrates proper multi-level grouping
    (
        "How many critical alerts are still open for more than 48 hours, grouped by region and transporter?",
        """SELECT 
    'critical_alerts_open_48hrs' AS "Data Table",
    a.zone as region,
    a.transporter_name,
    COUNT(a.id) as critical_alert_count
FROM alerts a
WHERE 
    a.alert_section = 'VTS'
    AND a.alert_status = 'Open'
    AND a.severity = 'CRITICAL'
    AND a.created_at <= CURRENT_TIMESTAMP - INTERVAL '48 hours'
GROUP BY 
    a.zone, a.transporter_name
ORDER BY 
    a.zone, critical_alert_count DESC"""
    ),

    # ===== NEW: GOLD STANDARD - Correct Violation Counting =====
    ("Which vehicles have a total of more than 10 speed violations in the last 30 days?",
    """SELECT 'vts_alert_history' AS "Data Table",
       tl_number AS vehicle_number,
       SUM(speed_violation_count) AS total_speed_violations
FROM vts_alert_history
WHERE vts_end_datetime >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY tl_number
HAVING SUM(speed_violation_count) > 10"""),

    # ===== NEW: GOLD STANDARD - Exclusion Logic for "No Alerts" =====
    ("List blacklisted vehicles that have a risk score",
    """SELECT
    'blacklist_risk' AS "Data Table",
    vtm.truck_no,
    trs.risk_score
FROM vts_truck_master vtm
JOIN tt_risk_score trs ON vtm.truck_no = trs.tt_number
WHERE vtm.whether_truck_blacklisted = 'Y'"""),

    ("Give vehicles have not generated any alerts in the last 7 days",
    """SELECT
    'vts_truck_master' AS "Data Table",
    vtm.truck_no,
    vtm.transporter_name
FROM vts_truck_master vtm
WHERE NOT EXISTS (
    SELECT 1
    FROM alerts a
    WHERE a.vehicle_number = vtm.truck_no
    AND a.created_at >= CURRENT_DATE - INTERVAL '7 days'
);"""),
]

# Convert raw pairs to structured dictionaries with metadata
def _classify_query_for_metadata(question: str, sql: str) -> str:
    """Classify query for metadata based on keywords."""
    q_lower = question.lower()
    sql_lower = sql.lower()
    if "lag(" in sql_lower or "lead(" in sql_lower:
        return "sequential_analysis"
    if "corr(" in sql_lower:
        return "correlation_analysis"
    if "not exists" in sql_lower or "left join" in sql_lower and "is null" in sql_lower:
        return "exclusion_logic"
    if "avg(" in sql_lower or "stddev" in sql_lower:
        return "statistical_aggregation"
    return "general"

TRAINING_QA_PAIRS = [
    {"question": q, "sql": s, "query_type": _classify_query_for_metadata(q, s)} for q, s in TRAINING_QA_PAIRS_RAW
]
