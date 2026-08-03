column_definitions = {
    "historic_ownership": {
        "table": {
            "comment": (
                "Time-segmented ownership history for assets, reconciled across multiple time-varying datasets: "
                "(1) fund→asset company ownership chain (relationship graph), "
                "(2) asset↔investment portfolio mapping, "
                "(3) investment portfolio version validity and portfolio→fund association, "
                "(4) asset lifecycle milestones/phases, and "
                "(5) asset ownership milestones/phases. "
                "The pipeline constructs all change boundaries from these sources, slices time into minimal contiguous "
                "segments [start, end) where all states are constant, annotates each segment with the state from each "
                "dataset, applies business rules (including a Lock Box override that forces ownership to 1.0 on/after "
                "the Lock Box milestone), then collapses adjacent identical segments to avoid unnecessary splits. "
                "The natural grain is (asset_id, fund_core_id, investment_portfolio_id, invp_version_id, full_rel_id, "
                "company_chain_id, start, end)."
            ),
        },
        "columns": {
            # -----------------------
            # Segment metadata / provenance
            # -----------------------
            "opened_by_sources": {
                "schema": {"data_type": "ARRAY<STRING>"},
                "description": {
                    "comment": (
                        "Provenance for why this segment boundary exists. Contains the set of source boundary types "
                        "that opened the segment start (e.g., relationship from/to, mapping from/to, portfolio→fund validity, "
                        "lifecycle milestone change, ownership milestone change). When segments are collapsed, this list is "
                        "merged across the collapsed rows."
                    ),
                    "long_name": "Segment Opening Sources",
                    "unit": "n/a",
                },
            },
            "start": {
                "schema": {"data_type": "TIMESTAMP"},
                "description": {
                    "comment": (
                        "Segment start timestamp (inclusive). The segment applies for all timestamps t where start ≤ t < end. "
                        "Within a segment, relationship chain, mapping/validity state, milestones/phases, and computed ownership "
                        "are constant."
                    ),
                    "long_name": "Segment Start Timestamp",
                    "unit": "n/a",
                },
            },
            "end": {
                "schema": {"data_type": "TIMESTAMP"},
                "description": {
                    "comment": (
                        "Segment end timestamp (exclusive). The segment applies for all timestamps t where start ≤ t < end. "
                        "The next segment (if any) begins at this timestamp."
                    ),
                    "long_name": "Segment End Timestamp",
                    "unit": "n/a",
                },
            },

            # -----------------------
            # Human-friendly asset / fund / portfolio fields (for reporting)
            # -----------------------
            "asset": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Human-readable asset name (from infrastructure or platform asset dimension).",
                    "long_name": "Asset Name",
                    "unit": "n/a",
                },
            },
            "asset_type": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": (
                        "Asset class inferred from asset_id prefix: 'Infrastructure' when asset_id starts with 'ASST_INFR_', "
                        "otherwise 'Platform'."
                    ),
                    "long_name": "Asset Type",
                    "unit": "n/a",
                },
            },
            "fund": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Human-readable fund display name (resolved via fund core → primary fund name).",
                    "long_name": "Fund Name",
                    "unit": "n/a",
                },
            },
            "investment_portfolio": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Human-readable investment portfolio name for the portfolio referenced by investment_portfolio_id.",
                    "long_name": "Investment Portfolio Name",
                    "unit": "n/a",
                },
            },
            "investment_portfolio_version": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Investment portfolio version identifier used for the segment (alias of invp_version_id).",
                    "long_name": "Investment Portfolio Version",
                    "unit": "n/a",
                },
            },

            # -----------------------
            # Lifecycle display fields (resolved names)
            # -----------------------
            "lifecycle_milestones": {
                "schema": {"data_type": "ARRAY<STRUCT<milestone_id:STRING, milestone_date:TIMESTAMP>>"},
                "description": {
                    "comment": (
                        "Array of lifecycle milestone events that fall within the segment's active phase/interval. "
                        "Each element contains the raw milestone identifier and its specific event date. "
                        "Used as the source for the resolved 'lifecycle_milestones_details' column."
                    ),
                    "long_name": "Lifecycle Milestones (Raw IDs & Dates)",
                    "unit": "n/a",
                },
            },
            "lifecycle_phase": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": (
                        "Lifecycle phase name inferred for the segment (resolved from lifecycle_phase_id). "
                        "Represents the asset’s lifecycle phase during the entire segment."
                    ),
                    "long_name": "Lifecycle Phase (Name)",
                    "unit": "n/a",
                },
            },
            "lifecycle_milestone_type": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": (
                        "Lifecycle milestone type name (resolved from lifecycle_milestone_type_id). "
                        "This classifies the lifecycle milestone into a broader milestone type/category."
                    ),
                    "long_name": "Lifecycle Milestone Type (Name)",
                    "unit": "n/a",
                },
            },

            # -----------------------
            # Ownership display fields (resolved names)
            # -----------------------
            "ownership_milestones": {
                "schema": {"data_type": "ARRAY<STRUCT<milestone_id:STRING, milestone_date:TIMESTAMP>>"},
                "description": {
                    "comment": (
                        "Array of ownership milestone events that fall within the segment's active phase/interval. "
                        "Each element contains the raw milestone identifier and its specific event date. "
                        "Used as the source for the resolved 'ownership_milestones_details' column and for Lock Box logic."
                    ),
                    "long_name": "Ownership Milestones (Raw IDs & Dates)",
                    "unit": "n/a",
                },
            },
            "ownership_phase": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": (
                        "Ownership phase name inferred for the segment (resolved from ownership_phase_id). "
                        "Represents the asset’s ownership phase during the entire segment."
                    ),
                    "long_name": "Ownership Phase (Name)",
                    "unit": "n/a",
                },
            },
            "ownership_milestone_type": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": (
                        "Ownership milestone type name (resolved from ownership_milestone_type_id). "
                        "This classifies the ownership milestone into a broader milestone type/category."
                    ),
                    "long_name": "Ownership Milestone Type (Name)",
                    "unit": "n/a",
                },
            },

            # -----------------------
            # Core identifiers (technical keys)
            # -----------------------
            "asset_id": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": (
                        "Unique identifier of the asset. Infrastructure assets typically use the 'ASST_INFR_' prefix; "
                        "platform assets use a different prefix. Used as a key throughout segmentation and joining."
                    ),
                    "long_name": "Asset Identifier",
                    "unit": "n/a",
                },
            },
            "investment_portfolio_id": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": (
                        "Identifier of the investment portfolio the asset is mapped to for the segment. "
                        "Mapping validity is enforced via mapping_active."
                    ),
                    "long_name": "Investment Portfolio Identifier",
                    "unit": "n/a",
                },
            },
            "invp_version_id": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": (
                        "Identifier of the investment portfolio version active for the segment. "
                        "Part of the natural grain; used in mapping and fund alignment."
                    ),
                    "long_name": "Investment Portfolio Version Identifier",
                    "unit": "n/a",
                },
            },
            "fund_core_id": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": (
                        "Identifier of the fund associated with the portfolio version for the segment. "
                        "This fund must match the fund at the top of the relationship chain used to compute ownership."
                    ),
                    "long_name": "Fund Core Identifier",
                    "unit": "n/a",
                },
            },
            "ultimate_parent_id": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": (
                        "Company identifier of the ultimate parent in the relationship chain (typically the fund company). "
                        "This is the chain root used to anchor ownership calculations."
                    ),
                    "long_name": "Ultimate Parent Company Identifier",
                    "unit": "n/a",
                },
            },
            "ultimate_child_id": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": (
                        "Company identifier of the ultimate child in the relationship chain (typically the asset company). "
                        "This is mapped to asset_id via the assetco→asset bridge."
                    ),
                    "long_name": "Ultimate Child Company Identifier",
                    "unit": "n/a",
                },
            },
            "company_chain_id": {
                "schema": {"data_type": "ARRAY<STRING>"},
                "description": {
                    "comment": (
                        "Array of company_core_id values representing the ordered company chain from ultimate_parent_id to ultimate_child_id. "
                        "Consecutive duplicates are removed during chain parsing to avoid artificial edges."
                    ),
                    "long_name": "Company Chain (IDs)",
                    "unit": "n/a",
                },
            },
            "company_chain": {
                "schema": {"data_type": "ARRAY<STRING>"},
                "description": {
                    "comment": (
                        "Human-readable company chain derived from company_chain_id by mapping company_core_id → registered company name. "
                        "If a name is missing, the ID is retained as a fallback."
                    ),
                    "long_name": "Company Chain (Names)",
                    "unit": "n/a",
                },
            },

            # -----------------------
            # Lifecycle milestone state (technical IDs)
            # -----------------------
            "lifecycle_milestones": {
                "schema": {"data_type": "ARRAY<STRUCT<milestone_id:STRING, milestone_date:TIMESTAMP>>"},
                "description": {
                    "comment": (
                        "Array of lifecycle milestone events that fall within the segment's active phase/interval. "
                        "Each element contains the raw milestone identifier and its specific event date. "
                        "Used as the source for the resolved 'lifecycle_milestones_details' column."
                    ),
                    "long_name": "Lifecycle Milestones (Raw IDs & Dates)",
                    "unit": "n/a",
                },
            },
            "lifecycle_milestone_type_id": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Identifier of the milestone type/category for the lifecycle milestone covering the segment.",
                    "long_name": "Lifecycle Milestone Type Identifier",
                    "unit": "n/a",
                },
            },
            "lifecycle_phase_id": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Lifecycle phase identifier inferred for the segment.",
                    "long_name": "Lifecycle Phase Identifier",
                    "unit": "n/a",
                },
            },

            # -----------------------
            # Ownership milestone state (technical IDs)
            # -----------------------
            "ownership_milestones": {
                "schema": {"data_type": "ARRAY<STRUCT<milestone_id:STRING, milestone_date:TIMESTAMP>>"},
                "description": {
                    "comment": (
                        "Array of ownership milestone events covering the segment. Aggregated from the source timeline "
                        "based on phase grouping. Contains technical IDs and precise event timestamps used for ownership overrides."
                    ),
                    "long_name": "Ownership Milestones (IDs & Dates)",
                    "unit": "n/a",
                },
            },
            "ownership_milestone_type_id": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Identifier of the milestone type/category for the ownership milestone covering the segment.",
                    "long_name": "Ownership Milestone Type Identifier",
                    "unit": "n/a",
                },
            },
            "ownership_phase_id": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Ownership phase identifier inferred for the segment.",
                    "long_name": "Ownership Phase Identifier",
                    "unit": "n/a",
                },
            },

            # -----------------------
            # Ownership calculations
            # -----------------------
            "ownership": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": (
                        "Raw computed ownership for the fund→asset relationship chain for the segment, derived from the relationship graph. "
                        "Produced by multiplying edge percentages along the path and rounding."
                    ),
                    "long_name": "Raw Path Ownership",
                    "unit": "fraction",
                },
            },
            "company_chain_ownership_id": {
                "schema": {"data_type": "ARRAY<STRUCT<pair_rel_id:STRING, percentage:DOUBLE, parent_child_pair:ARRAY<STRING>>>"},
                "description": {
                    "comment": (
                        "Raw (ID-based) ownership breakdown of the relationship chain. Each element represents one parent→child edge in order, "
                        "including the edge identifier (pair_rel_id), the edge percentage, and the parent/child company IDs."
                    ),
                    "long_name": "Company Chain Ownership Breakdown (IDs)",
                    "unit": "mixed",
                },
            },
            "company_chain_ownership": {
                "schema": {"data_type": "ARRAY<STRUCT<pair_rel_id:STRING, percentage:DOUBLE, parent_child_pair:ARRAY<STRING>>>"},
                "description": {
                    "comment": (
                        "Human-readable ownership breakdown of the relationship chain, derived from company_chain_ownership_id by mapping "
                        "company IDs to registered company names. pair_rel_id is rendered as 'ParentName.ChildName'."
                    ),
                    "long_name": "Company Chain Ownership Breakdown (Names)",
                    "unit": "mixed",
                },
            },
            "effective_path_ownership": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": (
                        "Business-rule ownership output for the segment. "
                        "Rules: (1) if the asset is on/after the Lock Box milestone (OWNR_MILE_1002), set to 1.0; "
                        "(2) else if all gating checks pass and raw ownership is not null, use raw ownership; "
                        "(3) otherwise set to 0.0."
                    ),
                    "long_name": "Effective Path Ownership",
                    "unit": "fraction",
                },
            },
            "equal_ownerships": {
                "schema": {"data_type": "BOOLEAN"},
                "description": {
                    "comment": (
                        "TRUE when ownership equals effective_path_ownership for the segment. "
                        "FALSE indicates a business-rule adjustment (Lock Box override or gating failure)."
                    ),
                    "long_name": "Raw vs Effective Ownership Match Flag",
                    "unit": "n/a",
                },
            },

            # -----------------------
            # Gating / validity flags
            # -----------------------
            "has_relpath": {
                "schema": {"data_type": "BOOLEAN"},
                "description": {
                    "comment": "TRUE when a fund→asset relationship chain exists and fully covers the segment.",
                    "long_name": "Has Relationship Path Flag",
                    "unit": "n/a",
                },
            },
            "mapping_active": {
                "schema": {"data_type": "BOOLEAN"},
                "description": {
                    "comment": (
                        "TRUE when the asset→investment portfolio mapping is valid and covers the entire segment."
                    ),
                    "long_name": "Asset to Portfolio Mapping Active Flag",
                    "unit": "n/a",
                },
            },
            "portfolio_active": {
                "schema": {"data_type": "BOOLEAN"},
                "description": {
                    "comment": (
                        "TRUE when the investment portfolio version maps to THIS fund (fund_core_id) and that association covers the entire segment. "
                        "Note: in this table, portfolio_active is sourced from the ID table’s invp_active flag."
                    ),
                    "long_name": "Portfolio-to-Fund Validity Flag",
                    "unit": "n/a",
                },
            },
            "has_lifecycle_milestone": {
                "schema": {"data_type": "BOOLEAN"},
                "description": {
                    "comment": "TRUE when a lifecycle milestone interval exists that fully covers the segment for the asset.",
                    "long_name": "Has Lifecycle Milestone Flag",
                    "unit": "n/a",
                },
            },
            "has_ownership_milestone": {
                "schema": {"data_type": "BOOLEAN"},
                "description": {
                    "comment": "TRUE when an ownership milestone interval exists that fully covers the segment for the asset.",
                    "long_name": "Has Ownership Milestone Flag",
                    "unit": "n/a",
                },
            },

            # -----------------------
            # Data quality
            # -----------------------
            "questionable": {
                "schema": {"data_type": "BOOLEAN"},
                "description": {
                    "comment": (
                        "Data quality flag for segments that fail expected consistency/coverage checks. "
                        "Generally TRUE when any key gate is false (missing relpath, mapping inactive, portfolio/fund validity inactive, "
                        "missing lifecycle milestone, missing ownership milestone) or when raw vs effective ownership differs. "
                        "Exception: if the ONLY issue is raw vs effective mismatch and the asset is on/after Lock Box, "
                        "questionable is forced to FALSE because the mismatch is expected under the Lock Box override."
                    ),
                    "long_name": "Questionable Segment Flag",
                    "unit": "n/a",
                },
            },
        },
    },


"historic_ownership_ids": {
        "table": {
            "comment": (
                "ID-first time-segmented ownership history for assets, representing the same calendarised segment logic as "
                "historic_ownership but retaining technical identifiers rather than display names. Built by anchoring across "
                "(1) fund→asset company ownership chain (relationship graph), (2) asset↔investment portfolio mapping, "
                "(3) portfolio version→fund association validity, and (4) lifecycle and (5) ownership milestone histories. "
                "All change boundaries are unioned to create minimal contiguous segments [start, end) where all states are constant. "
                "Each segment includes raw chain ownership plus business-rule effective ownership (including Lock Box override). "
                "Adjacent identical segments are collapsed to avoid unnecessary splits. Natural grain: "
                "(asset_id, fund_core_id, investment_portfolio_id, invp_version_id, full_rel_id, company_chain_id, start, end)."
            ),
        },
        "columns": {
            # -----------------------
            # Segment metadata / provenance
            # -----------------------
            "opened_by_sources": {
                "schema": {"data_type": "ARRAY<STRING>"},
                "description": {
                    "comment": (
                        "Provenance for why this segment boundary exists. Contains the set of boundary source types that opened "
                        "the segment start (e.g., relationship from/to, mapping from/to, portfolio→fund validity from/to, "
                        "lifecycle milestone change, ownership milestone change). When segments are collapsed, the source lists "
                        "are merged and de-duplicated."
                    ),
                    "long_name": "Segment Opening Sources",
                    "unit": "n/a",
                },
            },
            "start": {
                "schema": {"data_type": "TIMESTAMP"},
                "description": {
                    "comment": (
                        "Segment start timestamp (inclusive). The segment applies for all timestamps t where start ≤ t < end. "
                        "Within a segment, relationship chain, mapping/validity, milestone/phase IDs, and computed ownership "
                        "are constant."
                    ),
                    "long_name": "Segment Start Timestamp",
                    "unit": "n/a",
                },
            },
            "end": {
                "schema": {"data_type": "TIMESTAMP"},
                "description": {
                    "comment": (
                        "Segment end timestamp (exclusive). The segment applies for all timestamps t where start ≤ t < end."
                    ),
                    "long_name": "Segment End Timestamp",
                    "unit": "n/a",
                },
            },

            # -----------------------
            # Core identifiers (technical keys)
            # -----------------------
            "asset_id": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Unique identifier of the asset (infrastructure or platform).",
                    "long_name": "Asset Identifier",
                    "unit": "n/a",
                },
            },
            "investment_portfolio_id": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Identifier of the investment portfolio the asset is mapped to for the segment.",
                    "long_name": "Investment Portfolio Identifier",
                    "unit": "n/a",
                },
            },
            "investment_portfolio_version": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Identifier of the investment portfolio version active for the segment.",
                    "long_name": "Investment Portfolio Version Identifier",
                    "unit": "n/a",
                },
            },
            "fund_core_id": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": (
                        "Identifier of the fund associated with the portfolio version for the segment. Must align with the "
                        "fund at the top of the relationship chain used to compute ownership."
                    ),
                    "long_name": "Fund Core Identifier",
                    "unit": "n/a",
                },
            },
            "full_rel_id": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Identifier for the full relationship chain path from ultimate_parent_id to ultimate_child_id.",
                    "long_name": "Fund to Asset Company Relationship Chain ID",
                    "unit": "n/a",
                },
            },
            "ultimate_parent_id": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Company identifier of the ultimate parent in the relationship chain (typically the fund company).",
                    "long_name": "Ultimate Parent Company Identifier",
                    "unit": "n/a",
                },
            },
            "ultimate_child_id": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Company identifier of the ultimate child in the relationship chain (typically the asset company).",
                    "long_name": "Ultimate Child Company Identifier",
                    "unit": "n/a",
                },
            },
            "company_chain_id": {
                "schema": {"data_type": "ARRAY<STRING>"},
                "description": {
                    "comment": (
                        "Ordered array of company_core_id values representing the company chain from ultimate_parent_id to ultimate_child_id."
                    ),
                    "long_name": "Company Chain (IDs)",
                    "unit": "n/a",
                },
            },

            # -----------------------
            # Milestone / phase IDs
            # -----------------------
            "lifecycle_milestones": {
                "schema": {"data_type": "ARRAY<STRUCT<milestone_id:STRING, milestone_date:TIMESTAMP>>"},
                "description": {
                    "comment": (
                        "Array of lifecycle milestone events covering the segment. Aggregated from the source timeline "
                        "based on phase grouping. Contains technical IDs and precise event timestamps."
                    ),
                    "long_name": "Lifecycle Milestones (IDs & Dates)",
                    "unit": "n/a",
                },
            },
            "lifecycle_milestone_type_id": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Lifecycle milestone type identifier covering the segment.",
                    "long_name": "Lifecycle Milestone Type Identifier",
                    "unit": "n/a",
                },
            },
            "lifecycle_phase_id": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Lifecycle phase identifier covering the segment.",
                    "long_name": "Lifecycle Phase Identifier",
                    "unit": "n/a",
                },
            },
            "ownership_milestones": {
                "schema": {"data_type": "ARRAY<STRUCT<milestone_id:STRING, milestone_date:TIMESTAMP>>"},
                "description": {
                    "comment": (
                        "Array of ownership milestone events covering the segment. Aggregated from the source timeline "
                        "based on phase grouping. Contains technical IDs and precise event timestamps used for ownership overrides."
                    ),
                    "long_name": "Ownership Milestones (IDs & Dates)",
                    "unit": "n/a",
                },
            },
            "ownership_milestone_type_id": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Ownership milestone type identifier covering the segment.",
                    "long_name": "Ownership Milestone Type Identifier",
                    "unit": "n/a",
                },
            },
            "ownership_phase_id": {
                "schema": {"data_type": "STRING"},
                "description": {
                    "comment": "Ownership phase identifier covering the segment.",
                    "long_name": "Ownership Phase Identifier",
                    "unit": "n/a",
                },
            },

            # -----------------------
            # Ownership calculations
            # -----------------------
            "ownership": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": "Raw computed ownership for the relationship chain for the segment (product of edge percentages).",
                    "long_name": "Raw Path Ownership",
                    "unit": "fraction",
                },
            },
            "company_chain_ownership_id": {
                "schema": {"data_type": "ARRAY<STRUCT<pair_rel_id:STRING, percentage:DOUBLE, parent_child_pair:ARRAY<STRING>>>"},
                "description": {
                    "comment": (
                        "ID-based breakdown of ownership along the company chain. Each element represents one parent→child edge "
                        "in chain order with: pair_rel_id, percentage, and parent_child_pair."
                    ),
                    "long_name": "Company Chain Ownership Breakdown (IDs)",
                    "unit": "mixed",
                },
            },
            "effective_path_ownership": {
                "schema": {"data_type": "DOUBLE"},
                "description": {
                    "comment": (
                        "Business-rule ownership output for the segment (Lock Box forces 1.0 on/after OWNR_MILE_1002; "
                        "otherwise gated raw ownership; else 0.0)."
                    ),
                    "long_name": "Effective Path Ownership",
                    "unit": "fraction",
                },
            },
            "equal_ownerships": {
                "schema": {"data_type": "BOOLEAN"},
                "description": {
                    "comment": "TRUE when ownership equals effective_path_ownership for the segment.",
                    "long_name": "Raw vs Effective Ownership Match Flag",
                    "unit": "n/a",
                },
            },

            # -----------------------
            # Gating / validity flags
            # -----------------------
            "has_relpath": {
                "schema": {"data_type": "BOOLEAN"},
                "description": {
                    "comment": "TRUE when a valid fund→asset relationship chain exists and fully covers the segment.",
                    "long_name": "Has Relationship Path Flag",
                    "unit": "n/a",
                },
            },
            "mapping_active": {
                "schema": {"data_type": "BOOLEAN"},
                "description": {
                    "comment": "TRUE when the asset→portfolio mapping is valid and covers the entire segment.",
                    "long_name": "Asset to Portfolio Mapping Active Flag",
                    "unit": "n/a",
                },
            },
            "invp_active": {
                "schema": {"data_type": "BOOLEAN"},
                "description": {
                    "comment": (
                        "TRUE when the portfolio version maps to THIS fund_core_id and that association covers the entire segment "
                        "(portfolio→fund validity for this fund)."
                    ),
                    "long_name": "Portfolio-to-Fund Validity Flag",
                    "unit": "n/a",
                },
            },
            "has_lifecycle_milestone": {
                "schema": {"data_type": "BOOLEAN"},
                "description": {
                    "comment": "TRUE when a lifecycle milestone interval exists that fully covers the segment for the asset.",
                    "long_name": "Has Lifecycle Milestone Flag",
                    "unit": "n/a",
                },
            },
            "has_ownership_milestone": {
                "schema": {"data_type": "BOOLEAN"},
                "description": {
                    "comment": "TRUE when an ownership milestone interval exists that fully covers the segment for the asset.",
                    "long_name": "Has Ownership Milestone Flag",
                    "unit": "n/a",
                },
            },

            # -----------------------
            # Data quality
            # -----------------------
            "questionable": {
                "schema": {"data_type": "BOOLEAN"},
                "description": {
                    "comment": (
                        "TRUE when key gates fail or when raw vs effective ownership differs unexpectedly. "
                        "Lock Box mismatches are treated as expected and are not flagged as questionable when gates are otherwise good."
                    ),
                    "long_name": "Questionable Segment Flag",
                    "unit": "n/a",
                },
            },
        },
    },
}