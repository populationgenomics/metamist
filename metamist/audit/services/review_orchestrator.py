"""
Class for orchestrating the upload bucket audit review process.
"""

import pandas as pd
import numpy as np

from metamist.audit.data_access.gcs_data_access import GCSDataAccess
from metamist.audit.models import AuditReportEntry, ReviewResult
from metamist.audit.services import BucketAuditLogger, Reporter

from cpg_utils import to_path

ACTIONS = ['DELETE', 'INGEST', 'REVIEW']


class AuditReviewOrchestrator:  # pylint: disable=too-many-instance-attributes
    """
    A class that manages and orchestrates the audit review process.
    """

    def __init__(
        self,
        dataset: str,
        results_folder: str,
    ):
        self.dataset = dataset
        self.results_folder = results_folder
        self.report = 'files_to_review'

        self.audit_logger = BucketAuditLogger(self.dataset, 'audit_review')
        self.gcs = GCSDataAccess(self.dataset)
        self.reporter = Reporter(self.gcs, self.audit_logger, self.results_folder)

    def review_audit_report(
        self, action: str, comment: str, filter_expressions: list[str] | None = None
    ) -> None:
        """Review files listed in the audit results."""
        self.audit_logger.info_nl(f"Reading report '{self.report}'".center(50, '~'))
        rows = self.reporter.get_report_rows_from_name(self.report)
        rows = self.filter_rows_and_update(rows, action, comment, filter_expressions)
        review_result = self.review_rows(
            rows,
            action,
            comment,
        )
        self.audit_logger.info_nl(
            f'Added comments to {len(review_result.reviewed_files)} / {len(rows)} files.'
        )
        if review_result.reviewed_files:
            self.reporter.write_reviewed_files_report(review_result)

        self.audit_logger.info_nl('Review complete!')

    def review_rows(
        self,
        rows: list[AuditReportEntry],
        action: str,
        comment: str,
    ) -> ReviewResult:
        """Review files from audit results based on filters and comments."""
        action = action.upper()
        if action not in ACTIONS:
            raise ValueError(f'Invalid action: {action}. Must be one of {ACTIONS}.')
        self.audit_logger.info_nl(f"Annotating rows with comment: '{comment}'")
        reviewed_rows: list[AuditReportEntry] = []
        for row in rows:
            file_path = to_path(row.filepath)
            if not file_path.exists():
                self.audit_logger.warning(
                    f'File {file_path} does not exist, skipping review.'
                )
                continue

            row.update_action(action)
            row.update_review_comment(comment)
            reviewed_rows.append(row)

        return ReviewResult(reviewed_files=reviewed_rows)

    def filter_rows_and_update(
        self,
        rows: list[AuditReportEntry],
        action: str,
        comment: str,
        filters: list[str] | None = None,
    ) -> list[AuditReportEntry]:
        """Apply filters to audit report entries and update action/comment."""
        if not filters:
            return rows

        # Convert rows to DataFrame for filtering
        df = pd.DataFrame([r.to_report_dict() for r in rows])

        # 1. Convert CLI 'contains' syntax to pandas syntax if needed
        # User: "File Path contains .tar" -> Pandas: "`File Path`.str.contains('.tar')"
        # User: "SG Type==exome" -> Pandas: "`SG Type` == 'exome'"
        query_parts = []
        for f in filters:
            if ' contains ' in f:
                col, val = f.split(' contains ', 1)
                # clean up quotes and whitespace
                col = col.strip()
                val = val.strip().strip('\'"')
                # Pandas string contains query
                query_parts.append(f"`{col}`.str.contains('{val}', na=False)")
            else:
                # Assume standard python syntax (==, !=, >, <) which pandas .query() supports
                # Just ensure columns with spaces are backticked
                # This requires a bit of regex parsing to robustly wrap columns in backticks
                # For simple cases, we pass it through:
                query_parts.append(f)

        # 2. Combine filters (Generic CLI usually implies AND logic for multiple flags)
        full_query = ' and '.join(query_parts)

        print(f'Executing Query: {full_query}')

        try:
            # 3. Find matching rows
            # Pandas .query is very powerful and safe
            matches = df.query(full_query)

            # 4. Update
            if not matches.empty:
                df.loc[matches.index, 'Action'] = action
                df.loc[matches.index, 'Review Comment'] = comment
                print(f'Updated {len(matches)} rows.')
            else:
                print('No rows matched the criteria.')

        except Exception as e:  # pylint: disable=broad-exception-caught
            print(f'Query failed: {e}')

        clean_df = df.replace({np.nan: None})
        records = clean_df.to_dict('records')

        # Convert back to AuditReportEntry
        return [AuditReportEntry.from_report_dict(record) for record in records]
