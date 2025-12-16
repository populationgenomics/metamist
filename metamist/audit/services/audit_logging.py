from collections import defaultdict
import logging
import tempfile

from metamist.audit.models import AuditConfig, AuditResult, SequencingGroup


def setup_logger(
    dataset: str, name: str, level: str = 'INFO', log_file: str = None
) -> logging.Logger:
    """Set up logger for audit reviews."""
    logger = logging.getLogger(name)

    # Avoid duplicate handlers
    if logger.handlers:
        return logging.LoggerAdapter(logger, {'dataset': dataset})

    formatter = logging.Formatter(
        fmt='%(asctime)s %(levelname)s %(module)s:%(lineno)d - %(dataset)s :: %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
    )

    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    # Optional file handler for audit persistence
    if log_file:
        file_handler = logging.FileHandler(log_file)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    logger.setLevel(getattr(logging, level.upper()))
    logger.propagate = False

    return logging.LoggerAdapter(logger, {'dataset': dataset})


class BucketAuditLogger:
    """Logging wrapper for bucket audit operations."""

    def __init__(self, dataset: str, name: str):
        """Initialize the audit logger."""
        self.log_file = tempfile.NamedTemporaryFile(  # pylint: disable=consider-using-with
            delete=False
        ).name
        self.logger = setup_logger(dataset, name, log_file=self.log_file)

    def info(self, message: str):
        """Log an info message."""
        self.logger.info(message)

    def info_nl(self, message: str):
        """Log an info message and a newline."""
        self.logger.info(message)
        self.logger.info('')

    def warning(self, message: str):
        """Log a warning message."""
        self.logger.warning(message)

    def error(self, message: str):
        """Log an error message."""
        self.logger.error(message)

    def log_initialization(self, config: AuditConfig):
        """Log audit initialization details."""
        self.info(f'Initializing {config.audit_type} audit'.center(50, '~'))
        self.info('')
        self.info(f'Dataset:                    {config.dataset}')
        self.info(
            f'Sequencing Types:           {", ".join(sorted(config.sequencing_types))}'
        )
        self.info(
            f'Sequencing Technologies:    {", ".join(sorted(config.sequencing_technologies))}'
        )
        self.info(
            f'Sequencing Platforms:       {", ".join(sorted(config.sequencing_platforms))}'
        )
        self.info(
            f'Analysis Types:             {", ".join(sorted(config.analysis_types))}'
        )
        self.info(
            f'File Types:                 {", ".join(sorted(ft.name for ft in config.file_types))}'
        )
        if config.excluded_prefixes:
            self.info(
                f'Excluded Prefixes:          {", ".join(sorted(config.excluded_prefixes))}'
            )
        self.info('')

    def log_sg_summary(self, sgs: list[SequencingGroup]):
        """Log summary of sequencing groups by type/technology."""
        counts = defaultdict(int)
        for sg in sgs:
            key = f'{sg.technology}|{sg.type}|{sg.platform}'
            counts[key] += 1

        for key, count in sorted(counts.items()):
            tech, typ, platform = key.split('|')
            self.info(f'  {count:5} ({tech}, {typ}, {platform})')
        self.info('')

    def log_result_summary(self, result: AuditResult) -> dict[str, float]:
        """Log audit result summary."""
        total_size_to_delete = sum(
            entry.filesize or 0 for entry in result.files_to_delete
        )
        total_size_to_review = sum(
            entry.filesize or 0 for entry in result.files_to_review
        )

        stats = {
            'files_to_delete': len(result.files_to_delete),
            'files_to_delete_size_gb': total_size_to_delete / (1024**3),
            'files_to_review': len(result.files_to_review),
            'files_to_review_size_gb': total_size_to_review / (1024**3),
            'unaligned_sgs': len(result.unaligned_sequencing_groups),
        }

        self.info_nl('Audit Summary'.center(50, '~'))
        self.info(
            f'Files to delete:           {stats["files_to_delete"]} '
            f'({stats["files_to_delete_size_gb"]:.2f} GB)'
        )
        self.info(
            f'Files to review:           {stats["files_to_review"]} '
            f'({stats["files_to_review_size_gb"]:.2f} GB)'
        )
        self.info_nl(f'Unaligned SGs:             {stats["unaligned_sgs"]}')
        return stats
