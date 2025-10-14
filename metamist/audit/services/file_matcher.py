"""File matching service for finding moved and duplicate files."""

from abc import ABC, abstractmethod

from metamist.audit.models import Analysis, FileMetadata, MovedFile


class FileMatcher(ABC):
    """Abstract base class for file matching strategies."""

    @abstractmethod
    def match(
        self, file: FileMetadata, candidates: list[FileMetadata]
    ) -> FileMetadata | None:
        """
        Match a file against a list of candidates.

        Args:
            file: File to match
            candidates: List of candidate files

        Returns:
            Matched file or None
        """
        pass  # pylint: disable=unnecessary-pass


class ChecksumMatcher(FileMatcher):
    """Match files by checksum."""

    def match(
        self, file: FileMetadata, candidates: list[FileMetadata]
    ) -> FileMetadata | None:
        """Match files by checksum."""
        if not file.checksum:
            return None

        for candidate in candidates:
            if candidate.checksum == file.checksum:
                return candidate

        return None


class FilenameSizeMatcher(FileMatcher):
    """Match files by filename and size."""

    def match(
        self, file: FileMetadata, candidates: list[FileMetadata]
    ) -> FileMetadata | None:
        """Match files by filename and size."""
        if not file.filesize and not file.filepath.name:
            return None

        for candidate in candidates:
            if (
                candidate.filepath.name == file.filepath.name
                and candidate.filesize == file.filesize
            ):
                return candidate

        return None


class CompositeFileMatcher:
    """Try multiple matching strategies in order."""

    def __init__(self, matchers: list[FileMatcher]):
        """
        Initialize with list of matchers.

        Args:
            matchers: List of matcher strategies to try
        """
        self.matchers = matchers

    def match(
        self, file: FileMetadata, candidates: list[FileMetadata]
    ) -> FileMetadata | None:
        """
        Try each matcher until a match is found.

        Args:
            file: File to match
            candidates: List of candidate files

        Returns:
            First matched file or None
        """
        for matcher in self.matchers:
            result = matcher.match(file, candidates)
            if result:
                return result
        return None


class FileMatchingService:
    """Service for matching files between Metamist and GCS."""

    def __init__(self):
        """Initialize the file matching service."""
        self.matcher = CompositeFileMatcher([ChecksumMatcher(), FilenameSizeMatcher()])

    def find_moved_files(
        self, metamist_files: list[FileMetadata], bucket_files: list[FileMetadata]
    ) -> dict[str, MovedFile]:
        """
        Find files that have been moved within the bucket.

        Files are considered moved if they have the same checksum or
        same filename/size but different paths.

        Args:
            metamist_files: Files recorded in Metamist
            bucket_files: Files found in the bucket

        Returns:
            Dictionary mapping old path to MovedFile objects
        """
        moved_files = {}

        # Create lookup structures
        metamist_files_by_path = {str(f.filepath): f for f in metamist_files}

        for bucket_file in bucket_files:
            # Skip if file is at the expected location
            if str(bucket_file.filepath) in metamist_files_by_path:
                continue

            # Try to find a match in Metamist files
            match = self.matcher.match(bucket_file, metamist_files)

            if match and match.filepath != bucket_file.filepath:
                # File has been moved
                moved_files[str(match.filepath)] = MovedFile(
                    old_path=match.filepath,
                    new_path=bucket_file.filepath,
                    metadata=bucket_file,
                )

        return moved_files

    def find_original_analysis_files(
        self,
        analyses: list[Analysis],
        bucket_files: list[FileMetadata],
    ):
        """
        Update the original files for the given analyses based on checksums.
        """
        for analysis in analyses:
            for bucket_file in bucket_files:
                if analysis.output_file.checksum == bucket_file.checksum:
                    analysis.original_file = bucket_file

    def find_uningested_files(
        self,
        metamist_files: list[FileMetadata],
        bucket_files: list[FileMetadata],
        moved_files: dict[str, MovedFile],
        analysis_files: list[FileMetadata] | None = None,
    ) -> list[FileMetadata]:
        """
        Find files in the bucket that are not in Metamist.

        Args:
            metamist_files: Files recorded in Metamist
            bucket_files: Files found in the bucket
            moved_files: Files that have been moved
            analysis_files: Optional analysis output files to check against

        Returns:
            List of uningested files
        """
        # Get all known file paths
        known_paths = set()

        # Add Metamist file paths
        for f in metamist_files:
            known_paths.add(str(f.filepath))

        # Add moved file new paths
        for moved in moved_files.values():
            known_paths.add(str(moved.new_path))

        # Find files not in known paths
        uningested = []
        for bucket_file in bucket_files:
            if str(bucket_file.filepath) not in known_paths:
                # Check if it matches an analysis file
                is_analysis_file = False
                if analysis_files and bucket_file.checksum:
                    for analysis_file in analysis_files:
                        if analysis_file.checksum == bucket_file.checksum:
                            is_analysis_file = True
                            break

                if not is_analysis_file:
                    uningested.append(bucket_file)

        return uningested
