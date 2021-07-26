from typing import Optional


class Analysis:
    """Model for Analysis"""

    def __init__(self) -> None:
        self.type = None
        self.status = None
        self.output = None
        self.sample_ids = None
        # very easy to index, simple queries
        self.timestamp_completed: Optional = None
