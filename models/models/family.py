class Family:
    """Family model"""

    def __init__(
        self,
        id_=None,
        external_id=None,
        project=None,
        description=None,
        coded_phenotype=None,
    ):
        self.id = id_
        self.external_id = external_id
        self.project = project
        self.description = description
        self.coded_phenotype = coded_phenotype

    @staticmethod
    def from_db(d):
        """From DB fields"""
        identifier = d.pop('id')
        return Family(id_=identifier, **d)
