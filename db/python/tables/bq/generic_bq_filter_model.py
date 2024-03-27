import dataclasses
from typing import Any

from db.python.tables.bq.generic_bq_filter import GenericBQFilter
from db.python.utils import GenericFilterModel


def prepare_bq_query_from_dict_field(
    filter_, field_name, column_name
) -> tuple[list[str], dict[str, Any]]:
    """
    Prepare a SQL query from a dict field, which is a dict of GenericFilters.
    Usually this is a JSON field in the database that we want to query on.
    """
    conditionals: list[str] = []
    values: dict[str, Any] = {}
    for key, value in filter_.items():
        if not isinstance(value, GenericBQFilter):
            raise ValueError(f'Filter {field_name} must be a GenericFilter')
        if '"' in key:
            raise ValueError('Meta key contains " character, which is not allowed')
        if "'" in key:
            raise ValueError("Meta key contains ' character, which is not allowed")
        fconditionals, fvalues = value.to_sql(
            f"JSON_EXTRACT({column_name}, '$.{key}')",
            column_name=f'{column_name}_{key}',
        )
        conditionals.append(fconditionals)
        values.update(fvalues)

    return conditionals, values


@dataclasses.dataclass(kw_only=True)
class GenericBQFilterModel(GenericFilterModel):
    """
    Class that contains fields of GenericBQFilters that can be used to filter
    """

    def __post_init__(self):
        for field in dataclasses.fields(self):
            value = getattr(self, field.name)
            if value is None:
                continue

            if isinstance(value, tuple) and len(value) == 1 and value[0] is None:
                raise ValueError(
                    'There is very likely a trailing comma on the end of '
                    f'{self.__class__.__name__}.{field.name}. If you actually want a '
                    'tuple of length one with the value = (None,), then use '
                    'dataclasses.field(default_factory=lambda: (None,))'
                )
            if isinstance(value, GenericBQFilter):
                continue

            if isinstance(value, dict):
                # make sure each field is a GenericFilter, or set it to be one,
                # in this case it's always 'eq', never automatically in_
                new_value = {
                    k: v if isinstance(v, GenericBQFilter) else GenericBQFilter(eq=v)
                    for k, v in value.items()
                }
                setattr(self, field.name, new_value)
                continue

            # lazily provided a value, which we'll correct
            if isinstance(value, list):
                setattr(self, field.name, GenericBQFilter(in_=value))
            else:
                setattr(self, field.name, GenericBQFilter(eq=value))

    def to_sql(
        self, field_overrides: dict[str, str] = None,  only: list[str] | None = None, exclude: list[str] | None = None,
    ) -> tuple[str, dict[str, Any]]:
        """Convert the model to SQL, and avoid SQL injection"""
        _foverrides = field_overrides or {}

        # check for bad field_overrides
        bad_field_overrides = set(_foverrides.keys()) - set(
            f.name for f in dataclasses.fields(self)
        )
        if bad_field_overrides:
            raise ValueError(
                f'Specified field overrides that were not used: {bad_field_overrides}'
            )

        fields = dataclasses.fields(self)
        conditionals, values = [], {}
        for field in fields:
            fcolumn = _foverrides.get(field.name, field.name)
            if filter_ := getattr(self, field.name):
                if isinstance(filter_, dict):
                    meta_conditionals, meta_values = prepare_bq_query_from_dict_field(
                        filter_=filter_, field_name=field.name, column_name=fcolumn
                    )
                    conditionals.extend(meta_conditionals)
                    values.update(meta_values)
                elif isinstance(filter_, GenericBQFilter):
                    fconditionals, fvalues = filter_.to_sql(fcolumn)
                    conditionals.append(fconditionals)
                    values.update(fvalues)
                else:
                    raise ValueError(
                        f'Filter {field.name} must be a GenericBQFilter or '
                        'dict[str, GenericBQFilter]'
                    )

        if not conditionals:
            return 'True', {}

        return ' AND '.join(filter(None, conditionals)), values
