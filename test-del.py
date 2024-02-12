from typing import Any

import rapidjson


def reformat_bigqquery_labels(data: list[dict[str, Any]]) -> dict[str, Any]:
    """
    Convert from {'key': 'KEY1', 'value': 'VAL1'} to {'KEY1': 'VAL1'}
    and keep other keys as there are
    """
    labels = {}
    for kv in data:
        if 'key' in kv:
            labels[kv['key']] = kv['value']
        else:
            # otherwise keep the original key
            for k, v in kv.items():
                labels[k] = v

    return labels


d = {
    "labels": [
        {"key": "dataset", "value": "fewgenomes"},
        {"key": "batch_id", "value": "431827"},
        {"key": "job_id", "value": "1"},
        {"key": "batch_resource", "value": "ip-fee/1024/1"},
        {
            "key": "batch_name",
            "value": "michael.franklin metamist:7a3611cf6e0f97ad8f2a2e3b2f0f381e01e7f501/python -c from metamist.graphql import query; q \u003d \"query q {\\n  myProjects {\\n    name\\n  }\\n}\"; print(query(q))",
        },
        {
            "key": "url",
            "value": "https://batch.hail.populationgenomics.org.au/batches/431827",
        },
        {"key": "namespace", "value": "test"},
        {"key": "ar-guid", "value": "de7ba882-505f-4b6f-a8bb-e614604c8a5d"},
    ]
}

print(
    rapidjson.dumps(
        reformat_bigqquery_labels(d["labels"]),
        sort_keys=True,
    )
)
