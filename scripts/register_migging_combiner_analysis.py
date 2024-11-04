"""Register a missing combiner analysis in Metamist

Ad-hoc script to register a missing combiner analysis in Metamist.
Is supremely inflexible since there is a short timeframe for combining previous
VDS' for the OurDNA browser release.
"""

from argparse import ArgumentParser

from hail.vds import read_vds

from cpg_utils import to_path
from metamist.apis import AnalysisApi
from metamist.models import Analysis, AnalysisStatus


def get_sg_ids(vds: str) -> list[str]:
    return read_vds(to_path(vds)).variant_data.s.collect()


def main(vds: str, dataset: str):
    aapi = AnalysisApi()
    am = Analysis(
        type='combiner',
        output=vds,
        status=AnalysisStatus('completed'),
        sequencing_group_ids=get_sg_ids(vds),
        meta=None,
    )
    aapi.create_analysis(project=dataset, analysis=am)


if __name__ == '__main__':
    parser = ArgumentParser()
    parser.add_argument('--vds', help='VDS to register analysis for.')
    parser.add_argument('--dataset', help='Dataset to register analysis in.')
    args = parser.parse_args()
    main(args.vds, args.dataset)
