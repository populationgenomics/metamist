# Ingesting population labels for HGDP and 1KG (thousand-genomes) datasets

1. Download [HGDP population list](https://www.internationalgenome.org/data-portal/data-collection/hgdp) (54 populations -> Download the list) and save as `sources/igsr-hgdp-pops.tsv`

1. Download [HGDP sample list](https://www.internationalgenome.org/data-portal/data-collection/hgdp) (828 samples -> Download the list) and save as `sources/igsr-hgdp-samples.tsv`

1. Do the same for [SGDP](https://www.internationalgenome.org/data-portal/data-collection/sgdp) and for [30x GRCh38 1KG](https://www.internationalgenome.org/data-portal/data-collection/30x-grch38)

1. SGDP external IDs used in NAGIM are illumina sequencing IDs, and extra metadata is needed to map them to sample IDs. Download [this metadata](https://github.com/kaspermunch/PopulationGenomicsCourse/blob/5a6a2a3850af755cff63661e95644e9381f8031e/Cluster/metadata/Simons_meta_ENArun.txt) and save it in the same folder.

1. Run the Rmd notebook `tidy.Rmd` in RStudio to process 7 files above, harmonise the population labels and descriptions, and merge into 1 file `sources/samples-pops.tsv`.

1. Download [1kg pedigree data](https://www.internationalgenome.org/faq/can-i-get-phenotype-gender-and-family-relationship-information-for-the-individuals) and cut 6 columns (otherwise metamist will error):

```sh
cut -f1-6 sources/20130606_g1k.ped > sources/20130606_g1k-cut16.ped
```

1. Run script to populate and update sample, participant, and family entries in metamist:

```sh
python ingest_metadata.py
```

The script will ask for your confirmation on each step.
