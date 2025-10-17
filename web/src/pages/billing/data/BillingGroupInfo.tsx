//Add Billing groups of R&D Team here
export const BillingGroupListRD = [
    {
        name: 'All RD Datasets',
        groupBy: 'topic',
        topics: 'acute-care,ag-cardiac,ag-hidden,ag-very-hidden,bccc,brain-malf,broad-rgp,chop-gliadx,circa,epileptic-enceph,flinders-ophthal,geneadd,genomic-autopsy,ghfm-kidgen,gid-grdr,heartkids,hereditary-neuro,ibmdx,kidgen,leukodystrophies,mcri-lrp,mgha-bmf,mito-disease,mito-mdt,ohmr3-mendelian,ohmr4-epilepsy,perth-neuro,ravenscroft-arch,ravenscroft-rdstudy,ravenscroft-rpl,rdc-hsp,rdnow,rdp-autoimmune,rdp-kidney,rdp-neuro,schr-neuro,udn-aus,udn-aus-training,uom-ird,validation',
    },
    {
        name: 'Perkins',
        groupBy: 'topic',
        topics: 'perth-neuro,ravenscroft-arch,ravenscroft-rdstudy,ravenscroft-rpl,rdc-hsp',
    },
    {
        name: 'Reanalysis',
        groupBy: 'topic',
        topics: 'acute-care,ag-cardiac,ag-hidden,brain-malf,broad-rgp,epileptic-enceph,genomic-autopsy,kidgen,leukodystrophies,mito-disease,talos',
    },
    {
        name: 'GID',
        groupBy: 'topic',
        topics: 'heartkids,flinders-ophthal,geneadd,gid-grdr,rdp-autoimmune',
    },
    {
        name: 'Peter Mac / Blombery',
        groupBy: 'topic',
        topics: 'ibmdx,bccc,mgha-bmf',
    },
    {
        name: 'All RD GCP-Projects',
        groupBy: 'gcp_project',
        gcpProjects:
            'acute-care-321904,ag-cardiac,ag-hidden,ag-very-hidden,bccc-641745,brain-malf,broad-rgp,chop-gliadx,circa-716939,cpg-rare-disease,cpg-talos,epileptic-enceph,flinders-ophthal,geneadd-314159,genomic-autopsy,ghfm-kidgen,gid-grdr-564131,heartkids,hereditary-neuro,ibmdx-501430,kidgen,leukodystrophies,mcri-lrp,mgha-bmf,mito-disease,mito-mdt,ohmr3-mendelian,ohmr4-epilepsy,perth-neuro,ravenscroft-arch,ravenscroft-rdstudy,ravenscroft-rpl,rdc-hsp,rdnow-331902,rdp-autoimmune,rdp-kidney,rdp-neuro,schr-neuro,udn-aus,udn-aus-training,uom-ird,validation-351902',
    },
]

//Add Billing groups of PopGen Team here
export const BillingGroupListPopGen = [
    {
        name: 'agdd',
        groupBy: 'topic',
        topics: 'agdd,bioheart,hgdp,hgdp-1kg,mackenzie,mgrb,ourdna,tenk10k,thousand-genomes,tob-wgs',
    },
    {
        name: 'mackenzie',
        groupBy: 'topic',
        topics: 'mackenzie,hgdp,hgdp-1kg,thousand-genomes',
    },
    {
        name: 'tenk10k',
        groupBy: 'topic',
        topics: 'tenk10k,bioheart,tob-wgs,tenk10k-sv',
    },
    {
        name: 'All PopGen GCP-Projects',
        groupBy: 'gcp_project',
        gcpProjects:
            'agdd-490196,bioheart,gtex-461745,g-pangenome,mackenzie-549705,mgrb-324606,lof-curation-655721,prophecy-339301,tenk10k,tenk10k-sv,tob-wgs,tx-adapt,validation-351902,thousand-genomes,hgdp-1kg-165049,hgdp-323802',
    },
]
