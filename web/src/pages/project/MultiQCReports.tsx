import * as React from 'react'

interface MultiQCReportsProps {
    projectName: string
}

const REPORT_PREFIX = 'https://main-web.populationgenomics.org.au/'

const REPORT_TYPES = {
    'WGS Cram': '/qc/cram/multiqc.html',
    'WGS GVCF': '/qc/gvcf/multiqc.html',
    'WGS FASTQC': '/qc/fastqc/multiqc.html',
    'Exome Cram': '/exome/qc/cram/multiqc.html',
    'Exome GVCF': '/exome/qc/gvcf/multiqc.html',
    'Exome FASTQC': '/exome/qc/fastqc/multiqc.html',
}

const MultiQCReports: React.FunctionComponent<MultiQCReportsProps> = ({ projectName }) => (
    <>
        <h4> MultiQC Links</h4>
        {Object.entries(REPORT_TYPES).map(([key, value]) => (
            <a
                href={`${REPORT_PREFIX}${projectName}${value}`}
                className="ui button"
                key={key}
                target="_blank"
                rel="noreferrer"
            >
                {key}
            </a>
        ))}
    </>
)

export default MultiQCReports
